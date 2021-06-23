// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use alloc::alloc::{alloc, dealloc, Layout};
use alloc::prelude::v1::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;

use core::cell::{Cell, UnsafeCell};
use core::default::Default;
use core::fmt;
use core::hint::spin_loop;
use core::mem::{align_of, size_of};
use core::ops::{Drop, FnMut};
use core::slice::from_raw_parts_mut;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

use crate::context::MAX_PENDING_OPS;
use crate::replica::MAX_THREADS_PER_REPLICA;

/// The default size of the shared log in bytes. If constructed using the
/// default constructor, the log will be these many bytes in size. Currently
/// set to 32 MiB based on the ASPLOS 2017 paper.
const DEFAULT_LOG_BYTES: usize = 32 * 1024 * 1024;
const_assert!(DEFAULT_LOG_BYTES >= 1 && (DEFAULT_LOG_BYTES & (DEFAULT_LOG_BYTES - 1) == 0));

/// The maximum number of replicas that can be registered with the log.
pub const MAX_REPLICAS_PER_LOG: usize = 192;

/// Constant required for garbage collection. When the tail and the head are
/// these many entries apart on the circular buffer, garbage collection will
/// be performed by one of the replicas registered with the log.
///
/// For the GC algorithm to work, we need to ensure that we can support the
/// largest possible append after deciding to perform GC. This largest possible
/// append is when every thread within a replica has a full batch of writes
/// to be appended to the shared log.
const GC_FROM_HEAD: usize = MAX_PENDING_OPS * MAX_THREADS_PER_REPLICA;
const_assert!(GC_FROM_HEAD >= 1 && (GC_FROM_HEAD & (GC_FROM_HEAD - 1) == 0));

/// Threshold after how many iterations we log a warning for busy spinning loops.
///
/// This helps with debugging to figure out where things may end up blocking.
/// Should be a power of two to avoid divisions.
const WARN_THRESHOLD: usize = 1 << 28;

/// Callback function which indicates which replicas need to be advanced for GC
/// to make progress.
type CallbackFn = dyn FnMut(&[AtomicBool; MAX_REPLICAS_PER_LOG], usize);

/// An entry that sits on the log. Each entry consists of three fields: The operation to
/// be performed when a thread reaches this entry on the log, the replica that appended
/// this operation, and a flag indicating whether this entry is valid.
///
/// `T` is the type on the operation - typically an enum class containing opcodes as well as
/// arguments. It is required that this type be sized and cloneable.
#[derive(Default)]
#[repr(align(64))]
struct Entry<T>
where
    T: Sized + Clone,
{
    /// The operation that this entry represents.
    operation: Option<T>,

    /// Identifies the replica that issued the above operation.
    replica: usize,

    /// Identifies the replica-local thread-id that issued the operation.
    thread: usize,

    /// Identifies if the operation is of scan type or not.
    is_scan: bool,

    /// Identifies if the operation is immutable scan or not.
    is_read_op: bool,

    /// If operation is of scan type, then `depends_on` stores
    /// the offsets in other logs this operation depends on.
    depends_on: Option<Arc<Vec<usize>>>,

    /// Indicates whether this entry represents a valid operation when on the log.
    alivef: AtomicBool,

    /// Used to remove operation once all the replica consumes the entry.
    refcnt: AtomicUsize,
}

/// A log of operations that is typically accessed by multiple
/// [Replica](struct.Replica.html).
///
/// Operations can be added to the log by calling the `append()` method and
/// providing a list of operations to be performed.
///
/// Operations already on the log can be executed by calling the `exec()` method
/// and providing a replica-id along with a closure. Newly added operations
/// since the replica last called `exec()` will be executed by invoking the
/// supplied closure for each one of them.
///
/// Accepts one generic type parameter; `T` defines the type of operations and
/// their arguments that will go on the log and would typically be an enum
/// class.
///
/// This struct is aligned to 64 bytes optimizing cache access.\
///
/// # Note
/// As a client, typically there is no need to call any methods on the Log aside
/// from `new`. Only in the rare circumstance someone would implement their own
/// Replica would it be necessary to call any of the Log's methods.
#[repr(align(64))]
pub struct Log<'a, T>
where
    T: Sized + Clone,
{
    /// Raw pointer to the actual underlying log. Required for dealloc.
    rawp: *mut u8,

    /// Size of the underlying log in bytes. Required for dealloc.
    rawb: usize,

    /// The maximum number of entries that can be held inside the log.
    size: usize,

    /// A global unique id for each log.
    idx: usize,

    /// A reference to the actual log. Nothing but a slice of entries.
    slog: &'a [Cell<Entry<T>>],

    /// Logical index into the above slice at which the log starts.
    head: CachePadded<AtomicUsize>,

    /// Logical index into the above slice at which the log ends.
    /// New appends go here.
    tail: CachePadded<AtomicUsize>,

    /// Completed tail maintains an index <= tail that points to a
    /// log entry after which there are no completed operations across
    /// all replicas registered against this log.
    ctail: CachePadded<AtomicUsize>,

    /// Array consisting of the local tail of each replica registered with the log.
    /// Required for garbage collection; since replicas make progress over the log
    /// independently, we want to make sure that we don't garbage collect operations
    /// that haven't been executed by all replicas.
    pub ltails: [CachePadded<AtomicUsize>; MAX_REPLICAS_PER_LOG],

    /// Identifier that will be allocated to the next replica that registers with
    /// this Log. Also required to correctly index into ltails above.
    next: CachePadded<AtomicUsize>,

    /// Array consisting of local alive masks for each registered replica. Required
    /// because replicas make independent progress over the log, so we need to
    /// track log wrap-arounds for each of them separately.
    lmasks: [CachePadded<Cell<bool>>; MAX_REPLICAS_PER_LOG],

    /// The application can provide a callback function to the log. The function is invoked
    /// when one or more replicas lag and stop the log to garbage collected the entries.
    /// One example of a callback function is to notify the application with a lagging
    /// replica number for this log. The application can then take action to start the log
    /// consumption on that replica. If the application is proactively taking measures to
    /// consume the log on all the replicas, then the variable can be initialized to default.
    gc: UnsafeCell<Box<CallbackFn>>,

    /// Use to append scan op atomically to all the logs.
    scanlock: CachePadded<AtomicUsize>,

    /// Check if the log can issue a GC callback; reset
    /// after the GC is done in `advance_head` function.
    notify_replicas: CachePadded<AtomicBool>,

    /// Use this array in GC callback function to notify other replicas to make progress.
    /// Assumes that the callback handler clears the replica-ids which need to do GC.
    dormant_replicas: [AtomicBool; MAX_REPLICAS_PER_LOG],
}

impl<'a, T> fmt::Debug for Log<'a, T>
where
    T: Sized + Clone,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Log")
            .field("head", &self.tail)
            .field("tail", &self.head)
            .field("size", &self.size)
            .finish()
    }
}

/// The Log is Send. The *mut u8 (`rawp`) is never dereferenced.
unsafe impl<'a, T> Send for Log<'a, T> where T: Sized + Clone {}

/// The Log is Sync. We know this because: `head` and `tail` are atomic variables, `append()`
/// reserves entries using a CAS, and exec() does not concurrently mutate entries on the log.
unsafe impl<'a, T> Sync for Log<'a, T> where T: Sized + Clone {}

impl<'a, T> Log<'a, T>
where
    T: Sized + Clone,
{
    /// Constructs and returns a log of size `bytes` bytes.
    /// A size between 1-2 MiB usually works well in most cases.
    ///
    /// # Example
    ///
    /// ```
    /// use cnr::Log;
    ///
    /// // Operation type that will go onto the log.
    /// #[derive(Clone)]
    /// enum Operation {
    ///     Read,
    ///     Write(u64),
    ///     Invalid,
    /// }
    ///
    /// // Creates a 1 Mega Byte sized log.
    /// let l = Log::<Operation>::new(1 * 1024 * 1024, 1);
    /// ```
    ///
    /// This method also allocates memory for the log upfront. No further allocations
    /// will be performed once this method returns.
    pub fn new<'b>(bytes: usize, idx: usize) -> Log<'b, T> {
        // Calculate the number of entries that will go into the log, and retrieve a
        // slice to it from the allocated region of memory.
        let mut num = bytes / Log::<T>::entry_size();

        // Make sure the log is large enough to allow for periodic garbage collection.
        if num < 2 * GC_FROM_HEAD {
            num = 2 * GC_FROM_HEAD;
        }

        // Round off to the next power of two if required. If we overflow, then set
        // the number of entries to the minimum required for GC. This is unlikely since
        // we'd need a log size > 2^63 entries for this to happen.
        if !num.is_power_of_two() {
            num = num.checked_next_power_of_two().unwrap_or(2 * GC_FROM_HEAD)
        };

        // Now that we have the actual number of entries, allocate the log.
        let b = num * Log::<T>::entry_size();
        let mem = unsafe {
            alloc(
                Layout::from_size_align(b, align_of::<Cell<Entry<T>>>())
                    .expect("Alignment error while allocating the shared log!"),
            )
        };
        if mem.is_null() {
            panic!("Failed to allocate memory for the shared log!");
        }
        let raw = unsafe { from_raw_parts_mut(mem as *mut Cell<Entry<T>>, num) };

        // Initialize all log entries by calling the default constructor.
        for e in &mut raw[..] {
            unsafe {
                ::core::ptr::write(
                    e,
                    Cell::new(Entry {
                        operation: None,
                        replica: 0usize,
                        thread: 0usize,
                        is_scan: false,
                        is_read_op: false,
                        depends_on: None,
                        alivef: AtomicBool::new(false),
                        refcnt: AtomicUsize::new(0),
                    }),
                );
            }
        }

        const LTAIL_DEFAULT: CachePadded<AtomicUsize> = CachePadded::new(AtomicUsize::new(0));
        const LMASK_DEFAULT: CachePadded<Cell<bool>> = CachePadded::new(Cell::new(true));
        const DORMANT_DEFAULT: AtomicBool = AtomicBool::new(false);
        Log {
            rawp: mem,
            rawb: b,
            size: num,
            idx,
            slog: raw,
            head: CachePadded::new(AtomicUsize::new(0usize)),
            tail: CachePadded::new(AtomicUsize::new(0usize)),
            ctail: CachePadded::new(AtomicUsize::new(0usize)),
            ltails: [LTAIL_DEFAULT; MAX_REPLICAS_PER_LOG],
            next: CachePadded::new(AtomicUsize::new(1usize)),
            lmasks: [LMASK_DEFAULT; MAX_REPLICAS_PER_LOG],
            gc: UnsafeCell::new(Box::new(
                |_rid: &[AtomicBool; MAX_REPLICAS_PER_LOG], _lid: usize| {},
            )),
            scanlock: CachePadded::new(AtomicUsize::new(0)),
            notify_replicas: CachePadded::new(AtomicBool::new(true)),
            dormant_replicas: [DORMANT_DEFAULT; MAX_REPLICAS_PER_LOG],
        }
    }

    /// Returns the size of an entry in bytes.
    fn entry_size() -> usize {
        size_of::<Cell<Entry<T>>>()
    }

    /// The application calls this function to update the callback function.
    /// The application does not need to call this function if it knows that all
    /// the replicas are active for this log and no replica will lag behind.
    ///
    /// # Example
    ///
    /// ```
    /// use cnr::Log;
    /// use core::sync::atomic::AtomicBool;
    ///
    /// // Operation type that will go onto the log.
    /// #[derive(Clone)]
    /// enum Operation {
    ///     Read,
    ///     Write(u64),
    ///     Invalid,
    /// }
    ///
    /// // Creates a 1 Mega Byte sized log.
    /// let mut l = Log::<Operation>::new(1 * 1024 * 1024, 1);
    ///
    /// // Update the callback function for the log.
    /// let callback_func = |rid: &[AtomicBool; 192], idx: usize| {
    ///     // Take action on log `idx` and replicas in `rid`.
    /// };
    /// l.update_closure(callback_func)
    /// ```
    pub fn update_closure(
        &mut self,
        gc: impl FnMut(&[AtomicBool; MAX_REPLICAS_PER_LOG], usize) + 'static,
    ) {
        unsafe { *self.gc.get() = Box::new(gc) };
    }

    /// Registers a replica with the log. Returns an identifier that the replica
    /// can use to execute operations on the log.
    pub(crate) fn register(&self) -> Option<usize> {
        // Loop until we either run out of identifiers or we manage to increment `next`.
        loop {
            let n = self.next.load(Ordering::Relaxed);

            // Check if we've exceeded the maximum number of replicas the log can support.
            if n >= MAX_REPLICAS_PER_LOG {
                return None;
            };

            if self
                .next
                .compare_exchange_weak(n, n + 1, Ordering::SeqCst, Ordering::SeqCst)
                != Ok(n)
            {
                continue;
            };

            return Some(n);
        }
    }

    /// Adds a batch of operations to the shared log.
    ///
    /// # Note
    /// `append` is not intended as a public interface. It is marked
    /// as public due to being used by the benchmarking code.
    #[inline(always)]
    #[doc(hidden)]
    pub fn append<F: FnMut(T, usize, usize, bool, bool, Option<Arc<Vec<usize>>>) -> bool>(
        &self,
        ops: &[(T, usize, bool)],
        idx: usize,
        mut s: F,
    ) {
        let nops = ops.len();
        let mut iteration = 1;
        let mut waitgc = 1;

        // Keep trying to reserve entries and add operations to the log until
        // we succeed in doing so.
        loop {
            if iteration % WARN_THRESHOLD == 0 {
                warn!(
                    "append(ops.len()={}, {}) takes too many iterations ({}) to complete...",
                    ops.len(),
                    idx,
                    iteration,
                );
            }
            iteration += 1;

            let tail = self.tail.load(Ordering::Relaxed);
            let head = self.head.load(Ordering::Relaxed);

            // Head and tail doesn't wrap around; so it works.
            let used = tail - head + 1;

            if used > self.size / 3 {
                let r = self.next.load(Ordering::Relaxed);
                let mut is_stuck = false;
                let cur_local_tail = self.ltails[idx - 1].load(Ordering::Relaxed);

                // Find the smallest local tail across all replicas.
                for idx in 1..r {
                    let local_tail = self.ltails[idx - 1].load(Ordering::Relaxed);
                    if cur_local_tail > local_tail && cur_local_tail - local_tail > self.size / 3 {
                        self.dormant_replicas[idx - 1].store(true, Ordering::Relaxed);
                        is_stuck = true;
                    }
                }

                if is_stuck
                    && self.notify_replicas.compare_exchange_weak(
                        true,
                        false,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) == Ok(true)
                {
                    unsafe { (*self.gc.get())(&self.dormant_replicas, self.idx) };
                }
            }

            // If there are fewer than `GC_FROM_HEAD` entries on the log, then just
            // try again. The replica that reserved entry (h + self.size - GC_FROM_HEAD)
            // is currently trying to advance the head of the log. Keep refreshing the
            // replica against the log to make sure that it isn't deadlocking GC.
            if tail > head + self.size - GC_FROM_HEAD {
                if waitgc % WARN_THRESHOLD == 0 {
                    warn!(
                        "append(ops.len()={}, {}) takes too many iterations ({}) waiting for gc...",
                        ops.len(),
                        idx,
                        waitgc,
                    );
                }
                waitgc += 1;
                self.exec(idx, &mut s);
                continue;
            }

            // If on adding in the above entries there would be fewer than `GC_FROM_HEAD`
            // entries left on the log, then we need to advance the head of the log.
            let mut advance = false;
            if tail + nops > head + self.size - GC_FROM_HEAD {
                advance = true
            };

            // Try reserving slots for the operations. If that fails, then restart
            // from the beginning of this loop.
            if self.tail.compare_exchange_weak(
                tail,
                tail + nops,
                Ordering::Acquire,
                Ordering::Acquire,
            ) != Ok(tail)
            {
                continue;
            };

            // Successfully reserved entries on the shared log. Add the operations in.
            for (i, op) in ops.iter().enumerate().take(nops) {
                unsafe { self.update_entry(tail + i, op, idx, false, None) };
            }

            // If needed, advance the head of the log forward to make room on the log.
            if advance {
                self.advance_head(idx, &mut s);
            }

            return;
        }
    }

    /// Adds a scan operation to the shared log.
    #[inline(always)]
    #[doc(hidden)]
    pub(crate) fn try_append_scan<
        F: FnMut(T, usize, usize, bool, bool, Option<Arc<Vec<usize>>>) -> bool,
    >(
        &self,
        op: &(T, usize, bool),
        idx: usize,
        offset: &[usize],
        mut s: F,
    ) -> Result<usize, usize> {
        let nops = 1;
        let log_offset;

        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);

        // If there are fewer than `GC_FROM_HEAD` entries on the log, then just
        // try again. The replica that reserved entry (h + self.size - GC_FROM_HEAD)
        // is currently trying to advance the head of the log. Keep refreshing the
        // replica against the log to make sure that it isn't deadlocking GC.
        if tail > head + self.size - GC_FROM_HEAD {
            self.exec(idx, &mut s);
            return Err(0);
        }

        // If on adding in the above entries there would be fewer than `GC_FROM_HEAD`
        // entries left on the log, then we need to advance the head of the log.
        let mut advance = false;
        if tail + nops > head + self.size - GC_FROM_HEAD {
            advance = true;
        };

        // Try reserving slots for the operations. If that fails, then restart
        // from the beginning of this loop.
        if self
            .tail
            .compare_exchange_weak(tail, tail + nops, Ordering::Acquire, Ordering::Relaxed)
            != Ok(tail)
        {
            return Err(0);
        };

        // Successfully reserved entries on the shared log. Add the operations in.
        log_offset = tail;
        if self.idx != 1 {
            unsafe {
                self.update_entry(log_offset, op, idx, true, Some(Arc::new(vec![offset[0]])))
            };
        }

        // If needed, advance the head of the log forward to make room on the log.
        if advance {
            self.advance_head(idx, &mut s);
        }

        Ok(log_offset)
    }

    /// Update the depends_on field for scan operation. Replica mainatins the
    /// offset for the scan entry and later it updates the remaining offset there.
    pub(crate) fn fix_scan_entry(
        &self,
        op: &(T, usize, bool),
        idx: usize,
        offsets: Arc<Vec<usize>>,
    ) {
        unsafe { self.update_entry(offsets[0], op, idx, true, Some(offsets)) };
    }

    #[inline(always)]
    unsafe fn update_entry(
        &self,
        offset: usize,
        op: &(T, usize, bool),
        idx: usize,
        is_scan: bool,
        depends_on: Option<Arc<Vec<usize>>>,
    ) {
        let num_replicas = self.next.load(Ordering::Relaxed) - 1;
        let e = self.slog[self.index(offset)].as_ptr();
        let mut m = self.lmasks[idx - 1].get();

        // This entry was just reserved so it should be dead (!= m). However, if
        // the log has wrapped around, then the alive mask has flipped. In this
        // case, we flip the mask we were originally going to write into the
        // allocated entry. We cannot flip lmasks[idx - 1] because this replica
        // might still need to execute a few entries before the wrap around.
        if (*e).alivef.load(Ordering::Relaxed) == m {
            m = !m;
        }

        (*e).operation = Some(op.0.clone());
        (*e).replica = idx;
        (*e).thread = op.1;
        (*e).is_scan = is_scan;
        (*e).is_read_op = op.2;
        (*e).depends_on = depends_on;
        (*e).refcnt = AtomicUsize::new(num_replicas);
        (*e).alivef.store(m, Ordering::Release);
    }

    /// Try to acquire the scan lock.
    fn try_scan_lock(&self, tid: usize) -> bool {
        for _i in 0..4 {
            if unsafe {
                core::ptr::read_volatile(
                    &self.scanlock
                        as *const crossbeam_utils::CachePadded<core::sync::atomic::AtomicUsize>
                        as *const usize,
                )
            } != 0
            {
                /* someone else has the lock */
                return false;
            };
        }

        if self
            .scanlock
            .compare_exchange_weak(0, tid, Ordering::Acquire, Ordering::Relaxed)
            != Ok(0)
        {
            /* cas failed, we don't hold the lock */
            return false;
        }

        /* successfully acquired the lock */
        true
    }

    /// Acquire the scan lock.
    pub(crate) fn acquire_scan_lock(&self, tid: usize) {
        while !self.try_scan_lock(tid) {
            spin_loop();
        }
    }

    /// Release the scan lock.
    pub(crate) fn release_scan_lock(&self) {
        self.scanlock.store(0, Ordering::Release);
    }

    /// Executes a passed in closure (`d`) on all operations starting from
    /// a replica's local tail on the shared log. The replica is identified through an
    /// `idx` passed in as an argument.
    ///
    /// The passed in closure is expected to take in two arguments: The operation
    /// from the shared log to be executed and the replica that issued it.
    #[inline(always)]
    pub(crate) fn exec<F: FnMut(T, usize, usize, bool, bool, Option<Arc<Vec<usize>>>) -> bool>(
        &self,
        idx: usize,
        d: &mut F,
    ) {
        // Load the logical log offset from which we must execute operations.
        let ltail = self.ltails[idx - 1].load(Ordering::Relaxed);

        // Check if we have any work to do by comparing our local tail with the log's
        // global tail. If they're equal, then we're done here and can simply return.
        let gtail = self.tail.load(Ordering::Relaxed);
        if ltail == gtail {
            return;
        }

        let h = self.head.load(Ordering::Relaxed);

        // Make sure we're within the shared log. If we aren't, then panic.
        if ltail > gtail || ltail < h {
            panic!("Local tail not within the shared log!")
        };

        // Execute all operations from the passed in offset to the shared log's tail. Check if
        // the entry is live first; we could have a replica that has reserved entries, but not
        // filled them into the log yet.
        for i in ltail..gtail {
            let mut iteration = 1;
            let e = self.slog[self.index(i)].as_ptr();

            while unsafe { (*e).alivef.load(Ordering::Acquire) != self.lmasks[idx - 1].get() } {
                if iteration % WARN_THRESHOLD == 0 {
                    warn!(
                        "alivef not being set for self.index(i={}) = {} (self.lmasks[{}] is {})...",
                        i,
                        self.index(i),
                        idx - 1,
                        self.lmasks[idx - 1].get()
                    );
                }
                iteration += 1;
            }

            let mut depends_on = None;
            unsafe {
                if (*e).is_scan {
                    depends_on = Some((*e).depends_on.as_ref().unwrap().clone());
                }

                if !d(
                    (*e).operation.as_ref().unwrap().clone(),
                    (*e).replica,
                    (*e).thread,
                    (*e).is_scan,
                    (*e).is_read_op,
                    depends_on,
                ) {
                    // if the operation is unable to complete; then update the ctail for
                    // already executed operations and return. Only happends for scan ops.
                    self.ctail.fetch_max(i, Ordering::Relaxed);
                    return;
                }
                if (*e).refcnt.fetch_sub(1, Ordering::Release) == 1 {
                    (*e).operation = None;
                }
            }

            // Increment ltail for each operations, needed for scan
            // operations as the rubberband is ltail sensitive.
            self.ltails[idx - 1].fetch_add(1, Ordering::Relaxed);

            // Looks like we're going to wrap around now; flip this replica's local mask.
            if self.index(i) == self.size - 1 {
                self.lmasks[idx - 1].set(!self.lmasks[idx - 1].get());
                //trace!("idx: {} lmask: {}", idx, self.lmasks[idx - 1].get());
            }
        }

        // Update the completed tail after we've executed these operations.
        // Also update this replica's local tail.
        self.ctail.fetch_max(gtail, Ordering::Relaxed);
        self.ltails[idx - 1].store(gtail, Ordering::Relaxed);
    }

    /// Returns a physical index given a logical index into the shared log.
    #[inline(always)]
    fn index(&self, logical: usize) -> usize {
        logical & (self.size - 1)
    }

    /// Advances the head of the log forward. If a replica has stopped making progress,
    /// then this method will never return. Accepts a closure that is passed into exec()
    /// to ensure that this replica does not deadlock GC.
    #[inline(always)]
    fn advance_head<F: FnMut(T, usize, usize, bool, bool, Option<Arc<Vec<usize>>>) -> bool>(
        &self,
        rid: usize,
        mut s: &mut F,
    ) {
        // Keep looping until we can advance the head and create some free space
        // on the log. If one of the replicas has stopped making progress, then
        // this method might never return.
        let mut iteration = 1;
        loop {
            let r = self.next.load(Ordering::Relaxed);
            let global_head = self.head.load(Ordering::Relaxed);
            let f = self.tail.load(Ordering::Relaxed);

            let mut min_local_tail = self.ltails[0].load(Ordering::Relaxed);

            // Find the smallest local tail across all replicas.
            for idx in 1..r {
                let cur_local_tail = self.ltails[idx - 1].load(Ordering::Relaxed);
                if min_local_tail > cur_local_tail {
                    min_local_tail = cur_local_tail
                };
            }

            // If we cannot advance the head further, then start
            // from the beginning of this loop again. Before doing so, try consuming
            // any new entries on the log to prevent deadlock.
            if min_local_tail == global_head {
                if iteration % WARN_THRESHOLD == 0 {
                    warn!("Spending a long time in `advance_head`, are we starving?");
                }
                iteration += 1;
                self.exec(rid, &mut s);
                continue;
            }

            // There are entries that can be freed up; update the head offset.
            self.head.store(min_local_tail, Ordering::Relaxed);

            // Reset notify replicas after the GC.
            self.notify_replicas.store(true, Ordering::Relaxed);

            // Make sure that we freed up enough space so that threads waiting for
            // GC in append can make progress. Otherwise, try to make progress again.
            // If we're making progress again, then try consuming entries on the log.
            if f < min_local_tail + self.size - GC_FROM_HEAD {
                return;
            } else {
                self.exec(rid, &mut s);
            }
        }
    }

    /// Resets the log. Required for microbenchmarking the log; with this method, we
    /// can re-use the log across experimental runs without having to re-allocate the
    /// log over and over again.
    ///
    /// # Safety
    ///
    /// *To be used for testing/benchmarking only, hence marked unsafe*. Before calling
    /// this method, please make sure that there aren't any replicas/threads actively
    /// issuing/executing operations to/from this log.
    #[doc(hidden)]
    #[inline(always)]
    pub unsafe fn reset(&self) {
        // First, reset global metadata.
        self.head.store(0, Ordering::SeqCst);
        self.tail.store(0, Ordering::SeqCst);
        self.next.store(1, Ordering::SeqCst);

        // Next, reset replica-local metadata.
        for r in 0..MAX_REPLICAS_PER_LOG {
            self.ltails[r].store(0, Ordering::Relaxed);
            self.lmasks[r].set(true);
        }

        // Next, free up all log entries. Use pointers to avoid memcpy and speed up
        // the reset of the log here.
        for i in 0..self.size {
            let e = self.slog[self.index(i)].as_ptr();
            (*e).alivef.store(false, Ordering::Release);
        }
    }

    /// This method checks if the replica is in sync to execute a read-only operation
    /// right away. It does so by comparing the replica's local tail with the log's
    /// completed tail.
    #[inline(always)]
    pub(crate) fn is_replica_synced_for_reads(&self, idx: usize, ctail: usize) -> bool {
        self.ltails[idx - 1].load(Ordering::Relaxed) >= ctail
    }

    /// This method returns the current ctail value for the log.
    #[inline(always)]
    pub(crate) fn get_ctail(&self) -> usize {
        self.ctail.load(Ordering::Relaxed)
    }
}

impl<'a, T> Default for Log<'a, T>
where
    T: Sized + Clone,
{
    /// Default constructor for the shared log.
    fn default() -> Self {
        Log::new(DEFAULT_LOG_BYTES, 1)
    }
}

impl<'a, T> Drop for Log<'a, T>
where
    T: Sized + Clone,
{
    /// Destructor for the shared log.
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.rawp,
                Layout::from_size_align(self.rawb, align_of::<Cell<Entry<T>>>())
                    .expect("Alignment error while deallocating the shared log!"),
            )
        };
    }
}

#[cfg(test)]
mod tests {
    // Import std so that we have an allocator for our unit tests.
    extern crate std;

    use super::*;
    use std::sync::Arc;

    // Define operations along with their arguments that go onto the log.
    #[derive(Clone)] // Traits required by the log interface.
    #[derive(Debug, PartialEq)] // Traits required for testing.
    enum Operation {
        Read,
        Write(u64),
        Invalid,
    }

    // Required so that we can unit test Entry.
    impl Default for Operation {
        fn default() -> Operation {
            Operation::Invalid
        }
    }

    // Test that we can default construct entries correctly.
    #[test]
    fn test_entry_create_default() {
        let e = Entry::<Operation>::default();
        assert_eq!(e.operation, None);
        assert_eq!(e.replica, 0);
        assert_eq!(e.alivef.load(Ordering::Relaxed), false);
    }

    // Test that our entry_size() method returns the correct size.
    #[test]
    fn test_log_entry_size() {
        assert_eq!(Log::<Operation>::entry_size(), 64);
    }

    // Tests if a small log can be correctly constructed.
    #[test]
    fn test_log_create() {
        let l = Log::<Operation>::new(1024 * 1024, 1);
        let n = (1024 * 1024) / Log::<Operation>::entry_size();
        assert_eq!(l.rawb, 1024 * 1024);
        assert_eq!(l.size, n);
        assert_eq!(l.slog.len(), n);
        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 0);
        assert_eq!(l.next.load(Ordering::Relaxed), 1);
        assert_eq!(l.ctail.load(Ordering::Relaxed), 0);

        for i in 0..MAX_REPLICAS_PER_LOG {
            assert_eq!(l.ltails[i].load(Ordering::Relaxed), 0);
        }

        for i in 0..MAX_REPLICAS_PER_LOG {
            assert_eq!(l.lmasks[i].get(), true);
        }
    }

    // Tests if the constructor allocates enough space for GC.
    #[test]
    fn test_log_min_size() {
        let l = Log::<Operation>::new(1024, 1);
        assert_eq!(l.rawb, 2 * GC_FROM_HEAD * Log::<Operation>::entry_size());
        assert_eq!(l.size, 2 * GC_FROM_HEAD);
        assert_eq!(l.slog.len(), 2 * GC_FROM_HEAD);
    }

    // Tests that the constructor allocates a log whose number of entries
    // are a power of two.
    #[test]
    fn test_log_power_of_two() {
        let l = Log::<Operation>::new(524 * 1024, 1);
        let n = ((524 * 1024) / Log::<Operation>::entry_size()).checked_next_power_of_two();
        assert_eq!(l.rawb, n.unwrap() * Log::<Operation>::entry_size());
        assert_eq!(l.size, n.unwrap());
        assert_eq!(l.slog.len(), n.unwrap());
    }

    // Tests if the log can be successfully default constructed.
    #[test]
    fn test_log_create_default() {
        let l = Log::<Operation>::default();
        let n = DEFAULT_LOG_BYTES / Log::<Operation>::entry_size();
        assert_eq!(l.rawb, DEFAULT_LOG_BYTES);
        assert_eq!(l.size, n);
        assert_eq!(l.slog.len(), n);
        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 0);
        assert_eq!(l.next.load(Ordering::Relaxed), 1);
        assert_eq!(l.ctail.load(Ordering::Relaxed), 0);

        for i in 0..MAX_REPLICAS_PER_LOG {
            assert_eq!(l.ltails[i].load(Ordering::Relaxed), 0);
        }

        for i in 0..MAX_REPLICAS_PER_LOG {
            assert_eq!(l.lmasks[i].get(), true);
        }
    }

    // Tests if we can correctly index into the shared log.
    #[test]
    fn test_log_index() {
        let l = Log::<Operation>::new(2 * 1024 * 1024, 1);
        assert_eq!(l.index(99000), 696);
    }

    // Tests if we can correctly register with the shared log.
    #[test]
    fn test_log_register() {
        let l = Log::<Operation>::new(1024, 1);
        assert_eq!(l.register(), Some(1));
        assert_eq!(l.next.load(Ordering::Relaxed), 2);
    }

    // Tests that we cannot register more than the max replicas with the log.
    #[test]
    fn test_log_register_none() {
        let l = Log::<Operation>::new(1024, 1);
        l.next.store(MAX_REPLICAS_PER_LOG, Ordering::Relaxed);
        assert!(l.register().is_none());
        assert_eq!(l.next.load(Ordering::Relaxed), MAX_REPLICAS_PER_LOG);
    }

    // Test that we can correctly append an entry into the log.
    #[test]
    fn test_log_append() {
        let l = Log::<Operation>::default();
        let o = [(Operation::Read, 1, false)];
        l.append(&o, 1, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });

        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 1);
        let slog = l.slog[0].take();
        assert_eq!(slog.operation, Some(Operation::Read));
        assert_eq!(slog.replica, 1);
    }

    // Test that multiple entries can be appended to the log.
    #[test]
    fn test_log_append_multiple() {
        let l = Log::<Operation>::default();
        let o = [
            (Operation::Read, 1, false),
            (Operation::Write(119), 1, false),
        ];
        l.append(&o, 1, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });

        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 2);
    }

    // Tests that we can advance the head of the log to the smallest of all replica-local tails.
    #[test]
    fn test_log_advance_head() {
        let l = Log::<Operation>::default();

        l.next.store(5, Ordering::Relaxed);
        l.ltails[0].store(1023, Ordering::Relaxed);
        l.ltails[1].store(224, Ordering::Relaxed);
        l.ltails[2].store(4096, Ordering::Relaxed);
        l.ltails[3].store(799, Ordering::Relaxed);

        l.advance_head(0, &mut |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });
        assert_eq!(l.head.load(Ordering::Relaxed), 224);
    }

    // Tests that the head of the log is advanced when we're close to filling up the entire log.
    #[test]
    fn test_log_append_gc() {
        let l = Log::<Operation>::default();
        let o: [(Operation, usize, bool); 4] = unsafe {
            let mut a: [(Operation, usize, bool); 4] =
                ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, (Operation::Read, 1, false));
            }
            a
        };

        l.next.store(2, Ordering::Relaxed);
        l.tail.store(l.size - GC_FROM_HEAD - 1, Ordering::Relaxed);
        l.ltails[0].store(1024, Ordering::Relaxed);
        l.append(&o, 1, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });

        assert_eq!(l.head.load(Ordering::Relaxed), 1024);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.size - GC_FROM_HEAD + 3);
    }

    // Tests that on log wrap around, the local mask stays
    // the same because entries have not been executed yet.
    #[test]
    fn test_log_append_wrap() {
        let l = Log::<Operation>::default();
        let o: [(Operation, usize, bool); 1024] = unsafe {
            let mut a: [(Operation, usize, bool); 1024] =
                ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, (Operation::Read, 1, false));
            }
            a
        };

        l.next.store(2, Ordering::Relaxed);
        l.head.store(2 * 8192, Ordering::Relaxed);
        l.tail.store(l.size - 10, Ordering::Relaxed);
        l.append(&o, 1, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });

        assert_eq!(l.lmasks[0].get(), true);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.size + 1014);
    }

    // Test that we can execute operations appended to the log.
    #[test]
    fn test_log_exec() {
        let l = Log::<Operation>::default();
        let o = [(Operation::Read, 1, false)];
        let mut f = |op: Operation, i: usize, _, _, _, _| -> bool {
            assert_eq!(op, Operation::Read);
            assert_eq!(i, 1);
            true
        };

        l.append(&o, 1, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });
        l.exec(1, &mut f);

        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.ctail.load(Ordering::Relaxed)
        );
        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.ltails[0].load(Ordering::Relaxed)
        );
    }

    // Test that exec() doesn't do anything when the log is empty.
    #[test]
    fn test_log_exec_empty() {
        let l = Log::<Operation>::default();
        let mut f = |_o: Operation, _i: usize, _, _, _, _| -> bool {
            assert!(false);
            true
        };

        l.exec(1, &mut f);
    }

    // Test that exec() doesn't do anything if we're already up-to-date.
    #[test]
    fn test_log_exec_zero() {
        let l = Log::<Operation>::default();
        let o = [(Operation::Read, 1, false)];
        let mut f = |op: Operation, i: usize, _, _, _, _| -> bool {
            assert_eq!(op, Operation::Read);
            assert_eq!(i, 1);
            true
        };
        let mut g = |_op: Operation, _i: usize, _, _, _, _| -> bool {
            assert!(false);
            true
        };

        l.append(&o, 1, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });
        l.exec(1, &mut f);
        l.exec(1, &mut g);
    }

    // Test that multiple entries on the log can be executed correctly.
    #[test]
    fn test_log_exec_multiple() {
        let l = Log::<Operation>::default();
        let o = [
            (Operation::Read, 1, false),
            (Operation::Write(119), 1, false),
        ];
        let mut s = 0;
        let mut f = |op: Operation, _i: usize, _, _, _, _| -> bool {
            match op {
                Operation::Read => s += 121,
                Operation::Write(v) => s += v,
                Operation::Invalid => assert!(false),
            }
            true
        };

        l.append(&o, 1, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });
        l.exec(1, &mut f);
        assert_eq!(s, 240);

        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.ctail.load(Ordering::Relaxed)
        );
        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.ltails[0].load(Ordering::Relaxed)
        );
    }

    // Test that the replica local mask is updated correctly when executing over
    // a wrapped around log.
    #[test]
    fn test_log_exec_wrap() {
        let l = Log::<Operation>::default();
        let o: [(Operation, usize, bool); 1024] = unsafe {
            let mut a: [(Operation, usize, bool); 1024] =
                ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, (Operation::Read, 1, false));
            }
            a
        };
        let mut f = |op: Operation, i: usize, _, _, _, _| -> bool {
            assert_eq!(op, Operation::Read);
            assert_eq!(i, 1);
            true
        };

        l.append(&o, 1, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        }); // Required for GC to work correctly.
        l.next.store(2, Ordering::SeqCst);
        l.head.store(2 * 8192, Ordering::SeqCst);
        l.tail.store(l.size - 10, Ordering::SeqCst);
        l.append(&o, 1, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });

        l.ltails[0].store(l.size - 10, Ordering::SeqCst);
        l.exec(1, &mut f);

        assert_eq!(l.lmasks[0].get(), false);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.size + 1014);
    }

    // Tests that exec() panics if the head of the log advances beyond the tail.
    #[test]
    #[should_panic]
    fn test_exec_panic() {
        let l = Log::<Operation>::default();
        let o: [(Operation, usize, bool); 1024] = unsafe {
            let mut a: [(Operation, usize, bool); 1024] =
                ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, (Operation::Read, 1, false));
            }
            a
        };
        let mut f = |_op: Operation, _i: usize, _, _, _, _| -> bool {
            assert!(false);
            true
        };

        l.append(&o, 1, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });
        l.head.store(8192, Ordering::SeqCst);

        l.exec(1, &mut f);
    }

    // Tests that operations are cloned when added to the log, and that
    // they are correctly dropped once overwritten.
    #[test]
    fn test_log_change_refcount() {
        let l = Log::<Arc<Operation>>::default();
        let o1 = [(Arc::new(Operation::Read), 1, false)];
        let o2 = [(Arc::new(Operation::Read), 1, false)];
        assert_eq!(Arc::strong_count(&o1[0].0), 1);
        assert_eq!(Arc::strong_count(&o2[0].0), 1);

        l.append(
            &o1[..],
            1,
            |_o: Arc<Operation>, _i: usize, _, _, _, _| -> bool { true },
        );
        assert_eq!(Arc::strong_count(&o1[0].0), 2);
        l.append(
            &o1[..],
            1,
            |_o: Arc<Operation>, _i: usize, _, _, _, _| -> bool { true },
        );
        assert_eq!(Arc::strong_count(&o1[0].0), 3);

        unsafe { l.reset() };

        // Over here, we overwrite entries that were written to by the two
        // previous appends. This decreases the refcount of o1 and increases
        // the refcount of o2.
        l.append(
            &o2[..],
            1,
            |_o: Arc<Operation>, _i: usize, _, _, _, _| -> bool { true },
        );
        assert_eq!(Arc::strong_count(&o1[0].0), 2);
        assert_eq!(Arc::strong_count(&o2[0].0), 2);
        l.append(
            &o2[..],
            1,
            |_o: Arc<Operation>, _i: usize, _, _, _, _| -> bool { true },
        );
        assert_eq!(Arc::strong_count(&o1[0].0), 1);
        assert_eq!(Arc::strong_count(&o2[0].0), 3);
    }

    // Tests that operations are cloned when added to the log, and that
    // they are correctly dropped once overwritten after the GC.
    #[test]
    fn test_log_refcount_change_with_gc() {
        let entry_size = 64;
        let total_entries = 16384;

        assert_eq!(Log::<Arc<Operation>>::entry_size(), entry_size);
        let size: usize = total_entries * entry_size;
        let l = Log::<Arc<Operation>>::new(size, 1);
        let o1 = [(Arc::new(Operation::Read), 1, false)];
        let o2 = [(Arc::new(Operation::Read), 1, false)];
        assert_eq!(Arc::strong_count(&o1[0].0), 1);
        assert_eq!(Arc::strong_count(&o2[0].0), 1);

        for i in 1..(total_entries + 1) {
            l.append(
                &o1[..],
                1,
                |_o: Arc<Operation>, _i: usize, _, _, _, _| -> bool { true },
            );
            assert_eq!(Arc::strong_count(&o1[0].0), i + 1);
        }
        assert_eq!(Arc::strong_count(&o1[0].0), total_entries + 1);

        for i in 1..(total_entries + 1) {
            l.append(
                &o2[..],
                1,
                |_o: Arc<Operation>, _i: usize, _, _, _, _| -> bool { true },
            );
            assert_eq!(Arc::strong_count(&o1[0].0), (total_entries + 1) - i);
            assert_eq!(Arc::strong_count(&o2[0].0), i + 1);
        }
        assert_eq!(Arc::strong_count(&o1[0].0), 1);
        assert_eq!(Arc::strong_count(&o2[0].0), total_entries + 1);
    }

    // Tests that is_replica_synced_for_read() works correctly; it returns
    // false when a replica is not synced up and true when it is.
    #[test]
    fn test_replica_synced_for_read() {
        let l = Log::<Operation>::default();
        let one = l.register().unwrap();
        let two = l.register().unwrap();

        assert_eq!(one, 1);
        assert_eq!(two, 2);

        let o = [(Operation::Read, 1, false)];
        let mut f = |op: Operation, i: usize, _, _, _, _| -> bool {
            assert_eq!(op, Operation::Read);
            assert_eq!(i, 1);
            true
        };

        l.append(&o, one, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });
        l.exec(one, &mut f);
        assert_eq!(l.is_replica_synced_for_reads(one, l.get_ctail()), true);
        assert_eq!(l.is_replica_synced_for_reads(two, l.get_ctail()), false);

        l.exec(two, &mut f);
        assert_eq!(l.is_replica_synced_for_reads(two, l.get_ctail()), true);
    }
}
