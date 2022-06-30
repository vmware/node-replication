// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Contains the shared Log, in a nutshell it's a multi-producer, multi-consumer
//! circular-buffer.

use alloc::boxed::Box;
use alloc::vec::Vec;

use core::cell::Cell;
use core::default::Default;
use core::fmt;
use core::mem::size_of;
use core::ops::FnMut;
#[cfg(not(loom))]
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;
#[cfg(loom)]
pub use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use static_assertions::const_assert;

use crate::context::MAX_PENDING_OPS;
use crate::replica::MAX_THREADS_PER_REPLICA;

/// A token that identifies a replica for a log.
///
/// The replica is supposed to call [`Log::register()`] to get the token.
#[derive(Eq, PartialEq, Debug)]
pub struct LogToken(usize);

/// The default size of the shared log in bytes. If constructed using the default
/// constructor, the log will be these many bytes in size.
///
/// Currently set to 2 MiB which seems like a good space/performance trade-off for most
/// operation types. In general after more than 100k entries, there is little impact for
/// performance from the actual log size.
pub const DEFAULT_LOG_BYTES: usize = 2 * 1024 * 1024;
const_assert!(DEFAULT_LOG_BYTES.is_power_of_two());

/// The maximum number of replicas that can be registered with the log.
///
/// Should be equal or greater than the max. amount of NUMA nodes we expect on our system
/// Can't make it arbitrarily high as it will lead to more memory overheads / bigger
/// structs.
#[cfg(not(loom))]
pub const MAX_REPLICAS_PER_LOG: usize = 16;
#[cfg(loom)] // Otherwise uses too much stack space wich crashes in loom...
pub const MAX_REPLICAS_PER_LOG: usize = 3;

/// Constant required for garbage collection. When the tail and the head are these many
/// entries apart on the circular buffer, garbage collection will be performed by one of
/// the replicas registered with the log.
///
/// For the GC algorithm to work, we need to ensure that we can support the largest
/// possible append after deciding to perform GC. This largest possible append is when
/// every thread within a replica has a full batch of writes to be appended to the shared
/// log.
const GC_FROM_HEAD: usize = MAX_PENDING_OPS * MAX_THREADS_PER_REPLICA;
const_assert!(GC_FROM_HEAD.is_power_of_two());

/// Threshold after how many iterations we abort and report the replica we're waiting for
/// as stuck for busy spinning loops.
///
/// Should be a power of two to avoid divisions.
const WARN_THRESHOLD: usize = 1 << 7;
const_assert!(WARN_THRESHOLD.is_power_of_two());

/// An entry that sits on the log. Each entry consists of three fields: The operation to
/// be performed when a thread reaches this entry on the log, the replica that appended
/// this operation, and a flag indicating whether this entry is valid.
///
/// `T` is the type on the operation - typically an enum class containing opcodes as well
/// as arguments. It is required that this type be sized and cloneable.
#[repr(align(64))]
struct Entry<T>
where
    T: Sized + Clone,
{
    /// The operation that this entry represents.
    operation: Option<T>,

    /// Identifies the replica that issued the above operation.
    ///
    /// This is the number from inside the [`LogToken`]. It's not a LogToken itself
    /// because that shouldn't be Clone.
    replica: usize,

    /// Indicates whether this entry represents a valid operation when on the log.
    alivef: AtomicBool,
}

impl<T> Default for Entry<T>
where
    T: Sized + Clone,
{
    fn default() -> Self {
        Self {
            operation: None,
            replica: 0,
            alivef: AtomicBool::new(false),
        }
    }
}

/// A log of operations that is typically accessed by multiple
/// [`crate::replica::Replica`].
///
/// Operations can be added to the log by calling the [`Log::append()`] method and
/// providing a list of operations to be performed.
///
/// Operations already on the log can be executed by calling the `Log::exec()` method and
/// providing a replica identifier, along with a closure that describes what to do for a
/// given entry. Newly added operations since the last time a replica called `Log::exec()`
/// will be executed by invoking the supplied closure for each one of them.
///
/// Takes one generic type parameter, `T`, which defines the type of operations and their
/// arguments that will go on the log. Typically this is some enum with variants.
///
/// This struct is aligned to 64 bytes to optimize cache access.
///
/// # Note
/// In the common case a client of node-replication does not need to create or interact
/// with the log directly. Instead use [`crate::NodeReplicated`]. Only in the rare
/// circumstance where someone would implement their own Replica would it be necessary to
/// call any of the Log's methods.
#[repr(align(64))]
pub struct Log<T>
where
    T: Sized + Clone,
{
    /// The actual log, a slice of entries.
    slog: Box<[Cell<Entry<T>>]>,

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
    ltails: [CachePadded<AtomicUsize>; MAX_REPLICAS_PER_LOG],

    /// Identifier that will be allocated to the next replica that registers with
    /// this Log. Also required to correctly index into ltails above.
    next: CachePadded<AtomicUsize>,

    /// Array consisting of local alive masks for each registered replica. Required
    /// because replicas make independent progress over the log, so we need to
    /// track log wrap-arounds for each of them separately.
    lmasks: [CachePadded<Cell<bool>>; MAX_REPLICAS_PER_LOG],
}

impl<T> fmt::Debug for Log<T>
where
    T: Sized + Clone,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Log")
            .field("head", &self.tail)
            .field("tail", &self.head)
            .field("log_entries", &self.slog.len())
            .finish()
    }
}

/// The Log is Send. The *mut u8 (`rawp`) is never dereferenced.
unsafe impl<T> Send for Log<T> where T: Sized + Clone {}

/// The Log is Sync. We know this because: `head` and `tail` are atomic variables, `append()`
/// reserves entries using a CAS, and exec() does not concurrently mutate entries on the log.
unsafe impl<T> Sync for Log<T> where T: Sized + Clone {}

impl<T> Log<T>
where
    T: Sized + Clone,
{
    /// Constructs and returns a log of (approximately) `num` entries.
    ///
    /// `num` is rounded to the next power of two or set to `2 * GC_FROM_HEAD` if smaller
    /// than that.
    ///
    /// # Example
    ///
    /// ```
    /// use node_replication::log::Log;
    ///
    /// // Operation type that will go onto the log.
    /// #[derive(Clone)]
    /// enum Operation {
    ///     Read,
    ///     Write(u64),
    ///     Invalid,
    /// }
    ///
    /// // Creates a log with 262144 entries.
    /// let l = Log::<Operation>::new_with_entries(1 << 18);
    /// ```
    ///
    /// This method allocates memory for the log upfront. No further allocations will be
    /// performed once this method returns.
    pub fn new_with_entries(num: usize) -> Log<T> {
        // Allocate the log
        let mut v = Vec::with_capacity(Log::<T>::entries_to_log_entries(num));
        for _ in 0..v.capacity() {
            v.push(Default::default());
        }

        // Convert it to a boxed slice, so we don't accidentially change the size
        let raw = v.into_boxed_slice();

        #[allow(clippy::declare_interior_mutable_const)]
        const LMASK_DEFAULT: CachePadded<Cell<bool>> = CachePadded::new(Cell::new(true));

        #[cfg(not(loom))]
        {
            #[allow(clippy::declare_interior_mutable_const)]
            const LTAIL_DEFAULT: CachePadded<AtomicUsize> = CachePadded::new(AtomicUsize::new(0));

            Log {
                slog: raw,
                head: CachePadded::new(AtomicUsize::new(0usize)),
                tail: CachePadded::new(AtomicUsize::new(0usize)),
                ctail: CachePadded::new(AtomicUsize::new(0usize)),
                ltails: [LTAIL_DEFAULT; MAX_REPLICAS_PER_LOG],
                next: CachePadded::new(AtomicUsize::new(1usize)),
                lmasks: [LMASK_DEFAULT; MAX_REPLICAS_PER_LOG],
            }
        }
        // `AtomicUsize::new` is not const in loom. This code block (including arr
        // dependency) becomes redundant once
        // https://github.com/tokio-rs/loom/issues/170 is fixed:
        #[cfg(loom)]
        {
            use arr_macro::arr;
            Log {
                slog: raw,
                head: CachePadded::new(AtomicUsize::new(0usize)),
                tail: CachePadded::new(AtomicUsize::new(0usize)),
                ctail: CachePadded::new(AtomicUsize::new(0usize)),
                ltails: arr![CachePadded::new(AtomicUsize::new(0)); 3], // MAX_REPLICAS_PER_LOG
                next: CachePadded::new(AtomicUsize::new(1usize)),
                lmasks: [LMASK_DEFAULT; MAX_REPLICAS_PER_LOG],
            }
        }
    }

    /// Constructs and returns a log of (approximately) `bytes` bytes.
    ///
    /// Will be rounded up if the provided `bytes` mean the log can hold less than
    /// `2*GC_FROM_HEAD` entries.
    ///
    /// This method allocates memory for the log upfront. No further allocations will be
    /// performed once this method returns.
    ///
    /// A log size of 1-2 MiB tends to works well in general.
    ///
    /// # Example
    ///
    /// ```
    /// use node_replication::log::Log;
    ///
    /// // Operation type that will be stored on the log.
    /// #[derive(Clone)]
    /// enum Operation {
    ///     Read,
    ///     Write(u64),
    ///     Invalid,
    /// }
    ///
    /// // Creates a ~1 MiB sized log.
    /// let l = Log::<Operation>::new_with_bytes(1 * 1024 * 1024);
    /// ```
    pub fn new_with_bytes(bytes: usize) -> Log<T> {
        Log::new_with_entries(Log::<T>::bytes_to_log_entries(bytes))
    }

    /// Determines the number of entries in the log. This is likely just `entries` rounded
    /// to the next power of two -- as long as it's above the minimal threshold required
    /// for the log to work (2*GC_FROM_HEAD).
    fn entries_to_log_entries(entries: usize) -> usize {
        core::cmp::max(2 * GC_FROM_HEAD, entries)
            .checked_next_power_of_two()
            .unwrap_or(2 * GC_FROM_HEAD)
    }

    /// Converts a size (in bytes), to the number of log entries required to fill this
    /// size in the log.
    ///
    /// The resulting amount of entries likely will occupy more space as the #log-entries
    /// needs to be rounded to a power-of-two.
    fn bytes_to_log_entries(bytes: usize) -> usize {
        // Calculate the number of entries that will go into the log, and retrieve a
        // slice to it from the allocated region of memory.
        // Make sure the log is large enough to allow for periodic garbage collection.
        let mut num = core::cmp::max(2 * GC_FROM_HEAD, bytes / Log::<T>::entry_size());

        // Round off to the next power of two if required. If we overflow, then set
        // the number of entries to the minimum required for GC. This is unlikely since
        // we'd need a log size > 2^63 entries for this to happen.
        if !num.is_power_of_two() {
            num = num.checked_next_power_of_two().unwrap_or(2 * GC_FROM_HEAD)
        }

        num
    }

    /// Returns the size of an entry in bytes.
    ///
    /// # TODO
    /// This method should be marked `const` once `#![feature(const_fn_trait_bound)]` is
    /// stabilized.
    ///
    /// See issue #93706 <https://github.com/rust-lang/rust/issues/93706> .
    fn entry_size() -> usize {
        size_of::<Cell<Entry<T>>>()
    }

    /// Registers a replica with the log. Returns an identifier that the replica can use
    /// to execute operations on the log.
    ///
    /// # Visibility
    /// There is no need to call this function from a client if you're not using the log /
    /// replica API directly.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use node_replication::log::Log;
    ///
    /// // Operation type that will go onto the log.
    /// #[derive(Clone)]
    /// enum Operation {
    ///    Read,
    ///    Write(u64),
    ///    Invalid,
    /// }
    ///
    /// // Creates a 1 Mega Byte sized log.
    /// let l = Log::<Operation>::new(1 * 1024 * 1024);
    ///
    /// // Registers against the log. `idx` can now be used to append operations
    /// // to the log, and execute these operations.
    /// let idx = l.register().expect("Failed to register with the Log.");
    /// ```
    pub fn register(&self) -> Option<LogToken> {
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

            return Some(LogToken(n));
        }
    }

    /// Adds a batch of operations to the shared log.
    ///
    /// # Example
    ///
    /// ```
    /// use node_replication::log::Log;
    ///
    /// // Operation type that will go onto the log.
    /// #[derive(Clone)]
    /// enum Operation {
    ///     Read,
    ///     Write(u64),
    /// }
    ///
    /// let l = Log::<Operation>::new_with_bytes(1 * 1024 * 1024);
    /// let idx = l.register().expect("Failed to register with the Log.");
    ///
    /// // The set of operations we would like to append. The order will
    /// // be preserved by the interface.
    /// let ops = [Operation::Write(100), Operation::Read];
    ///
    /// // `append()` might have to garbage collect the log. When doing so,
    /// // it might encounter operations added in by another replica/thread.
    /// // This closure allows us to consume those operations. `mine` identifies
    /// // if it was 'our` replica that added in those operations.
    /// let f = |op: Operation, mine: bool| {
    ///     match(op) {
    ///         Operation::Read => println!("Read by me? {}", mine),
    ///         Operation::Write(x) => println!("Write({}) by me? {}", x, mine),
    ///     }
    /// };
    ///
    /// // Append the operations. These operations will be marked with `idx`,
    /// // and will be linearized at the tail of the log.
    /// l.append(&ops, &idx, f);
    /// ```
    ///
    /// If there isn't enough space to perform the append, this method busy for
    /// a while waits to see if it can advance the head. Accepts a replica
    /// `idx`; all appended operations/entries will be marked with this
    /// replica-identifier. Also accepts a closure `s`; when waiting for GC,
    /// this closure is passed into exec() to ensure that this replica does'nt
    /// cause a deadlock.
    ///
    /// # Returns
    /// This will return Ok if all `ops` were successfully appended to the log.
    /// It might return `Ok(Some(usize))` if all operations were added, but we
    /// couldn't run GC after adding the ops (`usize` indicating which replica
    /// we're waiting for). This will return `Err(usize)` if the append failed
    /// because we waited too long for the replica indicated by the `usize`.
    ///
    /// # Note
    /// Documentation for this function is hidden since `append` is currently
    /// not intended as a public interface. It is marked as public due to being
    /// used by the benchmarking code.
    #[inline(always)]
    #[doc(hidden)]
    pub fn append<F: FnMut(T, bool)>(
        &self,
        ops: &[T],
        idx: &LogToken,
        mut s: F,
    ) -> Result<Option<usize>, usize> {
        let nops = ops.len();
        let mut iteration = 1;
        let mut waitgc = 1;

        // Keep trying to reserve entries and add operations to the log until
        // we succeed in doing so.
        loop {
            if iteration % WARN_THRESHOLD == 0 {
                let (min_replica_idx, _min_local_tail) = self.find_min_tail();
                warn!(
                    "append(ops.len()={}, {}) takes too many iterations ({}) to complete (waiting for {})...",
                    ops.len(),
                    idx.0,
                    iteration,
                    min_replica_idx,
                );
                let r = self.next.load(Ordering::Relaxed);
                let (mut min_replica_idx, mut min_local_tail) =
                    (0, self.ltails[0].load(Ordering::Relaxed));

                // Find the smallest local tail across all replicas.
                for idx in 1..r {
                    let cur_local_tail = self.ltails[idx - 1].load(Ordering::Relaxed);
                    info!(
                        "Replica {} cur_local_tail {} head {}.",
                        idx - 1,
                        cur_local_tail,
                        self.head.load(Ordering::Relaxed)
                    );

                    if cur_local_tail < min_local_tail {
                        min_local_tail = cur_local_tail;
                        min_replica_idx = idx - 1;
                    }
                }

                return Err(min_replica_idx);
            }
            iteration += 1;

            let tail = self.tail.load(Ordering::Relaxed);
            let head = self.head.load(Ordering::Relaxed);

            // If there are fewer than `GC_FROM_HEAD` entries on the log, then just
            // try again. The replica that reserved entry (h + self.slog.len() - GC_FROM_HEAD)
            // is currently trying to advance the head of the log. Keep refreshing the
            // replica against the log to make sure that it isn't deadlocking GC.
            if tail > head + self.slog.len() - GC_FROM_HEAD {
                if waitgc % WARN_THRESHOLD == 0 {
                    warn!(
                        "append(ops.len()={}, {}) takes too many iterations ({}) waiting for gc...",
                        ops.len(),
                        idx.0,
                        waitgc,
                    );
                    let (min_replica_idx, _min_local_tail) = self.find_min_tail();
                    return Err(min_replica_idx);
                }
                waitgc += 1;
                self.exec(idx, &mut s);
                self.advance_head(idx, &mut s)?;

                #[cfg(loom)]
                loom::thread::yield_now();
                continue;
            }

            // If on adding in the above entries there would be fewer than `GC_FROM_HEAD`
            // entries left on the log, then we need to advance the head of the log.
            let mut advance = false;
            if tail + nops > head + self.slog.len() - GC_FROM_HEAD {
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
                let e = self.slog[self.index(tail + i)].as_ptr();
                let mut m = self.lmasks[idx.0 - 1].get();

                // This entry was just reserved so it should be dead (!= m). However, if
                // the log has wrapped around, then the alive mask has flipped. In this
                // case, we flip the mask we were originally going to write into the
                // allocated entry. We cannot flip lmasks[idx - 1] because this replica
                // might still need to execute a few entries before the wrap around.
                if unsafe { (*e).alivef.load(Ordering::Relaxed) == m } {
                    m = !m;
                }

                unsafe { (*e).operation = Some(op.clone()) };
                unsafe { (*e).replica = idx.0 };
                unsafe { (*e).alivef.store(m, Ordering::Release) };
            }

            // If needed, advance the head of the log forward to make room on the log.
            return if advance {
                // If `advance_head()` fails to advance it will return the
                // replica we're waiting for as an error. If a client calls
                // `combine()` this isn't considered an error as we have
                // succesfully applied the operations. But, we should still make
                // sure to eventually `unstuck` the replica we waited for, so
                // we transform the error to an Ok(Option<usize>) to convey this
                // information to clients.
                match self.advance_head(idx, &mut s) {
                    Ok(_) => Ok(None),
                    Err(min_replica_idx) => Ok(Some(min_replica_idx)),
                }
            } else {
                Ok(None)
            };
        }
    }

    /// Executes a passed in closure (`d`) on all operations starting from a replica's
    /// local tail on the shared log. The replica is identified through an `idx` passed in
    /// as an argument.
    ///
    /// The passed in closure is expected to accept two arguments: The operation from the
    /// shared log to be executed and a bool that's true only if the operation was issued
    /// originally on this replica.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use node_replication::log::Log;
    ///
    /// // Operation type that will go onto the log.
    /// #[derive(Clone)]
    /// enum Operation {
    ///     Read,
    ///     Write(u64),
    /// }
    ///
    /// let l = Log::<Operation>::new(1 * 1024 * 1024);
    /// let idx = l.register().expect("Failed to register with the Log.");
    /// let ops = [Operation::Write(100), Operation::Read];
    ///
    /// let f = |op: Operation, id: usize| {
    ///     match(op) {
    ///         Operation::Read => println!("Read by {}", id),
    ///         Operation::Write(x) => println!("Write({}) by {}", x, id),
    ///     }
    /// };
    /// l.append(&ops, idx, f);
    ///
    /// // This closure is executed on every operation appended to the
    /// // since the last call to `exec()` by this replica/thread.
    /// let mut d = 0;
    /// let mut g = |op: Operation, id: usize| {
    ///     match(op) {
    ///         // The write happened before the read.
    ///         Operation::Read => assert_eq!(100, d),
    ///         Operation::Write(x) => d += 100,
    ///     }
    /// };
    /// l.exec(idx, &mut g);
    /// ```
    #[inline(always)]
    pub(crate) fn exec<F: FnMut(T, bool)>(&self, idx: &LogToken, d: &mut F) {
        // Load the logical log offset from which we must execute operations.
        let ltail = self.ltails[idx.0 - 1].load(Ordering::Relaxed);

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

        // Execute all operations from the passed in offset to the shared log's tail.
        // Check if the entry is live first; we could have a replica that has reserved
        // entries, but not filled them into the log yet.
        for i in ltail..gtail {
            let mut iteration = 1;
            let e = self.slog[self.index(i)].as_ptr();

            while unsafe { (*e).alivef.load(Ordering::Acquire) != self.lmasks[idx.0 - 1].get() } {
                if iteration % WARN_THRESHOLD == 0 {
                    warn!(
                        "alivef not being set for self.index(i={}) = {} (self.lmasks[{}] is {})...",
                        i,
                        self.index(i),
                        idx.0 - 1,
                        self.lmasks[idx.0 - 1].get()
                    );
                }
                iteration += 1;

                #[cfg(loom)]
                loom::thread::yield_now();
            }

            unsafe {
                d(
                    (*e).operation.as_ref().unwrap().clone(),
                    (*e).replica == idx.0,
                )
            };

            // Looks like we're going to wrap around now; flip this replica's local mask.
            if self.index(i) == self.slog.len() - 1 {
                self.lmasks[idx.0 - 1].set(!self.lmasks[idx.0 - 1].get());
                //trace!("idx: {} lmask: {}", idx, self.lmasks[idx - 1].get());
            }
        }

        // Update the completed tail after we've executed these operations. Also update
        // this replica's local tail.
        self.ctail.fetch_max(gtail, Ordering::Relaxed);
        self.ltails[idx.0 - 1].store(gtail, Ordering::Relaxed);
    }

    /// Returns a physical index given a logical index into the shared log.
    #[inline(always)]
    fn index(&self, logical: usize) -> usize {
        logical & (self.slog.len() - 1)
    }

    /// Loops over all `ltails` and finds the replica with the lowest tail.
    ///
    /// # Returns
    /// The ID (in `LogToken`) of the replica with the lowest tail and the
    /// corresponding/lowest tail `idx` in the `Log`.
    fn find_min_tail(&self) -> (usize, usize) {
        let r = self.next.load(Ordering::Relaxed);
        let (mut min_replica_idx, mut min_local_tail) = (0, self.ltails[0].load(Ordering::Relaxed));

        // Find the smallest local tail across all replicas.
        for idx in 1..r {
            let cur_local_tail = self.ltails[idx - 1].load(Ordering::Relaxed);
            //info!("Replica {} cur_local_tail {}.", idx - 1, cur_local_tail);

            if cur_local_tail < min_local_tail {
                min_local_tail = cur_local_tail;
                min_replica_idx = idx - 1;
            }
        }

        (min_replica_idx, min_local_tail)
    }

    /// Advances the head of the log forward. If a replica has stopped making progress,
    /// then this method will never return. Accepts a closure that is passed into exec()
    /// to ensure that this replica does not deadlock GC.
    #[inline(always)]
    fn advance_head<F: FnMut(T, bool)>(&self, rid: &LogToken, mut s: &mut F) -> Result<(), usize> {
        // Keep looping until we can advance the head and create some free space
        // on the log. If one of the replicas has stopped making progress, then
        // this method might never return.
        let mut iteration = 1;
        loop {
            let global_head = self.head.load(Ordering::Relaxed);
            let f = self.tail.load(Ordering::Relaxed);
            let (min_replica_idx, min_local_tail) = self.find_min_tail();

            // If we cannot advance the head further, then start
            // from the beginning of this loop again. Before doing so, try consuming
            // any new entries on the log to prevent deadlock.
            if min_local_tail == global_head {
                if iteration % WARN_THRESHOLD == 0 {
                    warn!("Spending a long time in `advance_head`, are we starving (min_replica_idx = {})?", min_replica_idx);
                    return Err(min_replica_idx);
                }
                iteration += 1;
                self.exec(rid, &mut s);

                #[cfg(loom)]
                loom::thread::yield_now();
                continue;
            }

            // There are entries that can be freed up; update the head offset.
            self.head.store(min_local_tail, Ordering::Relaxed);

            // Make sure that we freed up enough space so that threads waiting for
            // GC in append can make progress. Otherwise, try to make progress again.
            // If we're making progress again, then try consuming entries on the log.
            if f < min_local_tail + self.slog.len() - GC_FROM_HEAD {
                return Ok(());
            } else {
                self.exec(rid, &mut s);
            }
        }
    }

    /// Resets the log. This is required for microbenchmarking the log; with this method,
    /// we can re-use the log across experimental runs without having to re-allocate the
    /// log over and over again (which blows up the tests runtime due to allocations /
    /// initializing the log memory).
    ///
    /// # Safety
    ///
    /// *To be used for testing/benchmarking only, hence marked unsafe*. Before calling
    /// this method, please make sure that there aren't any replicas/threads actively
    /// issuing/executing operations to/from this log or having potentially outstanding
    /// operations not applied on all replicas.
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

        // Next, free up all log entries. Use pointers to avoid memcpy and speed up the
        // reset of the log here.
        for i in 0..self.slog.len() {
            let e = self.slog[self.index(i)].as_ptr();
            (*e).alivef.store(false, Ordering::Release);
        }
    }

    /// This method checks if the replica is in sync to execute a read-only operation
    /// right away. It does so by comparing the replica's local tail with the log's
    /// completed tail.
    ///
    /// # Example
    ///
    /// Can't execute this until we have access to `pub(crate)` in tests.
    ///
    /// ```ignore
    /// use node_replication::log::Log;
    ///
    /// // Operation type that will go onto the log.
    /// #[derive(Clone)]
    /// enum Operation {
    ///     Read,
    ///     Write(u64),
    /// }
    ///
    /// // We register two replicas here, `idx1` and `idx2`.
    /// let l = Log::<Operation>::new_with_bytes(1 * 1024 * 1024);
    /// let idx1 = l.register().expect("Failed to register with the Log.");
    /// let idx2 = l.register().expect("Failed to register with the Log.");
    /// let ops = [Operation::Write(100), Operation::Read];
    ///
    /// let f = |op: Operation, mine: bool| {
    ///     match(op) {
    ///         Operation::Read => println!("Read by {}", mine),
    ///         Operation::Write(x) => println!("Write({}) done by me? {}", x, mine),
    ///     }
    /// };
    /// l.append(&ops, &idx2, f);
    ///
    /// let mut d = 0;
    /// let mut g = |op: Operation, _mind: bool| {
    ///     match(op) {
    ///         // The write happened before the read.
    ///         Operation::Read => assert_eq!(100, d),
    ///         Operation::Write(x) => d += 100,
    ///     }
    /// };
    /// l.exec(&idx2, &mut g);
    ///
    /// // This assertion fails because `idx1` has not executed operations
    /// // that were appended by `idx2`.
    /// assert_eq!(false, l.is_replica_synced_for_reads(&idx1, l.get_ctail()));
    ///
    /// let mut e = 0;
    /// let mut g = |op: Operation, mine: bool| {
    ///     match(op) {
    ///         // The write happened before the read.
    ///         Operation::Read => assert_eq!(100, e),
    ///         Operation::Write(x) => e += 100,
    ///     }
    /// };
    /// l.exec(&idx1, &mut g);
    ///
    /// // `idx1` is all synced up, so this assertion passes.
    /// assert_eq!(true, l.is_replica_synced_for_reads(&idx1, l.get_ctail()));
    /// ```
    #[inline(always)]
    pub(crate) fn is_replica_synced_for_reads(&self, idx: &LogToken, ctail: usize) -> bool {
        self.ltails[idx.0 - 1].load(Ordering::Relaxed) >= ctail
    }

    /// This method returns the current ctail value for the log.
    #[inline(always)]
    pub(crate) fn get_ctail(&self) -> usize {
        self.ctail.load(Ordering::Relaxed)
    }
}

impl<T> Default for Log<T>
where
    T: Sized + Clone,
{
    /// Default constructor for the shared log.
    ///
    /// Constructs a log of approximately [`DEFAULT_LOG_BYTES`] bytes.
    fn default() -> Self {
        Log::new_with_bytes(DEFAULT_LOG_BYTES)
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
        let l = Log::<Operation>::new_with_bytes(1024 * 1024);
        let n = (1024 * 1024) / Log::<Operation>::entry_size();
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
        let l = Log::<Operation>::new_with_bytes(1024);
        assert_eq!(l.slog.len(), 2 * GC_FROM_HEAD);
    }

    // Tests if the constructor allocates enough space for GC.
    #[test]
    fn test_log_min_size2() {
        let l = Log::<Operation>::new_with_entries(1);
        assert_eq!(l.slog.len(), 2 * GC_FROM_HEAD);
    }

    // Tests that the constructor allocates a log whose number of entries
    // are a power of two.
    #[test]
    fn test_log_power_of_two() {
        let l = Log::<Operation>::new_with_bytes(524 * 1024);
        let n = ((524 * 1024) / Log::<Operation>::entry_size()).checked_next_power_of_two();
        assert_eq!(l.slog.len(), n.unwrap());
    }

    // Tests that the constructor allocates a log whose number of entries
    // are a power of two.
    #[test]
    fn test_log_power_of_two2() {
        let l = Log::<Operation>::new_with_entries(524 * 1024);
        let n = (524 * 1024usize).checked_next_power_of_two();
        assert_eq!(l.slog.len(), n.unwrap());
    }

    // Tests if the log can be successfully default constructed.
    #[test]
    fn test_log_create_default() {
        let l = Log::<Operation>::default();
        let n = DEFAULT_LOG_BYTES / Log::<Operation>::entry_size();
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
        let l = Log::<Operation>::new_with_bytes(2 * 1024 * 1024);
        assert_eq!(l.index(99000), 696);
    }

    // Tests if we can correctly register with the shared log.
    #[test]
    fn test_log_register() {
        let l = Log::<Operation>::new_with_bytes(1024);
        assert_eq!(l.register(), Some(LogToken(1)));
        assert_eq!(l.next.load(Ordering::Relaxed), 2);
    }

    // Tests that we cannot register more than the max replicas with the log.
    #[test]
    fn test_log_register_none() {
        let l = Log::<Operation>::new_with_bytes(1024);
        l.next.store(MAX_REPLICAS_PER_LOG, Ordering::Relaxed);
        assert!(l.register().is_none());
        assert_eq!(l.next.load(Ordering::Relaxed), MAX_REPLICAS_PER_LOG);
    }

    // Test that we can correctly append an entry into the log.
    #[test]
    fn test_log_append() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o = [Operation::Read];
        assert!(l.append(&o, &lt, |_o: Operation, _mine: bool| {}).is_ok());

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
        let lt = l.register().unwrap();

        let o = [Operation::Read, Operation::Write(119)];
        assert!(l.append(&o, &lt, |_o: Operation, _mine: bool| {}).is_ok());

        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 2);
    }

    // Tests that we can advance the head of the log to the smallest of all replica-local tails.
    #[test]
    fn test_log_advance_head() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        l.next.store(5, Ordering::Relaxed);
        l.ltails[0].store(1023, Ordering::Relaxed);
        l.ltails[1].store(224, Ordering::Relaxed);
        l.ltails[2].store(4096, Ordering::Relaxed);
        l.ltails[3].store(799, Ordering::Relaxed);

        assert!(l
            .advance_head(&lt, &mut |_o: Operation, _mine: bool| {})
            .is_ok());
        assert_eq!(l.head.load(Ordering::Relaxed), 224);
    }

    // Tests that the head of the log is advanced when we're close to filling up the entire log.
    #[test]
    fn test_log_append_gc() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o: [Operation; 4] = unsafe {
            let mut a: [Operation; 4] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, Operation::Read);
            }
            a
        };

        l.next.store(2, Ordering::Relaxed);
        l.tail
            .store(l.slog.len() - GC_FROM_HEAD - 1, Ordering::Relaxed);
        l.ltails[0].store(1024, Ordering::Relaxed);
        assert!(l.append(&o, &lt, |_o: Operation, _mine: bool| {}).is_ok());

        assert_eq!(l.head.load(Ordering::Relaxed), 1024);
        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.slog.len() - GC_FROM_HEAD + 3
        );
    }

    // Tests that on log wrap around, the local mask stays
    // the same because entries have not been executed yet.
    #[test]
    fn test_log_append_wrap() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o: [Operation; 1024] = unsafe {
            let mut a: [Operation; 1024] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, Operation::Read);
            }
            a
        };

        l.next.store(2, Ordering::Relaxed);
        l.head.store(2 * 8192, Ordering::Relaxed);
        l.tail.store(l.slog.len() - 10, Ordering::Relaxed);
        assert!(l.append(&o, &lt, |_o: Operation, _mine: bool| {}).is_ok());

        assert_eq!(l.lmasks[0].get(), true);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.slog.len() + 1014);
    }

    // Test that we can execute operations appended to the log.
    #[test]
    fn test_log_exec() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o = [Operation::Read];
        let mut f = |op: Operation, mine: bool| {
            assert_eq!(op, Operation::Read);
            assert!(mine);
        };

        assert!(l.append(&o, &lt, |_o: Operation, _mine: bool| {}).is_ok());
        l.exec(&lt, &mut f);

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
        let lt = l.register().unwrap();

        let mut f = |_o: Operation, _mine| {
            assert!(false);
        };

        l.exec(&lt, &mut f);
    }

    // Test that exec() doesn't do anything if we're already up-to-date.
    #[test]
    fn test_log_exec_zero() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o = [Operation::Read];
        let mut f = |op: Operation, mine: bool| {
            assert_eq!(op, Operation::Read);
            assert!(mine);
        };
        let mut g = |_op: Operation, _mine: bool| {
            assert!(false);
        };

        assert!(l.append(&o, &lt, |_o: Operation, _mine| {}).is_ok());
        l.exec(&lt, &mut f);
        l.exec(&lt, &mut g);
    }

    // Test that multiple entries on the log can be executed correctly.
    #[test]
    fn test_log_exec_multiple() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o = [Operation::Read, Operation::Write(119)];
        let mut s = 0;
        let mut f = |op: Operation, _mine| match op {
            Operation::Read => s += 121,
            Operation::Write(v) => s += v,
            Operation::Invalid => assert!(false),
        };

        assert!(l.append(&o, &lt, |_o: Operation, _mine: bool| {}).is_ok());
        l.exec(&lt, &mut f);
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
        let lt = l.register().unwrap();

        let o: [Operation; 1024] = unsafe {
            let mut a: [Operation; 1024] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, Operation::Read);
            }
            a
        };
        let mut f = |op: Operation, mine: bool| {
            assert_eq!(op, Operation::Read);
            assert!(mine);
        };

        assert!(l.append(&o, &lt, |_o: Operation, _mine| {}).is_ok()); // Required for GC to work correctly.
        l.next.store(2, Ordering::SeqCst);
        l.head.store(2 * 8192, Ordering::SeqCst);
        l.tail.store(l.slog.len() - 10, Ordering::SeqCst);
        assert!(l.append(&o, &lt, |_o: Operation, _mine| {}).is_ok());

        l.ltails[0].store(l.slog.len() - 10, Ordering::SeqCst);
        l.exec(&lt, &mut f);

        assert_eq!(l.lmasks[0].get(), false);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.slog.len() + 1014);
    }

    // Tests that exec() panics if the head of the log advances beyond the tail.
    #[test]
    #[should_panic]
    fn test_exec_panic() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o: [Operation; 1024] = unsafe {
            let mut a: [Operation; 1024] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, Operation::Read);
            }
            a
        };
        let mut f = |_op: Operation, _mine| {
            assert!(false);
        };

        assert!(l.append(&o, &lt, |_o: Operation, _mine| {}).is_ok());
        l.head.store(8192, Ordering::SeqCst);

        l.exec(&lt, &mut f);
    }

    // Tests that operations are cloned when added to the log, and that
    // they are correctly dropped once overwritten.
    #[test]
    fn test_log_change_refcount() {
        let l = Log::<Arc<Operation>>::default();
        let lt = l.register().unwrap();

        let o1 = [Arc::new(Operation::Read)];
        let o2 = [Arc::new(Operation::Read)];
        assert_eq!(Arc::strong_count(&o1[0]), 1);
        assert_eq!(Arc::strong_count(&o2[0]), 1);

        assert!(l
            .append(&o1[..], &lt, |_o: Arc<Operation>, _mine| {})
            .is_ok());
        assert_eq!(Arc::strong_count(&o1[0]), 2);
        assert!(l
            .append(&o1[..], &lt, |_o: Arc<Operation>, _mine| {})
            .is_ok());
        assert_eq!(Arc::strong_count(&o1[0]), 3);

        unsafe { l.reset() };

        // Over here, we overwrite entries that were written to by the two
        // previous appends. This decreases the refcount of o1 and increases
        // the refcount of o2.
        assert!(l
            .append(&o2[..], &lt, |_o: Arc<Operation>, _mine| {})
            .is_ok());
        assert_eq!(Arc::strong_count(&o1[0]), 2);
        assert_eq!(Arc::strong_count(&o2[0]), 2);
        assert!(l
            .append(&o2[..], &lt, |_o: Arc<Operation>, _mine| {})
            .is_ok());
        assert_eq!(Arc::strong_count(&o1[0]), 1);
        assert_eq!(Arc::strong_count(&o2[0]), 3);
    }

    // Tests that operations are cloned when added to the log, and that
    // they are correctly dropped once overwritten after the GC.
    #[test]
    fn test_log_refcount_change_with_gc() {
        let entry_size = 64;
        let total_entries = 16384;

        assert_eq!(Log::<Arc<Operation>>::entry_size(), entry_size);
        let size: usize = total_entries * entry_size;
        let l = Log::<Arc<Operation>>::new_with_bytes(size);
        let lt = l.register().unwrap();
        let o1 = [Arc::new(Operation::Read)];
        let o2 = [Arc::new(Operation::Read)];
        assert_eq!(Arc::strong_count(&o1[0]), 1);
        assert_eq!(Arc::strong_count(&o2[0]), 1);

        for i in 1..(total_entries + 1) {
            assert!(l
                .append(&o1[..], &lt, |_o: Arc<Operation>, _mine| {})
                .is_ok());
            assert_eq!(Arc::strong_count(&o1[0]), i + 1);
        }
        assert_eq!(Arc::strong_count(&o1[0]), total_entries + 1);

        for i in 1..(total_entries + 1) {
            assert!(l
                .append(&o2[..], &lt, |_o: Arc<Operation>, _mine| {})
                .is_ok());
            assert_eq!(Arc::strong_count(&o1[0]), (total_entries + 1) - i);
            assert_eq!(Arc::strong_count(&o2[0]), i + 1);
        }
        assert_eq!(Arc::strong_count(&o1[0]), 1);
        assert_eq!(Arc::strong_count(&o2[0]), total_entries + 1);
    }

    // Tests that is_replica_synced_for_read() works correctly; it returns
    // false when a replica is not synced up and true when it is.
    #[test]
    fn test_replica_synced_for_read() {
        let l = Log::<Operation>::default();
        let one = l.register().unwrap();
        let two = l.register().unwrap();

        assert_eq!(one, LogToken(1));
        assert_eq!(two, LogToken(2));

        let o = [Operation::Read];
        let mut f = |op: Operation, mine: bool| {
            assert_eq!(op, Operation::Read);
            assert!(mine);
        };

        assert!(l.append(&o, &one, |_o: Operation, _mine| {}).is_ok());
        l.exec(&one, &mut f);
        assert_eq!(l.is_replica_synced_for_reads(&one, l.get_ctail()), true);
        assert_eq!(l.is_replica_synced_for_reads(&two, l.get_ctail()), false);

        let mut f = |op: Operation, mine: bool| {
            assert_eq!(op, Operation::Read);
            assert!(!mine);
        };
        l.exec(&two, &mut f);
        assert_eq!(l.is_replica_synced_for_reads(&two, l.get_ctail()), true);
    }
}
