// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use alloc::alloc::{alloc, dealloc, Layout};

use core::cell::Cell;
use core::default::Default;
use core::fmt;
use core::mem::{align_of, size_of};
use core::ops::{Drop, FnMut};
use core::slice::from_raw_parts_mut;
use core::sync::atomic::{compiler_fence, AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

/// The default size of the shared log in bytes. If constructed using the
/// default constructor, the log will be these many bytes in size. Currently
/// set to 1 GB based on the ASPLOS 2017 paper.
const DEFAULT_LOG_BYTES: usize = 1024 * 1024 * 1024;

/// The maximum number of replicas that can be used against the log.
const MAX_REPLICAS: usize = 64;

/// Constant required for garbage collection. When the tail and the head are
/// these many entries apart on the circular buffer, garbage collection will
/// be performed by one of the replicas registered with the log.
const GC_FROM_HEAD: usize = 1024 * 4;

/// Threshold after how many iterations we log a warning for busy spinning loops.
///
/// This helps with debugging to figure out where things may end up blocking.
/// Should be a power of two to avoid divisions.
const WARN_THRESHOLD: usize = 1 << 28;

/// An entry that sits on the log. Each entry consists of two fields: The operation to
/// be performed when a thread reaches this entry on the log, and a flag indicating whether
/// this entry is valid.
///
/// `T` is the type on the operation - typically an enum class containing opcodes as well as
/// arguments. It is required that this type be sized, cloneable, and default constructable.
///
/// `replica` identifies the replica that issued the above operation.
///
/// `alivef` indicates whether this entry represents a valid operation when on the log.
#[derive(Clone, Copy, Default)]
#[repr(align(64))]
struct Entry<T>
where
    T: Sized + Clone,
{
    operation: Option<T>,

    replica: usize,

    alivef: bool,
}

/// A log of operations that can be shared between multiple NUMA nodes.
///
/// Operations can be added to the log by calling the `append()` method and
/// providing a list of operations to be performed.
///
/// Operations already on the log can be executed by calling the `exec()` method
/// and providing a replica-id and a closure. Operations added to the log since the
/// replica last called `exec()` will be executed by invoking the supplied closure over
/// each one of them.
///
/// Accepts one type parameter; `T` defines the type of operations and their arguments
/// that will go on the log and would typically be an enum class.
///
/// This struct is cache aligned to 64 bytes.
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

    /// A reference to the actual log. Nothing but a slice of entries.
    slog: &'a [Cell<Entry<T>>],

    /// Logical index into the above slice at which the log starts.
    head: CachePadded<AtomicUsize>,

    /// Logical index into the above slice at which the log ends.
    /// New appends go here.
    tail: CachePadded<AtomicUsize>,

    /// Completed tail maintains an index <= tail that points to a
    /// log entry after which there are no completed operations.
    ctail: CachePadded<AtomicUsize>,

    /// Array consisting of the local tail of each replica registered with the log.
    /// Required for garbage collection; since replicas make progress over the log
    /// independently, we want to make sure that we don't garbage collect operations
    /// that haven't been executed by all replicas.
    ltails: [CachePadded<AtomicUsize>; MAX_REPLICAS],

    /// Identifier that will be allocated to the next replica that registers with
    /// this Log. Required to index into ltails above.
    next: CachePadded<AtomicUsize>,

    /// Array consisting of local alive masks for each registered replica. Required
    /// because replicas make independent progress over the log, so we need to
    /// track wrap-arounds for each of their local tails.
    lmasks: [CachePadded<Cell<bool>>; MAX_REPLICAS],
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
    /// Constructs and returns a log of size `bytes` bytes. This method also allocates
    /// memory for the log upfront. No further allocations will be performed once this
    /// method returns.
    pub fn new<'b>(bytes: usize) -> Log<'b, T> {
        use arr_macro::arr;

        let mem = unsafe {
            alloc(
                Layout::from_size_align(bytes, align_of::<Cell<Entry<T>>>())
                    .expect("Alignment error while allocating the shared log!"),
            )
        };
        if mem.is_null() {
            panic!("Failed to allocate memory for the shared log!");
        }

        // Calculate the number of entries that will go into the log, and retrieve a
        // slice to it from the allocated region of memory.
        let num = bytes / Log::<T>::entry_size();
        if !num.is_power_of_two() {
            panic!("Log size should be a power of two.")
        };
        let raw = unsafe { from_raw_parts_mut(mem as *mut Cell<Entry<T>>, num) };

        // Initialize all log entries by calling the default constructor.
        for e in &mut raw[..] {
            unsafe {
                ::core::ptr::write(
                    e,
                    Cell::new(Entry {
                        operation: None,
                        replica: 0usize,
                        alivef: false,
                    }),
                );
            }
        }

        let fls: [CachePadded<Cell<bool>>; MAX_REPLICAS] = arr![Default::default(); 64];
        for idx in 0..MAX_REPLICAS {
            fls[idx].set(true)
        }

        Log {
            rawp: mem,
            rawb: bytes,
            size: num,
            slog: raw,
            head: CachePadded::new(AtomicUsize::new(0usize)),
            tail: CachePadded::new(AtomicUsize::new(0usize)),
            ctail: CachePadded::new(AtomicUsize::new(0usize)),
            ltails: arr![Default::default(); 64],
            next: CachePadded::new(AtomicUsize::new(1usize)),
            lmasks: fls,
        }
    }

    /// Returns the size of an entry in bytes.
    fn entry_size() -> usize {
        size_of::<Cell<Entry<T>>>()
    }

    /// Registers a replica against the log. Returns an identifier that the replica
    /// can to execute operations on the log.
    pub fn register(&self) -> Option<usize> {
        // Loop until we either run out of identifiers or we manage to increment `next`.
        loop {
            let n = self.next.load(Ordering::Relaxed);

            // Check if we've exceeded the maximum number of replicas the log can support.
            if n >= MAX_REPLICAS {
                return None;
            };

            if self.next.compare_and_swap(n, n + 1, Ordering::SeqCst) != n {
                continue;
            };

            return Some(n);
        }
    }

    /// Adds a batch of operations to the shared log. If there isn't enough space
    /// to perform the append, this method busy waits until the head is advanced.
    /// Accepts a replica `idx`; all appended operations/entries will be marked
    /// with this replica-identifier. Also accepts a closure `s`; when waiting for
    /// GC, this closure is passed into exec() to ensure that this replica does'nt
    /// cause a deadlock.
    pub fn append<F: FnMut(T, usize)>(&self, ops: &[T], idx: usize, mut s: F) {
        let nops = ops.len();
        let mut iteration = 1;
        let mut waitgc = 1;

        // Keep trying to reserve entries and add operations to the log until
        // we succeed in doing so.
        loop {
            if iteration % WARN_THRESHOLD == 0 {
                warn!(
                    "{:?} append(ops.len()={}, {}) takes too many iterations ({}) to complete...",
                    std::thread::current().id(),
                    ops.len(),
                    idx,
                    iteration,
                );
            }
            iteration += 1;

            let tail = self.tail.load(Ordering::Relaxed);
            let head = self.head.load(Ordering::Relaxed);

            // If there are fewer than `GC_FROM_HEAD` entries on the log, then just
            // try again. The replica that reserved entry (h + self.size - GC_FROM_HEAD)
            // is currently trying to advance the head of the log. Keep refreshing the
            // replica against the log to make sure that it isn't deadlocking GC.
            if tail > head + self.size - GC_FROM_HEAD {
                if waitgc % WARN_THRESHOLD == 0 {
                    warn!(
                        "{:?} append(ops.len()={}, {}) takes too many iterations ({}) waiting for gc...",
                        std::thread::current().id(),
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
            if self
                .tail
                .compare_and_swap(tail, tail + nops, Ordering::SeqCst)
                != tail
            {
                continue;
            };

            // Successfully reserved entries on the shared log. Add the operations in.
            for i in 0..nops {
                let e = self.slog[self.index(tail + i)].as_ptr();
                let mut m = self.lmasks[idx - 1].get();

                // This entry was just reserved so it should be dead (!= m). However, if
                // the log has wrapped around, then the alive mask has flipped. In this
                // case, we flip the mask we were originally going to write into the
                // allocated entry. We cannot flip lmasks[idx - 1] because this replica
                // might still need to execute a few entries before the wrap around.
                if unsafe { (*e).alivef == m } {
                    m = !m;
                }

                unsafe { (*e).operation = Some(ops[i].clone()) };
                unsafe { (*e).replica = idx };
                compiler_fence(Ordering::AcqRel);
                unsafe { (*e).alivef = m };
            }

            // If needed, advance the head of the log forward to make room on the log.
            if advance {
                self.advance_head(idx, &mut s);
            }

            return;
        }
    }

    /// Executes a passed in closure (`d`) on all operations starting from
    /// a replica's local tail on the shared log. The replica is identified through an
    /// `idx` passed in as an argument.
    ///
    /// The passed in closure is expected to take in two arguments: The operation
    /// from the shared log to be executed and the replica that issued it.
    pub fn exec<F: FnMut(T, usize)>(&self, idx: usize, d: &mut F) {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);

        // Load the logical log offset from which we must execute operations.
        let local_tail = self.ltails[idx - 1].load(Ordering::Relaxed);

        // Make sure we're within the shared log. If we aren't, then panic.
        if local_tail > tail || local_tail < head {
            panic!("Local tail not within the shared log!")
        };

        // Execute all operations from the passed in offset to the shared log's tail. Check if
        // the entry is live first; we could have a replica that has reserved entries, but not
        // filled them into the log yet.
        for i in local_tail..tail {
            let mut iteration = 1;
            let e = self.slog[self.index(i)].as_ptr();

            while unsafe { (*e).alivef != self.lmasks[idx - 1].get() } {
                if iteration % WARN_THRESHOLD == 0 {
                    warn!(
                        "{:?} alivef not being set for self.index(i={}) = {} (self.lmasks[{}] is {})...",
                        std::thread::current().id(),
                        i,
                        self.index(i),
                        idx - 1,
                        self.lmasks[idx - 1].get()
                    );
                }
                iteration += 1;
            }

            unsafe { d((*e).operation.as_ref().unwrap().clone(), (*e).replica) };

            // Looks like we're going to wrap around now; flip this replica's local mask.
            if self.index(i) == self.size - 1 {
                self.lmasks[idx - 1].set(!self.lmasks[idx - 1].get());
                trace!("idx: {} lmask: {}", idx, self.lmasks[idx - 1].get());
            }
        }

        self.ctail.fetch_max(tail, Ordering::Relaxed);

        // Update the replica's local tail.
        self.ltails[idx - 1].store(tail, Ordering::Relaxed);
    }

    /// Returns a physical index given a logical index into the shared log.
    #[inline(always)]
    fn index(&self, logical: usize) -> usize {
        logical & (self.size - 1)
    }

    /// Advances the head of the log forward. If a replica has stopped making progress,
    /// then this method will never return. Accepts a closure that is passed into exec()
    /// to ensure that this replica does not deadlock GC.
    fn advance_head<F: FnMut(T, usize)>(&self, rid: usize, mut s: &mut F) {
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
                    warn!(
                        "{:?} Spending a long time in `advance_head`, are we starving?",
                        std::thread::current().id()
                    );
                }
                iteration += 1;
                self.exec(rid, &mut s);
                continue;
            }

            // There are entries that can be freed up; update the head offset.
            self.head.store(min_local_tail, Ordering::Relaxed);

            // Make sure that we freed up enough space so that threads waiting for
            // GC in append can make progress. Otherwise, try to make progress again.
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
    #[inline(always)]
    pub unsafe fn reset(&self) {
        // First, reset global metadata.
        self.head.store(0, Ordering::SeqCst);
        self.tail.store(0, Ordering::SeqCst);
        self.next.store(1, Ordering::SeqCst);

        // Next, reset replica-local metadata.
        for r in 0..MAX_REPLICAS {
            self.ltails[r].store(0, Ordering::Relaxed);
            self.lmasks[r].set(true);
        }

        // Next, free up all log entries. Use pointers to avoid memcpy and speed up
        // the reset of the log here.
        for i in 0..self.size {
            let e = self.slog[self.index(i)].as_ptr();
            (*e).alivef = false;
        }
    }

    /// This method checks if the replica is in sync to execute a read-only operation
    /// right away. And to check the same, it compares the localTail with completedTail.
    #[inline(always)]
    pub fn is_replica_synced_for_reads(&self, idx: usize) -> bool {
        let read_tail = self.ctail.load(Ordering::Relaxed);
        let local_tail = self.ltails[idx - 1].load(Ordering::Relaxed);
        local_tail >= read_tail
    }
}

impl<'a, T> Default for Log<'a, T>
where
    T: Sized + Clone,
{
    /// Default constructor for the shared log.
    fn default() -> Self {
        Log::new(DEFAULT_LOG_BYTES)
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
        assert_eq!(e.alivef, false);
    }

    // Test that our entry_size() method returns the correct size.
    #[test]
    fn test_log_entry_size() {
        assert_eq!(Log::<Operation>::entry_size(), 64);
    }

    // Tests if a small log can be correctly constructed.
    #[test]
    fn test_log_create() {
        let l = Log::<Operation>::new(1024);
        let n = 1024 / Log::<Operation>::entry_size();
        assert_eq!(l.rawb, 1024);
        assert_eq!(l.size, n);
        assert_eq!(l.slog.len(), n);
        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 0);
        assert_eq!(l.next.load(Ordering::Relaxed), 1);

        for i in 0..MAX_REPLICAS {
            assert_eq!(l.ltails[i].load(Ordering::Relaxed), 0);
        }

        for i in 0..MAX_REPLICAS {
            assert_eq!(l.lmasks[i].get(), true);
        }
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

        for i in 0..MAX_REPLICAS {
            assert_eq!(l.ltails[i].load(Ordering::Relaxed), 0);
        }

        for i in 0..MAX_REPLICAS {
            assert_eq!(l.lmasks[i].get(), true);
        }
    }

    // Tests if we can correctly index into the shared log.
    #[test]
    fn test_log_index() {
        let l = Log::<Operation>::new(4096);
        assert_eq!(l.index(104), 40);
    }

    // Tests if we can correctly register with the shared log.
    #[test]
    fn test_log_register() {
        let l = Log::<Operation>::new(1024);
        assert_eq!(l.register(), Some(1));
        assert_eq!(l.next.load(Ordering::Relaxed), 2);
    }

    // Tests that we cannot register more than 32 replicas with the log.
    #[test]
    fn test_log_register_none() {
        let l = Log::<Operation>::new(1024);
        l.next.store(64, Ordering::Relaxed);
        assert!(l.register().is_none());
        assert_eq!(l.next.load(Ordering::Relaxed), 64);
    }

    // Test that we can correctly append an entry into the log.
    #[test]
    fn test_log_append() {
        let l = Log::<Operation>::default();
        let o = [Operation::Read];
        l.append(&o, 1, |_o: Operation, _i: usize| {});

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
        let o = [Operation::Read, Operation::Write(119)];
        l.append(&o, 1, |_o: Operation, _i: usize| {});

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

        l.advance_head(0, &mut |_o: Operation, _i: usize| {});
        assert_eq!(l.head.load(Ordering::Relaxed), 224);
    }

    // Tests that the head of the log is advanced when we're close to filling up the entire log.
    #[test]
    fn test_log_append_gc() {
        let l = Log::<Operation>::default();
        let o: [Operation; 4] = unsafe {
            let mut a: [Operation; 4] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, Operation::Read);
            }
            a
        };

        l.next.store(2, Ordering::Relaxed);
        l.tail.store(l.size - GC_FROM_HEAD - 1, Ordering::Relaxed);
        l.ltails[0].store(1024, Ordering::Relaxed);
        l.append(&o, 1, |_o: Operation, _i: usize| {});

        assert_eq!(l.head.load(Ordering::Relaxed), 1024);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.size - GC_FROM_HEAD + 3);
    }

    // Tests that on log wrap around, the global mask gets flipped, but the local mask stays
    // the same because entries have not been executed yet.
    #[test]
    fn test_log_append_wrap() {
        let l = Log::<Operation>::default();
        let o: [Operation; 1024] = unsafe {
            let mut a: [Operation; 1024] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, Operation::Read);
            }
            a
        };

        l.next.store(2, Ordering::Relaxed);
        l.head.store(8192, Ordering::Relaxed);
        l.tail.store(l.size - 10, Ordering::Relaxed);
        l.append(&o, 1, |_o: Operation, _i: usize| {});

        assert_eq!(l.lmasks[0].get(), true);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.size + 1014);
    }

    // Test that we can execute operations appended to the log.
    #[test]
    fn test_log_exec() {
        let l = Log::<Operation>::default();
        let o = [Operation::Read];
        let mut f = |op: Operation, i: usize| {
            assert_eq!(op, Operation::Read);
            assert_eq!(i, 1);
        };

        l.append(&o, 1, |_o: Operation, _i: usize| {});
        l.exec(1, &mut f);
    }

    // Test that exec() doesn't do anything when the log is empty.
    #[test]
    fn test_log_exec_empty() {
        let l = Log::<Operation>::default();
        let mut f = |_o: Operation, _i: usize| {
            assert!(false);
        };

        l.exec(1, &mut f);
    }

    // Test that exec() doesn't do anything if we're already up-to-date.
    #[test]
    fn test_log_exec_zero() {
        let l = Log::<Operation>::default();
        let o = [Operation::Read];
        let mut f = |op: Operation, i: usize| {
            assert_eq!(op, Operation::Read);
            assert_eq!(i, 1);
        };
        let mut g = |_op: Operation, _i: usize| {
            assert!(false);
        };

        l.append(&o, 1, |_o: Operation, _i: usize| {});
        l.exec(1, &mut f);
        l.exec(1, &mut g);
    }

    // Test that multiple entries on the log can be executed correctly.
    #[test]
    fn test_log_exec_multiple() {
        let l = Log::<Operation>::default();
        let o = [Operation::Read, Operation::Write(119)];
        let mut s = 0;
        let mut f = |op: Operation, _i: usize| match op {
            Operation::Read => s += 121,
            Operation::Write(v) => s += v,
            Operation::Invalid => assert!(false),
        };

        l.append(&o, 1, |_o: Operation, _i: usize| {});
        l.exec(1, &mut f);
        assert_eq!(s, 240);
    }

    // Test that the replica local mask is updated correctly when executing over
    // a wrapped around log.
    #[test]
    fn test_log_exec_wrap() {
        let l = Log::<Operation>::default();
        let o: [Operation; 1024] = unsafe {
            let mut a: [Operation; 1024] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, Operation::Read);
            }
            a
        };
        let mut f = |op: Operation, i: usize| {
            assert_eq!(op, Operation::Read);
            assert_eq!(i, 1);
        };

        l.append(&o, 1, |_o: Operation, _i: usize| {}); // Required for GC to work correctly.
        l.next.store(2, Ordering::SeqCst);
        l.head.store(8192, Ordering::SeqCst);
        l.tail.store(l.size - 10, Ordering::SeqCst);
        l.append(&o, 1, |_o: Operation, _i: usize| {});

        l.ltails[0].store(l.size - 10, Ordering::SeqCst);
        l.exec(1, &mut f);

        assert_eq!(l.lmasks[0].get(), false);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.size + 1014);
    }

    #[test]
    fn test_log_change_refcount() {
        let l = Log::<Arc<Operation>>::default();
        let o1 = [Arc::new(Operation::Read)];
        let o2 = [Arc::new(Operation::Read)];
        assert_eq!(Arc::strong_count(&o1[0]), 1);
        assert_eq!(Arc::strong_count(&o2[0]), 1);

        l.append(&o1[..], 1, |_o: Arc<Operation>, _i: usize| {});
        assert_eq!(Arc::strong_count(&o1[0]), 2);
        l.append(&o1[..], 1, |_o: Arc<Operation>, _i: usize| {});
        assert_eq!(Arc::strong_count(&o1[0]), 3);

        unsafe { l.reset() };

        l.append(&o2[..], 1, |_o: Arc<Operation>, _i: usize| {});
        assert_eq!(Arc::strong_count(&o1[0]), 2);
        assert_eq!(Arc::strong_count(&o2[0]), 2);
        l.append(&o2[..], 1, |_o: Arc<Operation>, _i: usize| {});
        assert_eq!(Arc::strong_count(&o1[0]), 1);
        assert_eq!(Arc::strong_count(&o2[0]), 3);
    }

    #[test]
    fn test_replica_synced_for_read() {
        let l = Log::<Operation>::default();
        let one = l.register().unwrap();
        let two = l.register().unwrap();

        assert_eq!(one, 1);
        assert_eq!(two, 2);

        let o = [Operation::Read];
        let mut f = |op: Operation, i: usize| {
            assert_eq!(op, Operation::Read);
            assert_eq!(i, 1);
        };

        l.append(&o, one, |_o: Operation, _i: usize| {});
        l.exec(one, &mut f);
        assert_eq!(l.is_replica_synced_for_reads(one), true);
        assert_eq!(l.is_replica_synced_for_reads(two), false);

        l.exec(two, &mut f);
        assert_eq!(l.is_replica_synced_for_reads(two), true);
    }
}
