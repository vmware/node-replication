// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use alloc::alloc::{alloc, dealloc, Layout};

use core::cell::Cell;
use core::default::Default;
use core::fmt;
use core::mem::{align_of, size_of};
use core::ops::{Drop, FnMut};
use core::slice::from_raw_parts_mut;
use core::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

/// The default size of the shared log in bytes. If constructed using the
/// default constructor, the log will be these many bytes in size. Currently
/// set to 1 GB based on the ASPLOS 2017 paper.
const DEFAULT_LOG_BYTES: usize = 1024 * 1024 * 1024;

/// The maximum number of replicas that can be used against the log.
const MAX_REPLICAS: usize = 32;

/// Required for garbage collection. When this fraction of the log is full of valid
/// operations, then one round of garbage collection will be performed.
const GC_FRACTION: f64 = 0.8;

/// An entry that sits on the log. Each entry consists of two fields: The operation to
/// be performed when a thread reaches this entry on the log, and a flag indicating whether
/// this entry is valid.
///
/// `T` is the type on the operation. It is required that this type be sized, copyable,
/// and default constructable.
///
/// `alivef` indicates whether this entry is valid (true) or has been garbage
/// collected and should be ignored (false).
///
/// Entries are cache-line aligned to 64 bytes.
#[repr(align(64))]
#[derive(Clone, Copy, Default)]
struct Entry<T>
where
    T: Sized + Copy + Default,
{
    operation: T,

    alivef: bool,
}

impl<T> Entry<T>
where
    T: Sized + Copy + Default,
{
    /// Given an operation (`op`), constructs and returns an entry that can go onto the shared log.
    fn new(op: T) -> Entry<T> {
        Entry {
            operation: op,
            alivef: true,
        }
    }
}

/// A log of operations that can be shared between multiple NUMA nodes.
///
/// Operations can be added to the log by calling the `append()` method and
/// providing a list of operations to be performed.
///
/// Operations already on the log can be executed by calling the `exec()` method
/// and providing an offset and a closure. All operations from the offset will be
/// executed by invoking the supplied closure over each one of them.
///
/// Accepts one type parameters. `T` defines the type of operations and their arguments
/// that will go on the log and would typically be an enum class.
///
/// This struct is cache aligned to 64 bytes.
#[repr(align(64))]
pub struct Log<'a, T>
where
    T: Sized + Copy + Default,
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

    /// Array consisting of the local tail of each replica registered with the log.
    /// Required for garbage collection; since replicas make progress over the log
    /// independently, we want to make sure that we don't garbage collect operations
    /// that haven't been executed by all replicas.
    ltails: [CachePadded<AtomicUsize>; MAX_REPLICAS],

    /// Identifier that will be allocated to the next replica that registers with
    /// this Log. Required to index into ltails above.
    next: CachePadded<AtomicUsize>,
}

impl<'a, T> fmt::Debug for Log<'a, T>
where
    T: Sized + Copy + Default,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Log")
            .field("head", &self.tail)
            .field("tail", &self.head)
            .field("size", &self.size)
            .finish()
    }
}

/// The Log is Sync. The *mut u8 (`rawp`) is never dereferenced.
unsafe impl<'a, T> Send for Log<'a, T> where T: Sized + Copy + Default {}

/// The Log is Sync. We know this because: `head` and `tail` are atomic variables, `append()`
/// reserves entries using a CAS, and exec() does not mutate state on the log.
unsafe impl<'a, T> Sync for Log<'a, T> where T: Sized + Copy + Default {}

impl<'a, T> Log<'a, T>
where
    T: Sized + Copy + Default,
{
    /// Constructs and returns a log of size `bytes` bytes. This method also allocates
    /// memory for the log upfront. No further allocations will be performed once this
    /// method returns.
    pub fn new<'b>(bytes: usize) -> Log<'b, T> {
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
        let raw = unsafe { from_raw_parts_mut(mem as *mut Cell<Entry<T>>, num) };

        // Initialize all log entries to empty/dead by calling the default constructor.
        for e in &mut raw[..] {
            e.set(Entry::default());
        }

        Log {
            rawp: mem,
            rawb: bytes,
            size: num,
            slog: raw,
            head: CachePadded::new(AtomicUsize::new(0usize)),
            tail: CachePadded::new(AtomicUsize::new(0usize)),
            ltails: Default::default(),
            next: CachePadded::new(AtomicUsize::new(1usize)),
        }
    }

    /// Returns the size of an entry in bytes.
    fn entry_size() -> usize {
        size_of::<Cell<Entry<T>>>()
    }

    /// Registers a replica against the log. Returns an identifier that the replica
    /// can use while executing operations on the log.
    pub fn register(&self) -> Option<usize> {
        let n = self.next.load(Ordering::SeqCst);

        // Check if we've exceeded the maximum number of replicas the log can support.
        if n >= MAX_REPLICAS {
            return None;
        }

        Some(self.next.fetch_add(1, Ordering::SeqCst))
    }

    /// Adds a batch of operations to the shared log. Returns true if the operations
    /// were added. Returns false if they couldn't because there was no space.
    pub fn append(&self, ops: &[T]) -> Option<usize> {
        let n = ops.len();

        // Keep trying to reserve entries and add operations to the log until
        // we succeed in doing so or we run out of space on the log.
        loop {
            let t = self.tail.load(Ordering::SeqCst);
            let h = self.head.load(Ordering::SeqCst);

            // If there isn't space on the log, then return None.
            if t - h + n > self.size {
                return None;
            }

            // Try reserving slots for the operations. If that fails, then restart
            // from the beginning of this loop.
            if self.tail.compare_and_swap(t, t + n, Ordering::SeqCst) != t {
                continue;
            }

            // Successfully reserved entries on the shared log. Add the operations in.
            for idx in 0..n {
                self.slog[self.index(t + idx)].set(Entry::new(ops[idx]));
            }

            // Check if we need to perform one round of garbage collection. First compute
            // the number of live entries at which we need to kick-off garbage collection.
            // Next, check if this append operation made us exceed the number of these
            // live entries. If it did, then perform garbage collection.
            let i = (GC_FRACTION * (self.size as f64)) as usize;
            if t - h < i && t + n - h > i {
                self.gc();
            }

            return Some(t);
        }
    }

    /// Executes a passed in closure (`dispatch`) on all operations starting from
    /// a replica's local tail on the shared log. The replica is identified through an
    /// `idx` passed in as an argument.
    ///
    /// The passed in closure is expected to take in one argument: The operation
    /// from the shared log to be executed.
    pub fn exec<F: FnMut(T)>(&self, idx: usize, mut dispatch: F) -> usize {
        let t = self.tail.load(Ordering::SeqCst);
        let h = self.head.load(Ordering::SeqCst);

        // Load the logical log offset from which we must execute operations.
        let from = self.ltails[idx - 1].load(Ordering::SeqCst);

        // Make sure we're within the shared log. If we aren't, then return since there
        // anyway aren't any operations to execute.
        if from > t || from < h {
            return from;
        }

        // Execute all operations from the passed in offset to the shared log's tail.
        for idx in from..t {
            let mut entry;
            loop {
                entry = self.slog[self.index(idx)].get();
                if entry.alivef {
                    break;
                }
            }
            dispatch(entry.operation);
        }

        // Update the replica's local tail.
        self.ltails[idx - 1].store(t, Ordering::SeqCst);

        from
    }

    /// Returns a physical index given a logical index into the shared log.
    #[inline(always)]
    fn index(&self, logical: usize) -> usize {
        logical % self.size
    }

    /// Garbage collects entries on the log.
    fn gc(&self) {
        // First, look at all replica-local tails and find the minimum. It
        // is safe to garbage collect entries from the head upto this minimum.
        let n = self.next.load(Ordering::SeqCst);
        let h = self.head.load(Ordering::SeqCst);

        let mut new = self.ltails[0].load(Ordering::SeqCst);

        for idx in 0..n {
            let t = self.ltails[idx].load(Ordering::SeqCst);
            if new > t {
                new = t;
            }
        }

        // No entries to garbage collect. Return empty-handed.
        if new <= h {
            return;
        }

        // Update the head of the log to point to the minimum of all replica-local
        // tails. Next, mark all entries between the old and new heads as dead by
        // setting their `alivef` to false.
        self.head.store(new, Ordering::SeqCst);

        for idx in h..new {
            let mut e = self.slog[self.index(idx)].get();
            e.alivef = false;
            self.slog[self.index(idx)].set(e);
        }
    }

    /// Resets the log; *for testing only since we don't have GC*.
    ///
    /// # TODO
    /// Remove when we have GC.
    #[inline(always)]
    pub unsafe fn reset(&self) {
        for e in self.slog {
            e.set(Entry::default());
        }

        let t = self.tail.load(Ordering::SeqCst);
        let h = self.head.load(Ordering::SeqCst);

        self.tail.compare_and_swap(t, 0, Ordering::SeqCst);
        self.head.compare_and_swap(h, 0, Ordering::SeqCst);
    }
}

impl<'a, T> Default for Log<'a, T>
where
    T: Sized + Copy + Default,
{
    /// Default constructor for the shared log.
    fn default() -> Self {
        Log::new(DEFAULT_LOG_BYTES)
    }
}

impl<'a, T> Drop for Log<'a, T>
where
    T: Sized + Copy + Default,
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

    // Define operations along with their arguments that go onto the log.
    #[derive(Copy, Clone)] // Traits required by the log interface.
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

    // Test that we can construct entries correctly. The type `T` is deliberately
    // kept simple for this unit test.
    #[test]
    fn test_entry_create_basic() {
        let e: Entry<u64> = Entry::new(121);
        assert_eq!(e.operation, 121);
        assert_eq!(e.alivef.load(Ordering::SeqCst), true);
    }

    // Test that we can construct entries correctly. Use a richer type for T
    // in this unit test.
    #[test]
    fn test_entry_create() {
        let e = Entry::<Operation>::new(Operation::Write(121));
        assert_eq!(e.operation, Operation::Write(121));
        assert_eq!(e.alivef.load(Ordering::SeqCst), true);
    }

    // Test that we can default construct entries correctly.
    #[test]
    fn test_entry_create_default() {
        let e = Entry::<Operation>::default();
        assert_eq!(e.operation, Operation::default());
        assert_eq!(e.alivef.load(Ordering::SeqCst), false);
    }

    // Test that our entry_size() method returns the correct size.
    #[test]
    fn test_log_entry_size() {
        assert_eq!(Log::<Operation>::entry_size(), 64);
    }

    // Test that entries are cache aligned.
    #[test]
    fn test_entry_alignment() {
        assert_eq!(Log::<Operation>::entry_size() % 64, 0);
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
    }

    // Test if we can correctly index into the shared log.
    #[test]
    fn test_log_index() {
        let l = Log::<Operation>::new(1024);
        assert_eq!(l.index(100), 4);
    }

    // Test that we can correctly append an entry into the log.
    #[test]
    fn test_log_append() {
        let l = Log::<Operation>::new(1024);
        let o = [Operation::Read];
        assert!(l.append(&o).is_some());
        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 1);
        assert_eq!(l.slog[0].get().operation, Operation::Read);
    }

    // Test that multiple entries can be appended to the log.
    #[test]
    fn test_log_append_multiple() {
        let l = Log::<Operation>::new(1024);
        let o = [Operation::Read, Operation::Write(119)];
        assert!(l.append(&o).is_some());
    }

    // Test that appends fail when the log is full.
    #[test]
    fn test_log_append_full() {
        let l = Log::<Operation>::new(64);
        let o = [Operation::Read];
        assert!(l.append(&o).is_some()); // First append should succeed.
        assert!(l.append(&o).is_none()); // Second append must fail.
    }

    // Test that we can execute operations appended to the log.
    #[test]
    fn test_log_exec() {
        let l = Log::<Operation>::new(1024);
        let o = [Operation::Read];
        let f = |op: Operation| {
            assert_eq!(op, Operation::Read);
        };
        assert!(l.append(&o).is_some());
        assert_eq!(l.exec(0, f), 1);
    }

    // Test that exec() doesn't do anything when the log is empty.
    #[test]
    fn test_log_exec_empty() {
        let l = Log::<Operation>::new(1024);
        let f = |_op: Operation| {
            assert!(false);
        };
        assert_eq!(l.exec(0, f), 0);
    }

    // Test that exec() doesn't do anything if the supplied offset is
    // greater than or equal to the tail of the shared log.
    #[test]
    fn test_log_exec_zero() {
        let l = Log::<Operation>::new(1024);
        let o = [Operation::Read];
        let f = |_op: Operation| {
            assert!(false);
        };
        assert!(l.append(&o).is_some());
        assert_eq!(l.exec(1, f), 0);
    }

    // Test that multiple entries on the log can be executed correctly.
    #[test]
    fn test_log_exec_multiple() {
        let l = Log::<Operation>::new(1024);
        let o = [Operation::Read, Operation::Write(119)];
        let mut s = 0;
        let f = |op: Operation| match op {
            Operation::Read => s += 121,
            Operation::Write(v) => s += v,
            Operation::Invalid => assert!(false),
        };
        assert!(l.append(&o).is_some());
        assert_eq!(l.exec(0, f), 2);
        assert_eq!(s, 240);
    }

    // Test that a subset of all entries on the log can be executed correctly.
    #[test]
    fn test_log_exec_subset() {
        let l = Log::<Operation>::new(1024);
        let o = [Operation::Read, Operation::Write(119)];
        let mut s = 0;
        let f = |op: Operation| match op {
            Operation::Read => s += 121,
            Operation::Write(v) => s += v,
            Operation::Invalid => assert!(false),
        };
        assert!(l.append(&o).is_some());
        assert_eq!(l.exec(1, f), 1); // Execute only the second entry.
        assert_eq!(s, 119);
    }
}
