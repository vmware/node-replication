use alloc::alloc::{alloc, dealloc, Layout};

use core::cell::Cell;
use core::default::Default;
use core::mem::{align_of, size_of};
use core::ops::{Drop, FnMut};
use core::slice::from_raw_parts_mut;
use core::sync::atomic::{AtomicUsize, Ordering};

/// The default size of the shared log in bytes. If constructed using the
/// default constructor, the log will be these many bytes in size. Currently
/// set to 1 GB based on the ASPLOS 2017 paper.
const DEFAULT_LOG_BYTES: usize = 1024 * 1024 * 1024;

/// An entry that sits on the log. Each entry consists of two fields: An opcode
/// representing the operation to perform when a thread reaches the entry on the
/// log, and a parameter struct containing the operation's arguments.
///
/// `T` and `P` are the types on the opcode and parameter structs respectively.
/// The only requirement on thes types is that they be sized.
///
/// `alivef` indicates whether this entry is valid (true) or has been garbage
/// collected and should be ignored (false).
///
/// Entries are cache-line aligned to 64 bytes.
#[repr(align(64))]
#[derive(Clone, Copy, Default)]
struct Entry<T, P>
where
    T: Sized + Copy + Default,
    P: Sized + Copy + Default,
{
    opcode: T,

    params: P,

    alivef: bool,
}

impl<T, P> Entry<T, P>
where
    T: Sized + Copy + Default,
    P: Sized + Copy + Default,
{
    /// Given an opcode (`op`) and parameters struct (`args`), constructs and
    /// returns an entry that can go onto the shared log.
    fn new(op: T, args: P) -> Entry<T, P> {
        Entry {
            opcode: op,
            params: args,
            alivef: true,
        }
    }
}

/// A log of operations that can be shared between multiple NUMA nodes.
///
/// Operations can be added to the log by calling the `append()` method and
/// providing an opcode and a struct containing arguments for the operation.
///
/// Operations already on the log can be executed by calling the `exec()` method
/// and providing an offset and a closure. All operations from the offset will be
/// executed by invoking the supplied closure over each one of them.
///
/// Accepts two type parameters. `T` defines the type of operations that will go
/// on the log and would typically be an enum class. `P` defines a parameter
/// struct containing arguments for each operation defined inside of `T`.
///
/// This struct is cache aligned to 64 bytes.
#[repr(align(64))]
pub struct Log<'a, T, P>
where
    T: Sized + Copy + Default,
    P: Sized + Copy + Default,
{
    /// Raw pointer to the actual underlying log. Required for dealloc.
    rawp: *mut u8,

    /// Size of the underlying log in bytes. Required for dealloc.
    rawb: usize,

    /// The maximum number of entries that can be held inside the log.
    size: usize,

    /// A reference to the actual log. Nothing but a slice of entries.
    slog: &'a [Cell<Entry<T, P>>],

    /// Logical index into the above slice at which the log starts.
    head: AtomicUsize,

    /// Logical index into the above slice at which the log ends.
    /// New appends go here.
    tail: AtomicUsize,
}

impl<'a, T, P> Log<'a, T, P>
where
    T: Sized + Copy + Default,
    P: Sized + Copy + Default,
{
    /// Constructs and returns a log of size `bytes` bytes. This method also allocates
    /// memory for the log upfront. No further allocations will be performed once this
    /// method returns.
    pub fn new<'b>(bytes: usize) -> Log<'b, T, P> {
        let mem = unsafe {
            alloc(
                Layout::from_size_align(bytes, align_of::<Cell<Entry<T, P>>>())
                    .expect("Alignment error while allocating the shared log!"),
            )
        };
        if mem.is_null() {
            panic!("Failed to allocate memory for the shared log!");
        }

        // Calculate the number of entries that will go into the log, and retrieve a
        // slice to it from the allocated region of memory.
        let num = bytes / Log::<T, P>::entry_size();
        let raw = unsafe { from_raw_parts_mut(mem as *mut Cell<Entry<T, P>>, num) };

        // Initialize all log entries to empty/dead by calling the default constructor.
        for e in &mut raw[..] {
            e.set(Entry::default());
        }

        Log {
            rawp: mem,
            rawb: bytes,
            size: num,
            slog: raw,
            head: AtomicUsize::new(0usize),
            tail: AtomicUsize::new(0usize),
        }
    }

    /// Returns the size of an entry in bytes.
    fn entry_size() -> usize {
        size_of::<Cell<Entry<T, P>>>()
    }

    /// Adds a batch of operations, each with opcode `T` and parameters `P` to the shared log.
    /// Returns true if the operations were added. Returns false if they couldn't because
    /// there was no space on the log (even after an attempt to garbage collect).
    pub fn append(&self, ops: &[(T, P)]) -> bool {
        let n = ops.len();

        // Keep trying to reserve entries and add operations to the log until
        // we succeed in doing so or we run out of space on the log.
        loop {
            let t = self.tail.load(Ordering::SeqCst);
            let h = self.head.load(Ordering::SeqCst);

            // If there isn't space on the log, then return false.
            // TODO: Might want to add garbage collection here?
            if t - h + n > self.size {
                return false;
            }

            // Try reserving slots for the operations. If that fails, then restart
            // from the beginning of this loop.
            if self.tail.compare_and_swap(t, t + n, Ordering::SeqCst) != t {
                continue;
            }

            // Successfully reserved entries on the shared log. Add the operations in.
            for idx in 0..n {
                self.slog[self.index(t + idx)].set(Entry::new(ops[idx].0, ops[idx].1));
            }

            return true;
        }
    }

    /// Executes a passed in closure (`dispatch`) on all operations starting from
    /// logical index `from` on the shared log. Returns the number of operations
    /// that were successfully executed.
    ///
    /// The passed in closure is expected to take in two arguments: An opcode identifying
    /// the operation and a parameter struct with arguments for the operation.
    pub fn exec<F: FnMut(T, P)>(&self, from: usize, mut dispatch: F) -> usize {
        let t = self.tail.load(Ordering::SeqCst);
        let h = self.head.load(Ordering::SeqCst);

        // Make sure we're within the shared log. If we aren't, then return 0 since there
        // anyway aren't any operations to execute.
        if from > t || from < h {
            return 0;
        }

        // Execute all operations from the passed in offset to the shared log's tail.
        for idx in from..t {
            let entry = self.slog[self.index(idx)].get();
            dispatch(entry.opcode, entry.params);
        }

        t - from
    }

    /// Returns a physical index given a logical index into the shared log.
    #[inline(always)]
    fn index(&self, logical: usize) -> usize {
        logical % self.size
    }
}

impl<'a, T, P> Default for Log<'a, T, P>
where
    T: Sized + Copy + Default,
    P: Sized + Copy + Default,
{
    /// Default constructor for the shared log.
    fn default() -> Self {
        Log::new(DEFAULT_LOG_BYTES)
    }
}

impl<'a, T, P> Drop for Log<'a, T, P>
where
    T: Sized + Copy + Default,
    P: Sized + Copy + Default,
{
    /// Destructor for the shared log.
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.rawp,
                Layout::from_size_align(self.rawb, align_of::<Cell<Entry<T, P>>>())
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

    // Define an opcode to identify operations that go onto the log.
    #[derive(Copy, Clone)] // Traits required by the log interface.
    #[derive(Debug, PartialEq)] // Traits required for testing.
    enum Opcode {
        Read,
        Write,
    }

    impl Default for Opcode {
        fn default() -> Opcode {
            Opcode::Read
        }
    }

    // Define parameters for operations that go onto the log.
    #[derive(Copy, Clone, Default)] // Traits required by the log interface.
    #[derive(Debug, PartialEq)] // Traits required for testing.
    struct Params {
        one: usize,
        two: usize,
    }

    impl Params {
        pub fn new(first: usize, second: usize) -> Params {
            Params {
                one: first,
                two: second,
            }
        }
    }

    // Test that we can construct entries correctly. The types on T and P are
    // deliberately kept simple in this unit test.
    #[test]
    fn test_entry_create_basic() {
        let e: Entry<u64, u64> = Entry::new(121, 441);
        assert_eq!(e.opcode, 121);
        assert_eq!(e.params, 441);
        assert_eq!(e.alivef, true);
    }

    // Test that we can construct entries correctly. Use richer types on T and P
    // for this unit test.
    #[test]
    fn test_entry_create() {
        let e = Entry::<Opcode, Params>::new(Opcode::Read, Params::new(11, 121));
        assert_eq!(e.opcode, Opcode::Read);
        assert_eq!(e.params, Params::new(11, 121));
        assert_eq!(e.alivef, true);
    }

    // Test that we can default construct entries correctly.
    #[test]
    fn test_entry_create_default() {
        let e = Entry::<Opcode, Params>::default();
        assert_eq!(e.opcode, Opcode::default());
        assert_eq!(e.params, Params::default());
        assert_eq!(e.alivef, false);
    }

    // Test that our entry_size() method returns the correct size.
    #[test]
    fn test_log_entry_size() {
        assert_eq!(Log::<Opcode, Params>::entry_size(), 64);
    }

    // Test that entries are cache aligned.
    #[test]
    fn test_entry_alignment() {
        assert_eq!(Log::<Opcode, Params>::entry_size() % 64, 0);
    }

    // Tests if a small log can be correctly constructed.
    #[test]
    fn test_log_create() {
        let l = Log::<Opcode, Params>::new(1024);
        let n = 1024 / Log::<Opcode, Params>::entry_size();
        assert_eq!(l.rawb, 1024);
        assert_eq!(l.size, n);
        assert_eq!(l.slog.len(), n);
        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 0);
    }

    // Tests if the log can be successfully default constructed.
    #[test]
    fn test_log_create_default() {
        let l = Log::<Opcode, Params>::default();
        let n = DEFAULT_LOG_BYTES / Log::<Opcode, Params>::entry_size();
        assert_eq!(l.rawb, DEFAULT_LOG_BYTES);
        assert_eq!(l.size, n);
        assert_eq!(l.slog.len(), n);
        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 0);
    }

    // Test if we can correctly index into the shared log.
    #[test]
    fn test_log_index() {
        let l = Log::<Opcode, Params>::new(1024);
        assert_eq!(l.index(100), 4);
    }

    // Test that we can correctly append an entry into the log.
    #[test]
    fn test_log_append() {
        let l = Log::<Opcode, Params>::new(1024);
        let o = [(Opcode::Read, Params::new(11, 121))];
        assert!(l.append(&o));
        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 1);
        assert_eq!(l.slog[0].get().opcode, Opcode::Read);
        assert_eq!(l.slog[0].get().params, Params::new(11, 121));
    }

    // Test that multiple entries can be appended to the log.
    #[test]
    fn test_log_append_multiple() {
        let l = Log::<Opcode, Params>::new(1024);
        let o = [
            (Opcode::Read, Params::new(11, 121)),
            (Opcode::Write, Params::new(9, 119)),
        ];
        assert!(l.append(&o));
    }

    // Test that appends fail when the log is full.
    #[test]
    fn test_log_append_full() {
        let l = Log::<Opcode, Params>::new(64);
        let o = [(Opcode::Read, Params::new(11, 121))];
        assert!(l.append(&o)); // First append should succeed.
        assert!(!l.append(&o)); // Second append must fail.
    }

    // Test that we can execute operations appended to the log.
    #[test]
    fn test_log_exec() {
        let l = Log::<Opcode, Params>::new(1024);
        let o = [(Opcode::Read, Params::new(11, 121))];
        let f = |op: Opcode, args: Params| {
            assert_eq!(op, Opcode::Read);
            assert_eq!(args, Params::new(11, 121));
        };
        assert!(l.append(&o));
        assert_eq!(l.exec(0, f), 1);
    }

    // Test that exec() doesn't do anything when the log is empty.
    #[test]
    fn test_log_exec_empty() {
        let l = Log::<Opcode, Params>::new(1024);
        let f = |_op: Opcode, _args: Params| {
            assert!(false);
        };
        assert_eq!(l.exec(0, f), 0);
    }

    // Test that exec() doesn't do anything if the supplied offset is
    // greater than or equal to the tail of the shared log.
    #[test]
    fn test_log_exec_zero() {
        let l = Log::<Opcode, Params>::new(1024);
        let o = [(Opcode::Read, Params::new(11, 121))];
        let f = |_op: Opcode, _args: Params| {
            assert!(false);
        };
        assert!(l.append(&o));
        assert_eq!(l.exec(1, f), 0);
    }

    // Test that multiple entries on the log can be executed correctly.
    #[test]
    fn test_log_exec_multiple() {
        let l = Log::<Opcode, Params>::new(1024);
        let o = [
            (Opcode::Read, Params::new(11, 121)),
            (Opcode::Write, Params::new(9, 119)),
        ];
        let mut s = 0;
        let f = |_op: Opcode, args: Params| {
            s += args.two;
        };
        assert!(l.append(&o));
        assert_eq!(l.exec(0, f), 2);
        assert_eq!(s, 240);
    }

    // Test that a subset of all entries on the log can be executed correctly.
    #[test]
    fn test_log_exec_subset() {
        let l = Log::<Opcode, Params>::new(1024);
        let o = [
            (Opcode::Read, Params::new(11, 121)),
            (Opcode::Write, Params::new(9, 119)),
        ];
        let mut s = 0;
        let f = |_op: Opcode, args: Params| {
            s += args.two;
        };
        assert!(l.append(&o));
        assert_eq!(l.exec(1, f), 1); // Execute only the second entry.
        assert_eq!(s, 119);
    }
}
