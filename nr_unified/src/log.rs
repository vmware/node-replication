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
#[cfg(not(loom))]
pub struct LogToken(pub(crate) usize);
#[cfg(loom)]
pub struct LogToken(pub usize);

/// The default size of the shared log in bytes. If constructed using the default
/// constructor, the log will be these many bytes in size.
///
/// Currently set to 2 MiB which seems like a good space/performance trade-off for most
/// operation types. In general after more than 100k entries, there is little impact for
/// performance from the actual log size.
pub const DEFAULT_LOG_BYTES: usize = 2 * 1024 * 1024;
const_assert!(DEFAULT_LOG_BYTES.is_power_of_two());

/// The maximum number of replicas that can be registered with the log (plus one
/// since replica registration starts at 0).
///
/// Should be equal or greater than the max. amount of NUMA nodes we expect on
/// our system Can't make it arbitrarily high as it will lead to more memory
/// overheads / bigger structs.
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
pub const GC_FROM_HEAD: usize = MAX_PENDING_OPS * MAX_THREADS_PER_REPLICA;
const_assert!(GC_FROM_HEAD.is_power_of_two());

/// Threshold after how many iterations we abort and report the replica we're waiting for
/// as stuck for busy spinning loops.
///
/// Should be a power of two to avoid divisions.
pub const WARN_THRESHOLD: usize = 1 << 7;
const_assert!(WARN_THRESHOLD.is_power_of_two());

/// An entry that sits on the log. Each entry consists of three fields: The operation to
/// be performed when a thread reaches this entry on the log, the replica that appended
/// this operation, and a flag indicating whether this entry is valid.
///
/// `T` is the type on the operation - typically an enum class containing opcodes as well
/// as arguments. It is required that this type be sized and cloneable.
#[repr(align(64))]
pub(crate) struct Entry<T, M>
where
    T: Sized + Clone,
    M: Default,
{
    /// The operation that this entry represents.
    pub(crate) operation: Option<T>,

    /// Identifies the replica that issued the above operation.
    ///
    /// This is the number from inside the [`LogToken`]. It's not a LogToken itself
    /// because that shouldn't be Clone.
    pub(crate) replica: usize,

    /// Indicates whether this entry represents a valid operation when on the log.
    pub(crate) alivef: AtomicBool,
    /// Meta-data associated with this entry.
    ///
    /// The meta-data is generic and depends on which variant (NR/CNR) is used.
    pub(crate) metadata: M,
}

impl<T, M> Default for Entry<T, M>
where
    T: Sized + Clone,
    M: Default,
{
    fn default() -> Self {
        Self {
            operation: None,
            replica: 0,
            alivef: AtomicBool::new(false),
            metadata: Default::default(),
        }
    }
}

/// A log of operations that is typically accessed by multiple
/// [`crate::replica::Replica`]s.
///
/// # Note
/// In the common case a client of node-replication does not need to create or
/// interact with the log directly. Instead, the client should use
/// [`crate::NodeReplicated`] instead. Only in the rare circumstance where one
/// would need to implement a custom "replica dispatch logic" would it be
/// necessary to create logs and register replicas with it.
///
/// Takes one generic type parameter, `T`, which defines the type of the
/// operation that will go on the log. Typically this is somes enum with variants
/// identifying the different mutable operations.
///
/// This struct is aligned to 64 bytes to optimize cache access.
#[repr(align(64))]
pub struct Log<T, LM, M>
where
    T: Sized + Clone,
    M: Default,
{
    /// The actual log, a slice of entries.
    pub(crate) slog: Box<[Cell<Entry<T, M>>]>,

    /// Logical index into the above slice at which the log starts.
    pub(crate) head: CachePadded<AtomicUsize>,

    /// Logical index into the above slice at which the log ends.
    /// New appends go here.
    pub(crate) tail: CachePadded<AtomicUsize>,

    /// Completed tail maintains an index <= tail that points to a
    /// log entry after which there are no completed operations across
    /// all replicas registered against this log.
    pub(crate) ctail: CachePadded<AtomicUsize>,

    /// Array consisting of the local tail of each replica registered with the log.
    /// Required for garbage collection; since replicas make progress over the log
    /// independently, we want to make sure that we don't garbage collect operations
    /// that haven't been executed by all replicas.
    pub(crate) ltails: [CachePadded<AtomicUsize>; MAX_REPLICAS_PER_LOG],

    /// Identifier that will be allocated to the next replica that registers with
    /// this Log. Also required to correctly index into ltails above.
    pub(crate) next: CachePadded<AtomicUsize>,

    /// Array consisting of local alive masks for each registered replica. Required
    /// because replicas make independent progress over the log, so we need to
    /// track log wrap-arounds for each of them separately.
    pub(crate) lmasks: [CachePadded<Cell<bool>>; MAX_REPLICAS_PER_LOG],

    /// Meta-data used by log implementations.
    pub(crate) metadata: LM,
}

impl<T, LM, M> fmt::Debug for Log<T, LM, M>
where
    T: Sized + Clone,
    M: Default,
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
unsafe impl<T, LM, M> Send for Log<T, LM, M>
where
    T: Sized + Clone,
    M: Default,
{
}

/// The Log is Sync. We know this because: `head` and `tail` are atomic variables, `append()`
/// reserves entries using a CAS, and exec() does not concurrently mutate entries on the log.
unsafe impl<T, LM, M> Sync for Log<T, LM, M>
where
    T: Sized + Clone,
    M: Default,
{
}

impl<T, LM, M> Log<T, LM, M>
where
    T: Sized + Clone,
    M: Default,
{
    /// Constructs and returns a log of (approximately) `num` entries.
    ///
    /// Note that the code ensures that final num. of entries is rounded-up to
    /// the next power of two and at least `2 * GC_FROM_HEAD`, if `num` turns
    /// out to be smaller than that.
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
    /// // Creates a log with `262_144` entries.
    /// let l = Log::<Operation>::new_with_entries(1 << 18);
    /// ```
    ///
    /// This method allocates memory for the log upfront. No further allocations
    /// will be performed once this method returns.
    pub fn new_with_entries(num: usize, metadata: LM) -> Self {
        // Allocate the log
        let mut v = Vec::with_capacity(Log::<T, LM, M>::entries_to_log_entries(num));
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
                metadata,
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
                metadata,
            }
        }
    }

    /// Constructs and returns a log of (approximately) `bytes` bytes.
    ///
    /// Will be rounded up if the provided `bytes` mean the log can hold less
    /// than `2*GC_FROM_HEAD` entries or the amount of entries it can hold with
    /// `bytes` is not a power-of-two.
    ///
    /// This method allocates memory for the log upfront. No further allocations
    /// will be performed once this method returns.
    ///
    /// A log size of 1-2 MiB tends to works well in almost all situation.
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
    pub fn new_with_bytes(bytes: usize, metadata: LM) -> Self {
        Log::new_with_entries(Self::bytes_to_log_entries(bytes), metadata)
    }

    /// Constructs and returns a log of (approximately) [`DEFAULT_LOG_BYTES`]
    /// bytes.
    ///
    /// # See also
    /// - [`Log::new_with_bytes`]
    pub fn new_with_metadata(metadata: LM) -> Self {
        Log::new_with_entries(Self::bytes_to_log_entries(DEFAULT_LOG_BYTES), metadata)
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
        let mut num = core::cmp::max(2 * GC_FROM_HEAD, bytes / Log::<T, LM, M>::entry_size());

        // Round off to the next power of two if required. If we overflow, then set
        // the number of entries to the minimum required for GC. This is unlikely since
        // we'd need a log size > 2^63 entries for this to happen.
        if !num.is_power_of_two() {
            num = num.checked_next_power_of_two().unwrap_or(2 * GC_FROM_HEAD)
        }

        num
    }

    /// Returns the size of a log entry in bytes.
    pub const fn entry_size() -> usize {
        size_of::<Cell<Entry<T, M>>>()
    }

    /// Registers a replica with the log. Returns an identifier that the replica
    /// can use to execute operations on the log.
    ///
    /// As with all the other [`Log`] functions, there is no need to call this
    /// function from a client if you're using [`crate::NodeReplicated`].
    ///
    /// # Example
    ///
    /// ```
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
    /// let l = Log::<Operation>::new_with_entries(4 * 1024);
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
            if n > MAX_REPLICAS_PER_LOG {
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

    /// Returns a physical index given a logical index into the shared log.
    #[inline(always)]
    pub(crate) fn index(&self, logical: usize) -> usize {
        logical & (self.slog.len() - 1)
    }

    /// Loops over all `ltails` and finds the replica with the lowest tail.
    ///
    /// # Returns
    /// The ID (in `LogToken`) of the replica with the lowest tail and the
    /// corresponding/lowest tail `idx` in the `Log`.
    pub(crate) fn find_min_tail(&self) -> (usize, usize) {
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

    /// Resets the log. This is required for microbenchmarking the log; with
    /// this method, we can re-use the log across experimental runs without
    /// having to re-allocate the log over and over again (which blows up the
    /// tests runtime due to allocations / initializing the log memory).
    ///
    /// # Safety
    ///
    /// *To be used for testing/benchmarking only, hence marked unsafe*. Before
    /// calling this method, please make sure that there aren't any
    /// replicas/threads actively issuing/executing operations to/from this log
    /// or having potentially outstanding operations not applied on all
    /// replicas.
    #[doc(hidden)]
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

    /// This method checks if a replica is in sync to execute a read-only
    /// operation. It does so by comparing the replica's local tail with the
    /// log's completed tail.
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

impl<T, LM, M> Default for Log<T, LM, M>
where
    T: Sized + Clone,
    LM: Default,
    M: Default,
{
    /// Default constructor for the shared log.
    ///
    /// Constructs a log of approximately [`DEFAULT_LOG_BYTES`] bytes.
    fn default() -> Self {
        Log::new_with_bytes(DEFAULT_LOG_BYTES, LM::default())
    }
}

#[cfg(test)]
mod tests {
    // Import std so that we have an allocator for our unit tests.
    extern crate std;
    use super::*;

    // Define operations along with their arguments that go onto the log.
    #[derive(Clone)] // Traits required by the log interface.
    #[derive(Debug, PartialEq)] // Traits required for testing.
    enum Operation {
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
        let e = Entry::<Operation, ()>::default();
        assert_eq!(e.operation, None);
        assert_eq!(e.replica, 0);
        assert_eq!(e.alivef.load(Ordering::Relaxed), false);
        assert_eq!(e.metadata, ());
    }

    // Test that our entry_size() method returns the correct size.
    #[test]
    fn test_log_entry_size() {
        assert_eq!(Log::<Operation, (), ()>::entry_size(), 64);
    }

    // Tests if a small log can be correctly constructed.
    #[test]
    fn test_log_create() {
        let l = Log::<Operation, (), ()>::new_with_bytes(1024 * 1024, ());
        let n = (1024 * 1024) / Log::<Operation, (), ()>::entry_size();
        assert_eq!(l.slog.len(), n);
        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 0);
        assert_eq!(l.next.load(Ordering::Relaxed), 1);
        assert_eq!(l.ctail.load(Ordering::Relaxed), 0);
        assert_eq!(l.metadata, ());

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
        let l = Log::<Operation, (), ()>::new_with_bytes(1024, ());
        assert_eq!(l.slog.len(), 2 * GC_FROM_HEAD);
    }

    // Tests if the constructor allocates enough space for GC.
    #[test]
    fn test_log_min_size2() {
        let l = Log::<Operation, (), ()>::new_with_entries(1, ());
        assert_eq!(l.slog.len(), 2 * GC_FROM_HEAD);
    }

    // Tests that the constructor allocates a log whose number of entries
    // are a power of two.
    #[test]
    fn test_log_power_of_two() {
        let l = Log::<Operation, (), ()>::new_with_bytes(524 * 1024, ());
        let n = ((524 * 1024) / Log::<Operation, (), ()>::entry_size()).checked_next_power_of_two();
        assert_eq!(l.slog.len(), n.unwrap());
    }

    // Tests that the constructor allocates a log whose number of entries
    // are a power of two.
    #[test]
    fn test_log_power_of_two2() {
        let l = Log::<Operation, (), ()>::new_with_entries(524 * 1024, ());
        let n = (524 * 1024usize).checked_next_power_of_two();
        assert_eq!(l.slog.len(), n.unwrap());
    }

    // Tests if the log can be successfully default constructed.
    #[test]
    fn test_log_create_default() {
        let l = Log::<Operation, (), ()>::default();
        let n = DEFAULT_LOG_BYTES / Log::<Operation, (), ()>::entry_size();
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
        let l = Log::<Operation, (), ()>::new_with_bytes(2 * 1024 * 1024, ());
        assert_eq!(l.index(99000), 696);
    }

    // Tests if we can correctly register with the shared log.
    #[test]
    fn test_log_register() {
        let l = Log::<Operation, (), ()>::new_with_bytes(1024, ());
        assert_eq!(l.register(), Some(LogToken(1)));
        assert_eq!(l.next.load(Ordering::Relaxed), 2);
    }

    // Tests that we can register exactly `MAX_REPLICAS_PER_LOG` replicas.
    #[test]
    fn test_log_register_all() {
        let l = Log::<Operation, (), ()>::default();
        for _i in 0..MAX_REPLICAS_PER_LOG {
            assert!(l.register().is_some());
        }
        assert!(l.register().is_none());
    }
}
