use core::cell::{Cell, RefCell};
use core::sync::atomic::{spin_loop_hint, AtomicBool, AtomicUsize, Ordering};

use alloc::sync::Arc;
use alloc::vec::Vec;

use crossbeam_utils::CachePadded;

use super::context::Context;
use super::log::Log;
use super::Dispatch;

/// The maximum number of threads that can be registered with a replica. If more than
/// this number of threads try to register, the register() function will return None.
const MAX_THREADS_PER_REPLICA: usize = 32;

/// An instance of a replicated data structure. Uses a shared log to scale operations on
/// the data structure across cores and processors.
///
/// Takes in one type argument: `D` represents the replicated data structure against which
/// said operations will be run. `D` must implement the `Dispatch` trait.
///
/// A thread can be executed against the replica by calling `register()`. An operation can
/// be issued by calling `execute()`. This operation will be eventually executed against the
/// replica along with those that were received on other replicas that share the same
/// underlying log.
pub struct Replica<'a, D>
where
    D: Sized + Default + Dispatch,
    <D as Dispatch>::Operation: Sized + Copy + Default,
{
    /// Logical log offset upto which this replica has applied operations to its
    /// copy of the replicated data structure.
    tail: Cell<usize>,

    /// Thread idx of the thread currently responsible for flat combining. Zero
    /// if there isn't any thread actively performing flat combining on the log.
    combiner: CachePadded<AtomicUsize>,

    /// Idx that will be handed out to the next thread that registers with the replica.
    next: CachePadded<AtomicUsize>,

    /// Static array of thread contexts. Threads buffer operations in here when they
    /// cannot perform flat combining (because another thread might be doing so).
    contexts: [RefCell<Context<<D as Dispatch>::Operation>>; MAX_THREADS_PER_REPLICA],

    /// A buffer of operations for flat combining. The combiner stages operations in
    /// here and then batch appends them into the shared log.
    buffer: RefCell<Vec<<D as Dispatch>::Operation>>,

    /// Reference to the shared log that operations will be appended to and the
    /// data structure will be updated from.
    slog: Arc<Log<'a, <D as Dispatch>::Operation>>,

    /// The underlying replicated data structure. Shared between threads registered
    /// with this replica. Each replica maintains its own.
    data: D,

    /// Array of locks, one per context. These locks are required to synchronize between
    /// a thread issuing operations to it's local batch and the flat combiner reading
    /// operations from this batch and appending them to the shared log.
    locks: [CachePadded<AtomicBool>; MAX_THREADS_PER_REPLICA],
}

/// The Replica is Sync. `tail` and `buffer` are protected by a CAS on `combiner`.
/// `contexts` are protected by fine-grained locks.
unsafe impl<'a, D> Sync for Replica<'a, D>
where
    D: Sized + Default + Dispatch,
    <D as Dispatch>::Operation: Sized + Copy + Default,
{
}

impl<'a, D> core::fmt::Debug for Replica<'a, D>
where
    D: Sized + Default + Dispatch,
    <D as Dispatch>::Operation: Sized + Copy + Default,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "Replica")
    }
}

impl<'a, D> Replica<'a, D>
where
    D: Sized + Default + Dispatch,
    <D as Dispatch>::Operation: Sized + Copy + Default,
{
    /// Constructs an instance of a replicated data structure.
    ///
    /// Takes in a reference to the shared log as an argument. The Log is assumed to
    /// outlive the replica. The replica is bound to the log's lifetime.
    pub fn new<'b>(log: &Arc<Log<'b, <D as Dispatch>::Operation>>) -> Replica<'b, D> {
        Replica {
            tail: Cell::new(0usize),
            combiner: CachePadded::new(AtomicUsize::new(0)),
            next: CachePadded::new(AtomicUsize::new(1)),
            contexts: Default::default(),
            buffer: RefCell::new(Vec::with_capacity(
                MAX_THREADS_PER_REPLICA * Context::<<D as Dispatch>::Operation>::batch_size(),
            )),
            slog: log.clone(),
            data: D::default(),
            locks: Default::default(),
        }
    }

    /// Registers a thread with this replica. Returns an idx inside an Option if the registration
    /// was successfull. None if the registration failed.
    pub fn register(&self) -> Option<usize> {
        // Spin until we allocate an identifier for the thread or until we run out
        // of identifiers in which case we return None.
        loop {
            let idx = self.next.load(Ordering::SeqCst);

            if idx > MAX_THREADS_PER_REPLICA {
                return None;
            }

            if self.next.compare_and_swap(idx, idx + 1, Ordering::SeqCst) == idx {
                return Some(idx);
            }
        }
    }

    /// Executes an operation against this replica.
    ///
    /// `idx` is an identifier for the thread performing the execute operation.
    ///
    /// In addition to the supplied operation, this method might execute operations that were
    /// received on a different replica and appended to the shared log.
    pub fn execute(&self, op: <D as Dispatch>::Operation, idx: usize) {
        // Infinite loop until some thread on this replica is elected combiner.
        loop {
            // First, check if there already is a flat combiner. If yes, then just enqueue the
            // operation on the thread local context and return to the caller.
            let mut combine = 0;
            for _idx in 0..4 {
                combine += self.combiner.load(Ordering::Relaxed);
            }

            if combine != 0 {
                if self.make_pending(op, idx) {
                    return;
                }
            }

            // There is no active flat combiner. Try to perform combining on this thread. If
            // that succeeds, then just return to the caller.
            if self.try_combine(op, idx) {
                return;
            }
        }
    }

    /// Consume the replica and return the underlying replicated data structure.
    ///
    /// This also makes sure we have applied all updates from the log before returning.
    ///
    /// # TODO
    /// Most likely not very safe (and currently only used for testing purposes).
    pub unsafe fn data(self) -> D {
        // Execute any operations on the shared log against this replica.
        let f = |o: <D as Dispatch>::Operation| {
            self.data.dispatch(o);
        };
        let t = self.tail.get();
        self.tail.set(t + self.slog.exec(t, f));

        self.data
    }

    /// Enqueues an operation inside a thread local context.
    fn make_pending(&self, op: <D as Dispatch>::Operation, idx: usize) -> bool {
        let f;

        // Critical section because there might be a combiner trying to retrieve
        // operations from this thread's batch.
        self.acquire(idx);

        {
            let mut c = self.contexts[idx - 1].borrow_mut();
            f = c.enqueue(op);
        }

        self.release(idx);

        f
    }

    /// Appends an operation to the log and attempts to perform flat combining.
    fn try_combine(&self, op: <D as Dispatch>::Operation, idx: usize) -> bool {
        // Try to become the combiner here. If this fails, then simply return.
        if self.combiner.compare_and_swap(0, idx, Ordering::Acquire) != 0 {
            spin_loop_hint();
            return false;
        }

        // Loop through every context collecting all pending operations into a
        // buffer. Since we have only one combiner per node, this operation is
        // thread safe and does not need a lock around the buffer. Make sure we
        // append the arguments passed into this method too.
        {
            let mut b = self.buffer.borrow_mut();
            for idx in 1..self.next.load(Ordering::SeqCst) {
                self.acquire(idx);

                {
                    let mut c = self.contexts[idx - 1].borrow_mut();
                    if let Some(ops) = c.ops() {
                        b.extend_from_slice(ops);
                        c.reset_ops();
                    }
                }

                self.release(idx);
            }
            b.push(op);

            // Append all collected operations into the shared log and clear the buffer.
            while !self.slog.append(&b) {}
            b.clear();

            // Execute any operations on the shared log against this replica.
            let f = |o: <D as Dispatch>::Operation| {
                self.data.dispatch(o);
            };

            let t = self.tail.get();
            self.tail.set(t + self.slog.exec(t, f));
        }

        // Allow other threads to perform flat combining once we have finished all our work.
        // At this point, we've dropped all mutable references to thread contexts and to
        // the staging buffer as well.
        self.combiner.store(0, Ordering::Release);

        true
    }

    /// Reserves a context so that the batch can be written to or read from.
    /// `idx` identifies the context to be reserved.
    #[inline(always)]
    fn acquire(&self, idx: usize) {
        while self.locks[idx - 1].compare_and_swap(false, true, Ordering::Acquire) {
            while self.locks[idx - 1].load(Ordering::Relaxed) {
                spin_loop_hint();
            }
        }
    }

    /// Releases a context for reading from or writing to by other threads.
    /// `idx` identifies the context to be reserved.
    #[inline(always)]
    fn release(&self, idx: usize) {
        self.locks[idx - 1].compare_and_swap(true, false, Ordering::Release);
    }
}

#[cfg(test)]
mod test {
    extern crate std;

    use super::*;

    // Really dumb data structure to test against the Replica and shared log.
    #[derive(Default)]
    struct Data {
        junk: Cell<u64>,
    }

    impl Dispatch for Data {
        type Operation = u64;

        fn dispatch(&self, _op: Self::Operation) {
            self.junk.set(self.junk.get() + 1);
        }
    }

    // Tests whether we can construct a Replica given a log.
    #[test]
    fn test_replica_create() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        assert_eq!(repl.tail.get(), 0);
        assert_eq!(repl.combiner.load(Ordering::SeqCst), 0);
        assert_eq!(repl.next.load(Ordering::SeqCst), 1);
        assert_eq!(repl.contexts.len(), MAX_THREADS_PER_REPLICA);
        assert_eq!(
            repl.buffer.borrow().capacity(),
            MAX_THREADS_PER_REPLICA * Context::<u64>::batch_size()
        );
        assert_eq!(repl.data.junk.get(), 0);
        assert_eq!(repl.locks.len(), MAX_THREADS_PER_REPLICA);
    }

    // Tests whether we can register with this replica and receive an idx.
    #[test]
    fn test_replica_register() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        assert_eq!(repl.register(), Some(1));
        assert_eq!(repl.next.load(Ordering::SeqCst), 2);
        repl.next.store(17, Ordering::SeqCst);
        assert_eq!(repl.register(), Some(17));
        assert_eq!(repl.next.load(Ordering::SeqCst), 18);
    }

    // Tests whether registering more than the maximum limit of threads per replica is disallowed.
    #[test]
    fn test_replica_register_none() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        repl.next
            .store(MAX_THREADS_PER_REPLICA + 1, Ordering::SeqCst);
        assert!(repl.register().is_none());
    }

    // Tests that we can successfully allow operations to go pending on this replica.
    #[test]
    fn test_replica_make_pending() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        assert!(repl.contexts[7].borrow().ops().is_none());
        assert!(repl.make_pending(121, 8));
        let c = repl.contexts[7].borrow();
        let o = c.ops();
        assert!(o.is_some());
        let o = o.unwrap();
        assert_eq!(o.len(), 1);
        assert_eq!(o[0], 121);
    }

    // Tests that we can't pend operations on a context that is already full of operations.
    #[test]
    fn test_replica_make_pending_false() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        for _i in 0..Context::<u64>::batch_size() {
            assert!(repl.make_pending(121, 1));
        }
        assert!(!repl.make_pending(11, 1));
    }

    // Tests that we can issue, append, and execute operations using try_combine().
    #[test]
    fn test_replica_try_combine() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        assert!(repl.try_combine(121, 1));
        assert!(repl.contexts[0].borrow().ops().is_none());
        assert_eq!(repl.combiner.load(Ordering::SeqCst), 0);
        assert_eq!(repl.tail.get(), 1);
        assert_eq!(repl.data.junk.get(), 1);
    }

    // Tests whether try_combine() also applies pending operations on other threads to the log.
    #[test]
    fn test_replica_try_combine_pending() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        repl.next.store(9, Ordering::SeqCst);
        assert!(repl.make_pending(121, 8));
        assert!(repl.try_combine(11, 1));
        assert!(repl.contexts[7].borrow().ops().is_none());
        assert_eq!(repl.tail.get(), 2);
        assert_eq!(repl.data.junk.get(), 2);
    }

    // Tests whether try_combine() fails if someone else is currently flat combining.
    #[test]
    fn test_replica_try_combine_fail() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        repl.combiner.store(8, Ordering::SeqCst);
        assert!(!repl.try_combine(121, 1));
    }

    // Tests whether we can execute an operation against the log using execute().
    #[test]
    fn test_replica_execute_combine() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        repl.execute(121, 1);
        assert_eq!(repl.tail.get(), 1);
        assert_eq!(repl.data.junk.get(), 1);
    }

    // Tests whether calling execute() when there already is a combiner makes the operation
    // go pending inside the appropriate context.
    #[test]
    fn test_replica_execute_pending() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        repl.combiner.store(8, Ordering::SeqCst);
        repl.execute(121, 1);
        assert!(repl.contexts[0].borrow().ops().is_some());
        assert_eq!(repl.tail.get(), 0);
        assert_eq!(repl.data.junk.get(), 0);
    }

    // Tests if we can successfully acquire a context lock.
    #[test]
    fn test_replica_locks_acquire() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        repl.acquire(8);
        assert!(repl.locks[7].load(Ordering::SeqCst));
    }

    // Tests if we can successfully release an acquired context lock.
    #[test]
    fn test_context_release() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        repl.acquire(8);
        repl.release(8);
        assert!(!repl.locks[7].load(Ordering::SeqCst));
    }

    // Tests that releasing an unacquired context does nothing.
    #[test]
    fn test_context_release_unreserved() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        repl.release(8);
        assert!(!repl.locks[7].load(Ordering::SeqCst));
    }
}
