// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use core::cell::{RefCell, RefMut};
use core::mem::transmute;
use core::sync::atomic::{spin_loop_hint, AtomicUsize, Ordering};

use alloc::sync::Arc;
use alloc::vec::Vec;

use crossbeam_utils::CachePadded;

use super::context::Context;
use super::log::Log;
use super::Dispatch;

/// The maximum number of threads that can be registered with a replica. If more than
/// this number of threads try to register, the register() function will return None.
///
/// # Important
/// If this number is adjusted due to the use of the `arr_macro::arr` macro we
/// have to adjust the `64` literals in the `new` constructor of `Replica`.
const MAX_THREADS_PER_REPLICA: usize = 128;

/// An instance of a replicated data structure. Uses a shared log to scale operations on
/// the data structure across cores and processors.
///
/// Takes in one type argument: `D` represents the replicated data structure against which
/// said operations will be run. `D` must implement the `Dispatch` trait.
///
/// A thread can be registered against the replica by calling `register()`. An operation can
/// be issued by calling `execute()`. This operation will be eventually executed against the
/// replica along with those that were received on other replicas that share the same
/// underlying log.
pub struct Replica<'a, D>
where
    D: Sized + Default + Dispatch,
{
    /// A replica-identifier received when the replica is registered against
    /// the shared-log. Required when consuming operations from the log.
    idx: usize,

    /// Thread idx of the thread currently responsible for flat combining. Zero
    /// if there isn't any thread actively performing flat combining on the log.
    /// This also doubles up as the combiner lock.
    combiner: CachePadded<AtomicUsize>,

    /// Idx that will be handed out to the next thread that registers with the replica.
    next: CachePadded<AtomicUsize>,

    /// Static array of thread contexts. Threads buffer operations in here when they
    /// cannot perform flat combining (because another thread might be doing so).
    contexts: [Context<
        <D as Dispatch>::Operation,
        <D as Dispatch>::Response,
        <D as Dispatch>::ResponseError,
    >; MAX_THREADS_PER_REPLICA],

    /// A buffer of operations for flat combining. The combiner stages operations in
    /// here and then batch appends them into the shared log. This helps amortize
    /// the cost of the compare_and_swap() on the tail of the log.
    buffer: RefCell<Vec<<D as Dispatch>::Operation>>,

    /// Number of operations collected by the combiner from each thread at any
    /// given point of time. Index `i` holds the number of operations collected from
    /// thread with identifier `i + 1`.
    inflight: RefCell<[usize; MAX_THREADS_PER_REPLICA]>,

    /// A buffer of results collected after flat combining. With the help of `inflight`,
    /// the combiner enqueues these results into the appropriate thread context.
    result: RefCell<Vec<Result<<D as Dispatch>::Response, <D as Dispatch>::ResponseError>>>,

    /// Reference to the shared log that operations will be appended to and the
    /// data structure will be updated from.
    slog: Arc<Log<'a, <D as Dispatch>::Operation>>,

    /// The underlying replicated data structure. Shared between threads registered
    /// with this replica. Each replica maintains its own.
    data: RefCell<D>,
}

/// The Replica is Sync. Member variables are protected by a CAS on `combiner`.
/// Contexts are thread-safe.
unsafe impl<'a, D> Sync for Replica<'a, D> where D: Sized + Default + Dispatch {}

impl<'a, D> core::fmt::Debug for Replica<'a, D>
where
    D: Sized + Default + Dispatch,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "Replica")
    }
}

impl<'a, D> Replica<'a, D>
where
    D: Sized + Default + Dispatch,
{
    /// Constructs an instance of a replicated data structure.
    ///
    /// Takes in a reference to the shared log as an argument. The Log is assumed to
    /// outlive the replica. The replica is bound to the log's lifetime.
    pub fn new<'b>(log: &Arc<Log<'b, <D as Dispatch>::Operation>>) -> Replica<'b, D> {
        use arr_macro::arr;

        Replica {
            idx: log.register().unwrap(),
            combiner: CachePadded::new(AtomicUsize::new(0)),
            next: CachePadded::new(AtomicUsize::new(1)),
            contexts: arr![Default::default(); 128],
            buffer: RefCell::new(Vec::with_capacity(
                MAX_THREADS_PER_REPLICA
                    * Context::<
                        <D as Dispatch>::Operation,
                        <D as Dispatch>::Response,
                        <D as Dispatch>::ResponseError,
                    >::batch_size(),
            )),
            inflight: RefCell::new(arr![Default::default(); 128]),
            result: RefCell::new(Vec::with_capacity(
                MAX_THREADS_PER_REPLICA
                    * Context::<
                        <D as Dispatch>::Operation,
                        <D as Dispatch>::Response,
                        <D as Dispatch>::ResponseError,
                    >::batch_size(),
            )),
            slog: log.clone(),
            data: RefCell::new(D::default()),
        }
    }

    /// Registers a thread with this replica. Returns an idx inside an Option if the registration
    /// was successfull. None if the registration failed.
    pub fn register(&self) -> Option<usize> {
        // Loop until we either run out of identifiers or we manage to increment `next`.
        loop {
            let idx = self.next.load(Ordering::SeqCst);

            if idx > MAX_THREADS_PER_REPLICA {
                return None;
            };

            if self.next.compare_and_swap(idx, idx + 1, Ordering::SeqCst) != idx {
                continue;
            };

            return Some(idx);
        }
    }

    /// Executes an operation against this replica.
    ///
    /// `idx` is an identifier for the thread performing the execute operation.
    ///
    /// In addition to the supplied operation, this method might execute operations that were
    /// received on a different replica and appended to the shared log.
    pub fn execute(&self, op: <D as Dispatch>::Operation, idx: usize) {
        // Enqueue the operation onto the thread local batch and then try to flat combine.
        while !self.make_pending(op.clone(), idx) {}
        self.try_combine(idx);
    }

    /// Appends any pending responses to operations issued by this thread into a passed in
    /// buffer/vector. Returns the number of responses that were appended. Blocks until
    /// some responses can be returned.
    pub fn get_responses(
        &self,
        idx: usize,
        buf: &mut Vec<Result<<D as Dispatch>::Response, <D as Dispatch>::ResponseError>>,
    ) -> usize {
        let prev = buf.len();

        let mut iter = 0;
        let interval = 1 << 29;

        // No waiting requests. Just return to the caller.
        if self.contexts[idx - 1].tail.get() == self.contexts[idx - 1].head.get() {
            return 0;
        }

        // Keep trying to retrieve responses from the thread context. After trying `interval`
        // times with no luck, try to perform flat combining to make some progress.
        loop {
            self.contexts[idx - 1].res(buf);
            let next = buf.len();
            if next > prev {
                return next - prev;
            };

            iter += 1;

            if iter == interval {
                self.try_combine(idx);
                iter = 0;
            }
        }
    }

    /// Executes a passed in closure against the replica's underlying data
    /// structure. Useful for unit testing; can be used to verify certain properties
    /// of the data structure after issuing a bunch of operations against it.
    pub fn verify<F: FnMut(RefMut<D>)>(&self, mut v: F) {
        // Acquire the combiner lock before attempting anything on the data structure.
        // Use an idx greater than the maximum that can be allocated.
        while self
            .combiner
            .compare_and_swap(0, MAX_THREADS_PER_REPLICA + 2, Ordering::Acquire)
            != 0
        {}

        let mut data = self.data.borrow_mut();
        let mut f = |o: <D as Dispatch>::Operation, _i: usize| match data.dispatch(o) {
            Ok(_) => {}
            Err(_) => error!("Error in operation dispatch"),
        };

        self.slog.exec(self.idx, &mut f);

        v(data);

        self.combiner.store(0, Ordering::Release);
    }

    /// Syncs up the replica against the underlying log and executes a passed in
    /// closure against all consumed operations.
    pub fn sync<F: FnMut(<D as Dispatch>::Operation, usize)>(&self, mut d: F) {
        // Acquire the combiner lock before attempting anything on the data structure.
        // Use an idx greater than the maximum that can be allocated.
        while self
            .combiner
            .compare_and_swap(0, MAX_THREADS_PER_REPLICA + 2, Ordering::Acquire)
            != 0
        {}

        self.slog.exec(self.idx, &mut d);

        self.combiner.store(0, Ordering::Release);
    }

    /// Enqueues an operation inside a thread local context. Returns a boolean
    /// indicating whether the operation was enqueued (true) or not (false).
    #[inline(always)]
    fn make_pending(&self, op: <D as Dispatch>::Operation, idx: usize) -> bool {
        self.contexts[idx - 1].enqueue(op)
    }

    /// Appends an operation to the log and attempts to perform flat combining.
    /// Accepts a thread `tid` as an argument. Required to acquire the combiner lock.
    fn try_combine(&self, tid: usize) {
        // First, check if there already is a flat combiner. If there is no active flat combiner
        // then try to acquire the combiner lock. If there is, then just return.
        let mut combine = 0;
        for _i in 0..4 {
            combine += unsafe { transmute::<&AtomicUsize, &usize>(&self.combiner) }
        }

        if combine != 0 {
            return;
        };

        // Try to become the combiner here. If this fails, then simply return.
        if self.combiner.compare_and_swap(0, tid, Ordering::Acquire) != 0 {
            spin_loop_hint();
            return;
        }

        // Successfully became the combiner; perform one round of flat combining.
        self.combine();

        // Allow other threads to perform flat combining once we have finished all our work.
        // At this point, we've dropped all mutable references to thread contexts and to
        // the staging buffer as well.
        self.combiner.store(0, Ordering::Release);
    }

    /// Performs one round of flat combining. Collects, appends and executes operations.
    #[inline(always)]
    fn combine(&self) {
        let mut b = self.buffer.borrow_mut();
        let mut o = self.inflight.borrow_mut();
        let mut r = self.result.borrow_mut();

        b.clear();
        r.clear();

        let n = self.next.load(Ordering::Relaxed);

        // Collect operations from each thread registered with this replica.
        for i in 1..n {
            o[i - 1] = self.contexts[i - 1].ops(&mut b);
        }

        // Append all collected operations into the shared log. We pass a closure
        // in here because operations on the log might need to be consumed for GC.
        {
            let mut d = self.data.borrow_mut();
            let f = |o: <D as Dispatch>::Operation, i: usize| {
                let resp = d.dispatch(o);
                if i == self.idx {
                    r.push(resp);
                }
            };
            self.slog.append(&b, self.idx, f);
        }

        // Execute any operations on the shared log against this replica.
        let mut data = self.data.borrow_mut();
        let mut f = |o: <D as Dispatch>::Operation, i: usize| {
            let resp = data.dispatch(o);
            if i == self.idx {
                r.push(resp)
            };
        };
        self.slog.exec(self.idx, &mut f);

        // Return/Enqueue responses back into the appropriate thread context(s).
        let (mut s, mut f) = (0, 0);
        for i in 1..n {
            if o[i - 1] == 0 {
                continue;
            };

            f += o[i - 1];
            self.contexts[i - 1].enqueue_resps(&r[s..f]);
            s += o[i - 1];
            o[i - 1] = 0;
        }
    }
}

#[cfg(test)]
mod test {
    extern crate std;

    use super::*;

    // Really dumb data structure to test against the Replica and shared log.
    #[derive(Default)]
    struct Data {
        junk: u64,
    }

    impl Dispatch for Data {
        type Operation = u64;
        type Response = u64;
        type ResponseError = ();

        fn dispatch(
            &mut self,
            _op: Self::Operation,
        ) -> Result<Self::Response, Self::ResponseError> {
            self.junk += 1;
            return Ok(107);
        }
    }

    // Tests whether we can construct a Replica given a log.
    #[test]
    fn test_replica_create() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        assert_eq!(repl.idx, 1);
        assert_eq!(repl.combiner.load(Ordering::SeqCst), 0);
        assert_eq!(repl.next.load(Ordering::SeqCst), 1);
        assert_eq!(repl.contexts.len(), MAX_THREADS_PER_REPLICA);
        assert_eq!(
            repl.buffer.borrow().capacity(),
            MAX_THREADS_PER_REPLICA * Context::<u64, u64, ()>::batch_size()
        );
        assert_eq!(repl.inflight.borrow().len(), MAX_THREADS_PER_REPLICA);
        assert_eq!(
            repl.result.borrow().capacity(),
            MAX_THREADS_PER_REPLICA * Context::<u64, u64, ()>::batch_size()
        );
        assert_eq!(repl.data.borrow().junk, 0);
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
        let mut o = vec![];

        assert!(repl.make_pending(121, 8));
        assert_eq!(repl.contexts[7].ops(&mut o), 1);
        assert_eq!(o.len(), 1);
        assert_eq!(o[0], 121);
    }

    // Tests that we can't pend operations on a context that is already full of operations.
    #[test]
    fn test_replica_make_pending_false() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        for _i in 0..Context::<u64, u64, ()>::batch_size() {
            assert!(repl.make_pending(121, 1))
        }

        assert!(!repl.make_pending(11, 1));
    }

    // Tests that we can append and execute operations using try_combine().
    #[test]
    fn test_replica_try_combine() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::default());
        let repl = Replica::<Data>::new(&slog);
        let _idx = repl.register();
        let mut r = vec![];

        repl.make_pending(121, 1);
        repl.try_combine(1);
        repl.contexts[0].res(&mut r);

        assert_eq!(repl.combiner.load(Ordering::SeqCst), 0);
        assert_eq!(repl.data.borrow().junk, 1);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], Ok(107));
    }

    // Tests whether try_combine() also applies pending operations on other threads to the log.
    #[test]
    fn test_replica_try_combine_pending() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::default());
        let repl = Replica::<Data>::new(&slog);
        let mut r = vec![];

        repl.next.store(9, Ordering::SeqCst);
        repl.make_pending(121, 8);
        repl.try_combine(1);
        repl.contexts[7].res(&mut r);

        assert_eq!(repl.data.borrow().junk, 1);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], Ok(107));
    }

    // Tests whether try_combine() fails if someone else is currently flat combining.
    #[test]
    fn test_replica_try_combine_fail() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        let mut r = vec![];

        repl.next.store(9, Ordering::SeqCst);
        repl.combiner.store(8, Ordering::SeqCst);
        repl.make_pending(121, 1);
        repl.try_combine(1);
        repl.contexts[0].res(&mut r);

        assert_eq!(repl.data.borrow().junk, 0);
        assert_eq!(r.len(), 0);
    }

    // Tests whether we can execute an operation against the log using execute().
    #[test]
    fn test_replica_execute_combine() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::default());
        let repl = Replica::<Data>::new(&slog);
        let _idx = repl.register();

        repl.execute(121, 1);

        assert_eq!(repl.data.borrow().junk, 1);
    }

    // Tests whether calling execute() when there already is a combiner makes the operation
    // go pending inside the appropriate context.
    #[test]
    fn test_replica_execute_pending() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::default());
        let repl = Replica::<Data>::new(&slog);
        let mut o = vec![];

        repl.combiner.store(8, Ordering::SeqCst);
        repl.execute(121, 1);

        assert_eq!(repl.contexts[0].ops(&mut o), 1);
        assert_eq!(o.len(), 1);
        assert_eq!(o[0], 121);
        assert_eq!(repl.data.borrow().junk, 0);
    }

    // Tests whether get_responses() retrieves responses to an operation that was executed
    // against a replica.
    #[test]
    fn test_replica_get_responses() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::default());
        let repl = Replica::<Data>::new(&slog);
        let _idx = repl.register();
        let mut r = vec![];

        repl.execute(121, 1);

        assert_eq!(repl.get_responses(1, &mut r), 1);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], Ok(107));
    }

    // Tests whether get_responses() does not retrieve anything when an operation hasn't
    // been issued yet.
    #[test]
    fn test_replica_get_responses_none() {
        let slog = Arc::new(Log::<<Data as Dispatch>::Operation>::default());
        let repl = Replica::<Data>::new(&slog);
        let mut r = vec![];

        assert_eq!(repl.get_responses(1, &mut r), 0);
        assert_eq!(r.len(), 0);
    }
}
