// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use core::cell::RefCell;
use core::hint::spin_loop;
#[cfg(not(loom))]
use core::sync::atomic::{AtomicUsize, Ordering};
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};

use alloc::vec::Vec;

use crossbeam_utils::CachePadded;

use super::context::Context;
use super::log::{Log, LogToken};
use super::rwlock::RwLock;
use super::Dispatch;
use super::ReusableBoxFuture;

/// The idx that uniquely identifies a thread that's registered with the
/// replica.
pub type ThreadIdx = usize;

/// A token handed out to threads registered with replicas.
///
/// # Note

/// Ideally this would be an affine type and returned again by
/// `execute` and `execute_ro`. However it feels like this would
/// hurt API ergonomics a lot.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct ReplicaToken(ThreadIdx);

/// To make it harder to use the same ReplicaToken on multiple threads.
#[cfg(features = "unstable")]
impl !Send for ReplicaToken {}

impl ReplicaToken {
    /// Creates a new ReplicaToken
    ///
    /// # Safety
    /// This should only ever be used for the benchmark harness to create
    /// additional fake replica implementations.
    /// If we had a means to declare this not-pub we should do that instead.
    #[doc(hidden)]
    pub unsafe fn new(tid: ThreadIdx) -> Self {
        ReplicaToken(tid)
    }

    /// Getter for thread ID.
    pub fn tid(&self) -> ThreadIdx {
        self.0
    }
}

/// The maximum number of threads that can be registered with a replica. If more than
/// this number of threads try to register, the register() function will return None.
///
/// # Important
/// If this number is adjusted due to the use of the `arr_macro::arr` macro we
/// have to adjust the `256` literals in the `new` constructor of `Replica`.
#[cfg(not(loom))]
pub const MAX_THREADS_PER_REPLICA: usize = 256;
#[cfg(loom)]
pub const MAX_THREADS_PER_REPLICA: usize = 2;
const_assert!(
    MAX_THREADS_PER_REPLICA >= 1 && (MAX_THREADS_PER_REPLICA & (MAX_THREADS_PER_REPLICA - 1) == 0)
);

/// An instance of a replicated data structure. Uses a shared log to scale
/// operations on the data structure across cores and processors.
///
/// Takes in one type argument: `D` represents the underlying sequential data
/// structure `D` must implement the `Dispatch` trait.
///
/// A thread can be registered against the replica by calling `register()`. A
/// mutable operation can be issued by calling `execute_mut()` (immutable uses
/// `execute`). A mutable operation will be eventually executed against the replica
/// along with any operations that were received on other replicas that share
/// the same underlying log.
pub struct Replica<D>
where
    D: Sized + Dispatch + Sync,
{
    /// A replica-identifier received when the replica is registered against
    /// the shared-log. Required when consuming operations from the log.
    log_tkn: LogToken,

    /// Thread idx of the thread currently responsible for flat combining. Zero
    /// if there isn't any thread actively performing flat combining on the log.
    /// This also doubles up as the combiner lock.
    combiner: CachePadded<AtomicUsize>,

    /// Idx that will be handed out to the next thread that registers with the replica.
    next: CachePadded<AtomicUsize>,

    /// List of per-thread contexts. Threads buffer write operations in here when they
    /// cannot perform flat combining (because another thread might be doing so).
    ///
    /// The vector is initialized with `MAX_THREADS_PER_REPLICA` elements.
    contexts: Vec<Context<<D as Dispatch>::WriteOperation, <D as Dispatch>::Response>>,

    /// A buffer of operations for flat combining. The combiner stages operations in
    /// here and then batch appends them into the shared log. This helps amortize
    /// the cost of the compare_and_swap() on the tail of the log.
    buffer: RefCell<Vec<<D as Dispatch>::WriteOperation>>,

    /// Number of operations collected by the combiner from each thread at any
    /// given point of time. Index `i` holds the number of operations collected from
    /// thread with identifier `i + 1`.
    inflight: RefCell<[usize; MAX_THREADS_PER_REPLICA]>,

    /// A buffer of results collected after flat combining. With the help of `inflight`,
    /// the combiner enqueues these results into the appropriate thread context.
    result: RefCell<Vec<<D as Dispatch>::Response>>,

    /// The underlying replicated data structure. Shared between threads registered
    /// with this replica. Each replica maintains its own.
    data: CachePadded<RwLock<D>>,
}

/// The Replica is Sync. Member variables are protected by a CAS on `combiner`.
/// Contexts are thread-safe.
unsafe impl<'a, D> Sync for Replica<D> where D: Sized + Sync + Dispatch {}

impl<'a, D> core::fmt::Debug for Replica<D>
where
    D: Sized + Sync + Dispatch,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "Replica")
    }
}

impl<D> Replica<D>
where
    D: Sized + Default + Dispatch + Sync,
{
    /// Constructs an instance of a replicated data structure.
    ///
    /// Takes a reference to the shared log as an argument. The Log is assumed to
    /// outlive the replica. The replica is bound to the log's lifetime.
    ///
    /// # Example
    ///
    /// ```
    /// use node_replication::Dispatch;
    /// use node_replication::Log;
    /// use node_replication::Replica;
    ///
    /// use std::sync::Arc;
    ///
    /// // The data structure we want replicated.
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: u64,
    /// }
    ///
    /// // This trait allows the `Data` to be used with node-replication.
    /// impl Dispatch for Data {
    ///     type ReadOperation = ();
    ///     type WriteOperation = u64;
    ///     type Response = Option<u64>;
    ///
    ///     // A read returns the underlying u64.
    ///     fn dispatch(
    ///         &self,
    ///         _op: Self::ReadOperation,
    ///     ) -> Self::Response {
    ///         Some(self.junk)
    ///     }
    ///
    ///     // A write updates the underlying u64.
    ///     fn dispatch_mut(
    ///         &mut self,
    ///         op: Self::WriteOperation,
    ///     ) -> Self::Response {
    ///         self.junk = op;
    ///         None
    ///     }
    /// }
    ///
    /// // First create a shared log.
    /// let log = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
    ///
    /// // Create a replica that uses the above log.
    /// let replica = Replica::<Data>::new(&log);
    /// ```
    pub fn new(log_tkn: LogToken) -> Replica<D> {
        Replica::with_data(log_tkn, Default::default())
    }
}

/// Indicates that we succesfully hold the combiner lock (atomic `combiner` is
/// set to 1 and we have to reset it to 0)
pub struct CombinerLock<'a, D>
where
    D: Sized + Dispatch + Sync,
{
    replica: &'a Replica<D>,
}

impl<'a, D> CombinerLock<'a, D>
where
    D: Sized + Dispatch + Sync,
{
    unsafe fn new(replica: &'a Replica<D>) -> Self {
        Self { replica }
    }
}

impl<D> Drop for CombinerLock<'_, D>
where
    D: Sized + Dispatch + Sync,
{
    fn drop(&mut self) {
        // Allow other threads to perform flat combining once we have finished all our work.
        // At this point, we've dropped all mutable references to thread contexts and to
        // the staging buffer as well.
        self.replica.combiner.store(0, Ordering::Release);
    }
}

impl<'a, D> Replica<D>
where
    D: Sized + Dispatch + Sync,
{
    /// Similar to [`Replica<D>::new`], but we pass a pre-initialized
    /// data-structure as an argument (`d`) rather than relying on the
    /// [Default](core::default::Default) trait to create one.
    ///
    /// # Note
    /// [`Replica<D>::new`] should be the preferred method to create a Replica.
    /// If `with_data` is used, care must be taken that the same state is passed
    /// to every Replica object. If not the resulting operations executed
    /// against replicas may not give deterministic results.
    #[cfg(not(feature = "unstable"))]
    pub fn with_data(log_tkn: LogToken, d: D) -> Replica<D> {
        let mut contexts = Vec::with_capacity(MAX_THREADS_PER_REPLICA);
        // Add `MAX_THREADS_PER_REPLICA` contexts
        for _idx in 0..MAX_THREADS_PER_REPLICA {
            contexts.push(Default::default());
        }

        Replica {
            log_tkn,
            combiner: CachePadded::new(AtomicUsize::new(0)),
            next: CachePadded::new(AtomicUsize::new(1)),
            contexts,
            buffer:
                RefCell::new(
                    Vec::with_capacity(
                        MAX_THREADS_PER_REPLICA
                            * Context::<
                                <D as Dispatch>::WriteOperation,
                                <D as Dispatch>::Response,
                            >::batch_size(),
                    ),
                ),
            inflight: RefCell::new([0; MAX_THREADS_PER_REPLICA]),
            result:
                RefCell::new(
                    Vec::with_capacity(
                        MAX_THREADS_PER_REPLICA
                            * Context::<
                                <D as Dispatch>::WriteOperation,
                                <D as Dispatch>::Response,
                            >::batch_size(),
                    ),
                ),
            data: CachePadded::new(RwLock::<D>::new(d)),
        }
    }

    /// See `with_data` documentation without unstable feature.
    #[cfg(feature = "unstable")]
    pub fn with_data(log: &Log<<D as Dispatch>::WriteOperation>, d: D) -> Replica<'b, D> {
        use core::mem::MaybeUninit;
        let mut uninit_replica: Arc<MaybeUninit<Replica<D>>> = Arc::new_zeroed();

        // This is the preferred (but unsafe) mode of initialization as it avoids
        // putting the big Replica object on the stack first.
        unsafe {
            let uninit_ptr = Arc::get_mut_unchecked(&mut uninit_replica).as_mut_ptr();
            uninit_ptr.write(Replica {
                idx: log.register().unwrap(),
                combiner: CachePadded::new(AtomicUsize::new(0)),
                next: CachePadded::new(AtomicUsize::new(1)),
                contexts: Vec::with_capacity(MAX_THREADS_PER_REPLICA),
                buffer:
                    RefCell::new(
                        Vec::with_capacity(
                            MAX_THREADS_PER_REPLICA
                                * Context::<
                                    <D as Dispatch>::WriteOperation,
                                    <D as Dispatch>::Response,
                                >::batch_size(),
                        ),
                    ),
                inflight: RefCell::new([0; MAX_THREADS_PER_REPLICA]),
                result:
                    RefCell::new(
                        Vec::with_capacity(
                            MAX_THREADS_PER_REPLICA
                                * Context::<
                                    <D as Dispatch>::WriteOperation,
                                    <D as Dispatch>::Response,
                                >::batch_size(),
                        ),
                    ),
                slog: log.clone(),
                data: CachePadded::new(RwLock::<D>::new(d)),
            });

            let mut replica = uninit_replica.assume_init();
            // Add `MAX_THREADS_PER_REPLICA` contexts
            for _idx in 0..MAX_THREADS_PER_REPLICA {
                Arc::get_mut(&mut replica)
                    .unwrap()
                    .contexts
                    .push(Default::default());
            }

            replica
        }
    }

    /// Registers a thread with this replica. Returns an idx inside an Option if the registration
    /// was successfull. None if the registration failed.
    ///
    /// # Example
    ///
    /// ```
    /// use node_replication::Dispatch;
    /// use node_replication::Log;
    /// use node_replication::Replica;
    ///
    /// use std::sync::Arc;
    ///
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: u64,
    /// }
    ///
    /// impl Dispatch for Data {
    ///     type ReadOperation = ();
    ///     type WriteOperation = u64;
    ///     type Response = Option<u64>;
    ///
    ///     fn dispatch(
    ///         &self,
    ///         _op: Self::ReadOperation,
    ///     ) -> Self::Response {
    ///         Some(self.junk)
    ///     }
    ///
    ///     fn dispatch_mut(
    ///         &mut self,
    ///         op: Self::WriteOperation,
    ///     ) -> Self::Response {
    ///         self.junk = op;
    ///         None
    ///     }
    /// }
    ///
    /// let log = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
    /// let replica = Replica::<Data>::new(&log);
    ///
    /// // Calling register() returns an idx that can be used to execute
    /// // operations against the replica.
    /// let idx = replica.register().expect("Failed to register with replica.");
    /// ```
    pub fn register(&self) -> Option<ReplicaToken> {
        // Loop until we either run out of identifiers or we manage to increment `next`.
        loop {
            let idx = self.next.load(Ordering::SeqCst);

            if idx > MAX_THREADS_PER_REPLICA {
                return None;
            };

            if self
                .next
                .compare_exchange_weak(idx, idx + 1, Ordering::SeqCst, Ordering::SeqCst)
                != Ok(idx)
            {
                continue;
            };

            return Some(ReplicaToken(idx));
        }
    }

    /// Executes an mutable operation against this replica and returns a response.
    /// `idx` is an identifier for the thread performing the execute operation.
    ///
    /// # Example
    ///
    /// ```
    /// use node_replication::Dispatch;
    /// use node_replication::Log;
    /// use node_replication::Replica;
    ///
    /// use std::sync::Arc;
    ///
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: u64,
    /// }
    ///
    /// impl Dispatch for Data {
    ///     type ReadOperation = ();
    ///     type WriteOperation = u64;
    ///     type Response = Option<u64>;
    ///
    ///     fn dispatch(
    ///         &self,
    ///         _op: Self::ReadOperation,
    ///     ) -> Self::Response {
    ///         Some(self.junk)
    ///     }
    ///
    ///     fn dispatch_mut(
    ///         &mut self,
    ///         op: Self::WriteOperation,
    ///     ) -> Self::Response {
    ///         self.junk = op;
    ///         None
    ///     }
    /// }
    ///
    /// let log = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
    /// let replica = Replica::<Data>::new(&log);
    /// let idx = replica.register().expect("Failed to register with replica.");
    ///
    /// // execute_mut() can be used to write to the replicated data structure.
    /// let res = replica.execute_mut(100, idx);
    /// assert_eq!(None, res);
    pub fn execute_mut(
        &self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
        op: <D as Dispatch>::WriteOperation,
        idx: ReplicaToken,
    ) -> Result<<D as Dispatch>::Response, (usize, CombinerLock<D>)> {
        // Enqueue the operation onto the thread local batch and then try to flat combine.
        while !self.make_pending(op.clone(), idx.tid()) {}
        self.try_combine(slog)?;

        // Return the response to the caller function.
        self.get_response(slog, idx.tid())
    }

    /// Executes a read-only operation against this replica and returns a response.
    /// `idx` is an identifier for the thread performing the execute operation.
    ///
    /// # Example
    ///
    /// ```
    /// use node_replication::Dispatch;
    /// use node_replication::Log;
    /// use node_replication::Replica;
    ///
    /// use std::sync::Arc;
    ///
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: u64,
    /// }
    ///
    /// impl Dispatch for Data {
    ///     type ReadOperation = ();
    ///     type WriteOperation = u64;
    ///     type Response = Option<u64>;
    ///
    ///     fn dispatch(
    ///         &self,
    ///         _op: Self::ReadOperation,
    ///     ) -> Self::Response {
    ///         Some(self.junk)
    ///     }
    ///
    ///     fn dispatch_mut(
    ///         &mut self,
    ///         op: Self::WriteOperation,
    ///     ) -> Self::Response {
    ///         self.junk = op;
    ///         None
    ///     }
    /// }
    ///
    /// let log = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
    /// let replica = Replica::<Data>::new(&log);
    /// let idx = replica.register().expect("Failed to register with replica.");
    /// let _wr = replica.execute_mut(100, idx);
    ///
    /// // execute() can be used to read from the replicated data structure.
    /// let res = replica.execute((), idx);
    /// assert_eq!(Some(100), res);
    pub fn execute(
        &self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
        op: <D as Dispatch>::ReadOperation,
        idx: ReplicaToken,
    ) -> Result<<D as Dispatch>::Response, (usize, CombinerLock<D>)> {
        self.read_only(slog, op, idx.tid())
    }

    /// Busy waits until a response is available within the thread's context.
    /// `idx` identifies this thread.
    pub(crate) fn get_response(
        &self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
        idx: usize,
    ) -> Result<<D as Dispatch>::Response, (usize, CombinerLock<D>)> {
        let mut iter = 0;
        let interval = 1 << 29;

        // Keep trying to retrieve a response from the thread context. After trying `interval`
        // times with no luck, try to perform flat combining to make some progress.
        loop {
            let r = self.contexts[idx - 1].res();
            if let Some(resp) = r {
                return Ok(resp);
            }

            iter += 1;

            if iter == interval {
                self.try_combine(slog)?;
                iter = 0;
            }
        }
    }

    /// Executes a passed in closure against the replica's underlying data
    /// structure. Useful for unit testing; can be used to verify certain
    /// properties of the data structure after issuing a bunch of operations
    /// against it.
    ///
    /// # Note
    /// There is probably no need for a regular client to ever call this function.
    #[doc(hidden)]
    pub fn verify<F: FnMut(&D)>(&self, slog: &Log<<D as Dispatch>::WriteOperation>, mut v: F) {
        // Acquire the combiner lock before attempting anything on the data structure.
        // Use an idx greater than the maximum that can be allocated.
        while self.combiner.compare_exchange_weak(
            0,
            MAX_THREADS_PER_REPLICA + 2,
            Ordering::Acquire,
            Ordering::Acquire,
        ) != Ok(0)
        {
            spin_loop();
        }

        let mut data = self.data.write(self.next.load(Ordering::Relaxed));
        let mut f = |o: <D as Dispatch>::WriteOperation, _i: usize| {
            data.dispatch_mut(o);
        };

        slog.exec(self.log_tkn, &mut f);

        v(&data);

        self.combiner.store(0, Ordering::Release);
    }

    /// This method is useful when a replica stops making progress and some threads
    /// on another replica are still active. The active replica will use all the entries
    /// in the log and won't be able perform garbage collection because of the inactive
    /// replica. So, this method syncs up the replica against the underlying log.
    pub fn sync(
        &self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
    ) -> Result<(), (usize, CombinerLock<D>)> {
        let ctail = slog.get_ctail();
        while slog.is_replica_synced_for_reads(self.log_tkn, ctail) {
            self.try_combine(slog)?;
            spin_loop();
        }
        Ok(())
    }

    /// Issues a read-only operation against the replica and returns a response.
    /// Makes sure the replica is synced up against the log before doing so.
    fn read_only(
        &self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
        op: <D as Dispatch>::ReadOperation,
        tid: usize,
    ) -> Result<<D as Dispatch>::Response, (usize, CombinerLock<D>)> {
        // We can perform the read only if our replica is synced up against
        // the shared log. If it isn't, then try to combine until it is synced up.
        let ctail = slog.get_ctail();
        while !slog.is_replica_synced_for_reads(self.log_tkn, ctail) {
            self.try_combine(slog)?;
            spin_loop();
        }

        return Ok(self.data.read(tid - 1).dispatch(op));
    }

    /// Enqueues an operation inside a thread local context. Returns a boolean
    /// indicating whether the operation was enqueued (true) or not (false).
    #[inline(always)]
    fn make_pending(&self, op: <D as Dispatch>::WriteOperation, idx: usize) -> bool {
        self.contexts[idx - 1].enqueue(op)
    }

    // Try to become acquire the combiner lock here. If this fails, then return None.
    #[inline(always)]
    fn acquire_combiner_lock(&self) -> Option<CombinerLock<D>> {
        if self
            .combiner
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Acquire)
            != Ok(0)
        {
            #[cfg(loom)]
            loom::thread::yield_now();
            None
        } else {
            unsafe { Some(CombinerLock::new(self)) }
        }
    }

    /// Appends an operation to the log and attempts to perform flat combining.
    /// Accepts a thread `tid` as an argument. Required to acquire the combiner lock.
    fn try_combine<'r>(
        &'r self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
    ) -> Result<(), (usize, CombinerLock<'r, D>)> {
        // First, check if there already is a flat combiner. If there is no active flat combiner
        // then try to acquire the combiner lock. If there is, then just return.
        for _ in 0..4 {
            if self.combiner.load(Ordering::Relaxed) != 0 {
                #[cfg(loom)]
                loom::thread::yield_now();
                return Ok(());
            }
        }

        // Try to become the combiner here. If this fails, then simply return.
        if let Some(combiner_lock) = self.acquire_combiner_lock() {
            // Successfully became the combiner; perform one round of flat combining.
            self.combine(slog, combiner_lock)?;
            Ok(())
        } else {
            #[cfg(loom)]
            loom::thread::yield_now();
            Ok(())
        }
    }

    /// Performs one round of flat combining. Collects, appends and executes operations.
    #[inline(always)]
    pub(crate) fn combine<'r>(
        &'r self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
        combiner_lock: CombinerLock<'r, D>,
    ) -> Result<(), (usize, CombinerLock<'r, D>)> {
        let mut buffer = self.buffer.borrow_mut();
        let mut operations = self.inflight.borrow_mut();
        let mut results = self.result.borrow_mut();

        buffer.clear();
        results.clear();

        let next = self.next.load(Ordering::Relaxed);

        // Collect operations from each thread registered with this replica.
        for i in 1..next {
            operations[i - 1] = self.contexts[i - 1].ops(&mut buffer);
        }

        // Append all collected operations into the shared log. We pass a closure
        // in here because operations on the log might need to be consumed for GC.
        {
            let mut data = self.data.write(next);
            let f = |o: <D as Dispatch>::WriteOperation, i: usize| {
                #[cfg(not(loom))]
                let resp = data.dispatch_mut(o);
                #[cfg(loom)]
                let resp = data.dispatch_mut(o);
                if i == self.log_tkn {
                    results.push(resp);
                }
            };
            slog.append(&buffer, self.log_tkn, f)
                .map_err(|ridx| (ridx, combiner_lock))?;
        }

        // Execute any operations on the shared log against this replica.
        {
            let mut data = self.data.write(next);
            let mut f = |o: <D as Dispatch>::WriteOperation, i: usize| {
                let resp = data.dispatch_mut(o);
                if i == self.log_tkn {
                    results.push(resp)
                };
            };
            slog.exec(self.log_tkn, &mut f);
        }

        // Return/Enqueue responses back into the appropriate thread context(s).
        let (mut s, mut f) = (0, 0);
        for i in 1..next {
            if operations[i - 1] == 0 {
                continue;
            };

            f += operations[i - 1];
            self.contexts[i - 1].enqueue_resps(&results[s..f]);
            s += operations[i - 1];
            operations[i - 1] = 0;
        }

        Ok(())
    }

    pub async fn async_execute_mut(
        &'a self,
        op: <D as Dispatch>::WriteOperation,
        rid: ReplicaToken,
        resp: &mut ReusableBoxFuture<'a, <D as Dispatch>::Response>,
    ) {
        // Enqueue the operation onto the thread local batch.
        // For a single thread the future will append the operation and yield.
        async { while !self.make_pending(op.clone(), rid.0) {} }.await;

        resp.set(async move {
            // Check for the response even before becoming the combiner,
            // as some other async combiner might have completed the work.
            match self.contexts[rid.0 - 1].res() {
                Some(res) => res,
                None => {
                    self.try_combine(rid.0);
                    self.get_response(rid.0)
                }
            }
        });
    }

    pub fn async_execute(
        &'a self,
        op: <D as Dispatch>::ReadOperation,
        idx: ReplicaToken,
        resp: &mut ReusableBoxFuture<'a, <D as Dispatch>::Response>,
    ) {
        resp.set(async move { self.read_only(op, idx.0) });
    }
}

#[cfg(test)]
mod test {
    extern crate std;

    use super::*;
    use std::vec;

    // Really dumb data structure to test against the Replica and shared log.
    #[derive(Default)]
    struct Data {
        junk: u64,
    }

    impl Dispatch for Data {
        type ReadOperation = u64;
        type WriteOperation = u64;
        type Response = Result<u64, ()>;

        fn dispatch(&self, _op: Self::ReadOperation) -> Self::Response {
            Ok(self.junk)
        }

        fn dispatch_mut(&mut self, _op: Self::WriteOperation) -> Self::Response {
            self.junk += 1;
            return Ok(107);
        }
    }

    // Tests whether we can construct a Replica given a log.
    #[test]
    fn test_replica_create() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::new_with_bytes(1024);
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);
        assert_eq!(repl.log_tkn, 1);
        assert_eq!(repl.combiner.load(Ordering::SeqCst), 0);
        assert_eq!(repl.next.load(Ordering::SeqCst), 1);
        assert_eq!(repl.contexts.len(), MAX_THREADS_PER_REPLICA);
        assert_eq!(
            repl.buffer.borrow().capacity(),
            MAX_THREADS_PER_REPLICA * Context::<u64, Result<u64, ()>>::batch_size()
        );
        assert_eq!(repl.inflight.borrow().len(), MAX_THREADS_PER_REPLICA);
        assert_eq!(
            repl.result.borrow().capacity(),
            MAX_THREADS_PER_REPLICA * Context::<u64, Result<u64, ()>>::batch_size()
        );
        assert_eq!(repl.data.read(0).junk, 0);
    }

    // Tests whether we can register with this replica and receive an idx.
    #[test]
    fn test_replica_register() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::new_with_bytes(1024);
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);
        assert_eq!(repl.register(), Some(ReplicaToken(1)));
        assert_eq!(repl.next.load(Ordering::SeqCst), 2);
        repl.next.store(17, Ordering::SeqCst);
        assert_eq!(repl.register(), Some(ReplicaToken(17)));
        assert_eq!(repl.next.load(Ordering::SeqCst), 18);
    }

    // Tests whether registering more than the maximum limit of threads per replica is disallowed.
    #[test]
    fn test_replica_register_none() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::new_with_bytes(1024);
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);
        repl.next
            .store(MAX_THREADS_PER_REPLICA + 1, Ordering::SeqCst);
        assert!(repl.register().is_none());
    }

    // Tests that we can successfully allow operations to go pending on this replica.
    #[test]
    fn test_replica_make_pending() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::new_with_bytes(1024);
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);
        let mut o = vec![];

        assert!(repl.make_pending(121, 8));
        assert_eq!(repl.contexts[7].ops(&mut o), 1);
        assert_eq!(o.len(), 1);
        assert_eq!(o[0], 121);
    }

    // Tests that we can't pend operations on a context that is already full of operations.
    #[test]
    fn test_replica_make_pending_false() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::new_with_bytes(1024);
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);
        for _i in 0..Context::<u64, Result<u64, ()>>::batch_size() {
            assert!(repl.make_pending(121, 1))
        }

        assert!(!repl.make_pending(11, 1));
    }

    // Tests that we can append and execute operations using try_combine().
    #[test]
    fn test_replica_try_combine() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::default();
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);
        let _idx = repl.register();

        repl.make_pending(121, 1);
        assert!(repl.try_combine(&slog).is_ok());

        assert_eq!(repl.combiner.load(Ordering::SeqCst), 0);
        assert_eq!(repl.data.read(0).junk, 1);
        assert_eq!(repl.contexts[0].res(), Some(Ok(107)));
    }

    // Tests whether try_combine() also applies pending operations on other threads to the log.
    #[test]
    fn test_replica_try_combine_pending() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::default();
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);

        repl.next.store(9, Ordering::SeqCst);
        repl.make_pending(121, 8);
        assert!(repl.try_combine(&slog).is_ok());

        assert_eq!(repl.data.read(0).junk, 1);
        assert_eq!(repl.contexts[7].res(), Some(Ok(107)));
    }

    // Tests whether try_combine() fails if someone else is currently flat combining.
    #[test]
    fn test_replica_try_combine_fail() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::new_with_bytes(1024);
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);

        repl.next.store(9, Ordering::SeqCst);
        repl.combiner.store(8, Ordering::SeqCst);
        repl.make_pending(121, 1);
        assert!(repl.try_combine(&slog).is_ok());

        assert_eq!(repl.data.read(0).junk, 0);
        assert_eq!(repl.contexts[0].res(), None);
    }

    // Tests whether we can execute an operation against the log using execute_mut().
    #[test]
    fn test_replica_execute_combine() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::default();
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);
        let idx = repl.register().unwrap();

        assert_eq!(Ok(107), repl.execute_mut(&slog, 121, idx).unwrap());
        assert_eq!(1, repl.data.read(0).junk);
    }

    // Tests whether get_response() retrieves a response to an operation that was executed
    // against a replica.
    #[test]
    fn test_replica_get_response() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::default();
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);
        let _idx = repl.register();

        repl.make_pending(121, 1);

        assert_eq!(repl.get_response(&slog, 1).unwrap(), Ok(107));
    }

    // Tests whether we can issue a read-only operation against the replica.
    #[test]
    fn test_replica_execute() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::default();
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);
        let idx = repl.register().expect("Failed to register with replica.");

        assert_eq!(Ok(107), repl.execute_mut(&slog, 121, idx).unwrap());
        assert_eq!(Ok(1), repl.execute(&slog, 11, idx).unwrap());
    }

    // Tests that execute() syncs up the replica with the log before
    // executing the read against the data structure.
    #[test]
    fn test_replica_execute_not_synced() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::default();
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);

        // Add in operations to the log off the side, not through the replica.
        let o = [121, 212];
        assert!(slog.append(&o, 2, |_o: u64, _i: usize| {}).is_ok());
        slog.exec(2, &mut |_o: u64, _i: usize| {});

        let t1 = repl.register().expect("Failed to register with replica.");
        assert_eq!(Ok(2), repl.execute(&slog, 11, t1).unwrap());
    }

    #[tokio::test]
    async fn test_box_reuse() {
        use futures::executor::block_on;
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(&slog);
        let idx = repl.register().expect("Unable to register to the replica");

        let op = 0;
        let mut resp: ReusableBoxFuture<<Data as Dispatch>::Response> =
            ReusableBoxFuture::new(async move { Ok(0) });
        repl.async_execute_mut(op, idx, &mut resp).await;
        let res = block_on(&mut resp).unwrap();
        assert_eq!(res, 107);

        repl.async_execute(op, idx, &mut resp);
        let res = block_on(resp).unwrap();
        assert_eq!(res, 1);
    }
}
