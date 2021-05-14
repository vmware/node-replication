// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use core::cell::RefCell;
use core::hint::spin_loop;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use alloc::sync::Arc;
use alloc::vec::Vec;

use arr_macro::arr;
use crossbeam_utils::CachePadded;

use super::context::Context;
use super::log::Log;
use super::Dispatch;
use super::LogMapper;

/// A token handed out to threads registered with replicas.
///
/// # Note
/// Ideally this would be an affine type and returned again by
/// `execute` and `execute_ro`. However it feels like this would
/// hurt API ergonomics a lot.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct ReplicaToken(pub usize);

/// To make it harder to use the same ReplicaToken on multiple threads.
impl !Send for ReplicaToken {}

impl ReplicaToken {
    /// Creates a new ReplicaToken
    ///
    /// # Safety
    /// This should only ever be used for the benchmark harness to create
    /// additional fake replica implementations.
    /// If we had a means to declare this not-pub we should do that instead.
    #[doc(hidden)]
    pub unsafe fn new(ident: usize) -> Self {
        ReplicaToken(ident)
    }

    /// Getter for id
    pub fn id(&self) -> usize {
        self.0
    }
}

/// The maximum number of threads that can be registered with a replica. If more than
/// this number of threads try to register, the register() function will return None.
///
/// # Important
/// If this number is adjusted due to the use of the `arr_macro::arr` macro we
/// have to adjust the `256` literals in the `new` constructor of `Replica`.
pub const MAX_THREADS_PER_REPLICA: usize = 256;
const_assert!(
    MAX_THREADS_PER_REPLICA >= 1 && (MAX_THREADS_PER_REPLICA & (MAX_THREADS_PER_REPLICA - 1) == 0)
);

/// An instance of a replicated data structure. Uses one or more shared logs
/// to scale operations on the data structure across cores and processors.
///
/// Takes in one type argument: `D` represents the underlying concurrent data
/// structure. `D` must implement the `Dispatch` trait.
///
/// A thread can be registered against the replica by calling `register()`. A
/// mutable operation can be issued by calling `execute_mut()` (immutable uses
/// `execute`). A mutable operation will be eventually executed against the replica
/// along with any operations that were received on other replicas that share
/// the same underlying log.
pub struct Replica<'a, D>
where
    D: Sized + Dispatch + Sync,
{
    /// Idx that will be handed out to the next thread that registers with the replica.
    next: CachePadded<AtomicUsize>,

    /// References to the shared logs that operations will be appended to and the
    /// data structure will be updated from.
    slog: Vec<Arc<Log<'a, <D as Dispatch>::WriteOperation>>>,

    /// The underlying replicated data structure. Shared between threads registered
    /// with this replica. Each replica maintains its own copy of the data structure.
    data: CachePadded<D>,

    /// TODO: Merge all the following variables in single structure.

    /// A replica receives a replica-identifier when it registers against
    /// a log. Each replica registers itself against all the shared logs.
    /// It is required when consuming operations from the log.
    idx: Vec<usize>,

    /// Thread idx of the thread currently responsible for flat combining. Zero
    /// if there isn't any thread actively performing flat combining on the log.
    /// This also doubles up as the combiner lock. The replica matains combiner lock
    /// per log.
    combiners: Vec<CachePadded<AtomicUsize>>,

    /// List of per-thread contexts. Threads buffer write operations in here when they
    /// cannot perform flat combining (because another thread might be doing so).
    ///
    /// The vector is initialized with `MAX_THREADS_PER_REPLICA` elements.
    contexts: Vec<CachePadded<Context<<D as Dispatch>::WriteOperation, <D as Dispatch>::Response>>>,

    /// Number of pending operations for each thread per log.
    pending: Vec<[CachePadded<AtomicBool>; MAX_THREADS_PER_REPLICA]>,

    /// A buffer of operations for flat combining. The combiner stages operations in
    /// here and then batch appends them into the shared log. This helps amortize
    /// the cost of the compare_and_swap() on the tail of the log.
    buffer: Vec<CachePadded<RefCell<Vec<<D as Dispatch>::WriteOperation>>>>,

    /// Number of operations collected by the combiner from each thread at any
    /// given point of time. Index `i` holds the number of operations collected from
    /// thread with identifier `i + 1`.
    inflight: Vec<CachePadded<RefCell<[usize; MAX_THREADS_PER_REPLICA]>>>,

    /// A buffer of results collected after flat combining. With the help of `inflight`,
    /// the combiner enqueues these results into the appropriate thread context.
    result: Vec<CachePadded<RefCell<Vec<<D as Dispatch>::Response>>>>,
}

/// The Replica is Sync. Member variables are protected by a CAS on `combiner`.
/// Contexts are thread-safe.
unsafe impl<'a, D> Sync for Replica<'a, D> where D: Sized + Sync + Dispatch {}

impl<'a, D> core::fmt::Debug for Replica<'a, D>
where
    D: Sized + Sync + Dispatch,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "Replica")
    }
}

impl<'a, D> Replica<'a, D>
where
    D: Sized + Dispatch + Default + Sync,
{
    /// Constructs an instance of a replicated data structure.
    ///
    /// Takes references to all the shared logs as an argument. The Logs are assumed to
    /// outlive the replica. The replica is bound to the log's lifetime.
    ///
    /// # Example
    ///
    /// ```
    /// use cnr::Dispatch;
    /// use cnr::Log;
    /// use cnr::LogMapper;
    /// use cnr::Replica;
    ///
    /// use core::sync::atomic::{AtomicUsize, Ordering};
    /// use std::sync::Arc;
    ///
    /// // The data structure we want replicated.
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: AtomicUsize,
    /// }
    ///
    /// #[derive(Debug, Eq, PartialEq, Clone, Copy)]
    /// pub struct OpWr(pub usize);
    ///
    /// impl LogMapper for OpWr {
    ///     // Only one log used for the example, hence returning 0.
    ///     fn hash(&self) -> usize { 0 }
    /// }
    ///
    /// #[derive(Debug, Eq, PartialEq, Clone, Copy)]
    /// pub struct OpRd(());
    ///
    /// impl LogMapper for OpRd {
    ///     // Only one log used for the example, hence returning 0.
    ///     fn hash(&self) -> usize { 0 }
    /// }
    ///
    /// // This trait allows the `Data` to be used with node-replication.
    /// impl Dispatch for Data {
    ///     type ReadOperation = OpRd;
    ///     type WriteOperation = OpWr;
    ///     type Response = Option<usize>;
    ///
    ///     // A read returns the underlying u64.
    ///     fn dispatch(
    ///         &self,
    ///         _op: Self::ReadOperation,
    ///     ) -> Self::Response {
    ///         Some(self.junk.load(Ordering::Relaxed))
    ///     }
    ///
    ///     // A write updates the underlying u64.
    ///     fn dispatch_mut(
    ///         &self,
    ///         op: Self::WriteOperation,
    ///     ) -> Self::Response {
    ///         self.junk.store(op.0, Ordering::Relaxed);
    ///         None
    ///     }
    /// }
    ///
    /// // Create one or more logs.
    /// let log = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
    ///
    /// // Create a replica that uses the above log.
    /// let replica = Replica::<Data>::new(vec![log]);
    /// ```
    pub fn new<'b>(
        logs: Vec<Arc<Log<'b, <D as Dispatch>::WriteOperation>>>,
    ) -> Arc<Replica<'b, D>> {
        Replica::with_data(logs, Default::default())
    }
}

impl<'a, D> Replica<'a, D>
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
    pub fn with_data<'b>(
        logs: Vec<Arc<Log<'b, <D as Dispatch>::WriteOperation>>>,
        d: D,
    ) -> Arc<Replica<'b, D>> {
        let mut uninit_replica: Arc<MaybeUninit<Replica<D>>> = Arc::new_zeroed();

        // This is the preferred but unsafe mode of initialization as it avoids
        // putting the (often big) Replica object on the stack first.
        unsafe {
            let uninit_ptr = Arc::get_mut_unchecked(&mut uninit_replica).as_mut_ptr();
            let nlogs = logs.len();
            // Register the replica with all the logs.
            let mut idx = Vec::with_capacity(nlogs);
            for log in logs.iter() {
                idx.push(log.register().unwrap());
            }
            // Allocate a combiner for each log.
            let mut combiners = Vec::with_capacity(nlogs);
            let mut pending = Vec::with_capacity(nlogs);
            for _i in 0..nlogs {
                combiners.push(CachePadded::new(AtomicUsize::new(0)));
                pending.push(arr![CachePadded::new(AtomicBool::new(false)); 256]);
            }

            uninit_ptr.write(Replica {
                idx,
                combiners,
                next: CachePadded::new(AtomicUsize::new(1)),
                contexts: Vec::with_capacity(MAX_THREADS_PER_REPLICA),
                pending,
                buffer: vec![
                    CachePadded::new(RefCell::new(
                        Vec::with_capacity(
                            MAX_THREADS_PER_REPLICA
                                * Context::<
                                    <D as Dispatch>::WriteOperation,
                                    <D as Dispatch>::Response,
                                >::batch_size(),
                        ),
                    ));
                    nlogs
                ],
                inflight: vec![
                    CachePadded::new(RefCell::new(arr![Default::default(); 256]));
                    nlogs
                ],
                result: vec![
                    CachePadded::new(RefCell::new(
                        Vec::with_capacity(
                            MAX_THREADS_PER_REPLICA
                                * Context::<
                                    <D as Dispatch>::WriteOperation,
                                    <D as Dispatch>::Response,
                                >::batch_size(),
                        ),
                    ));
                    nlogs
                ],
                slog: logs.clone(),
                data: CachePadded::new(d),
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
    /// use cnr::Dispatch;
    /// use cnr::Log;
    /// use cnr::LogMapper;
    /// use cnr::Replica;
    ///
    /// use core::sync::atomic::{AtomicUsize, Ordering};
    /// use std::sync::Arc;
    ///
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: AtomicUsize,
    /// }
    ///
    /// #[derive(Debug, Eq, PartialEq, Clone, Copy)]
    /// pub struct OpWr(pub usize);
    ///
    /// impl LogMapper for OpWr {
    ///     // Only one log used for the example, hence returning 0.
    ///     fn hash(&self) -> usize { 0 }
    /// }
    ///
    /// #[derive(Debug, Eq, PartialEq, Clone, Copy)]
    /// pub struct OpRd(());
    ///
    /// impl LogMapper for OpRd {
    ///     // Only one log used for the example, hence returning 0.
    ///     fn hash(&self) -> usize { 0 }
    /// }
    ///
    /// impl Dispatch for Data {
    ///     type ReadOperation = OpRd;
    ///     type WriteOperation = OpWr;
    ///     type Response = Option<usize>;
    ///
    ///     fn dispatch(
    ///         &self,
    ///         _op: Self::ReadOperation,
    ///     ) -> Self::Response {
    ///         Some(self.junk.load(Ordering::Relaxed))
    ///     }
    ///
    ///     fn dispatch_mut(
    ///         &self,
    ///         op: Self::WriteOperation,
    ///     ) -> Self::Response {
    ///         self.junk.store(op.0, Ordering::Relaxed);
    ///         None
    ///     }
    /// }
    ///
    /// let log = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
    /// let replica = Replica::<Data>::new(vec![log]);
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
    /// use cnr::Dispatch;
    /// use cnr::Log;
    /// use cnr::LogMapper;
    /// use cnr::Replica;
    ///
    /// use core::sync::atomic::{AtomicUsize, Ordering};
    /// use std::sync::Arc;
    ///
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: AtomicUsize,
    /// }
    ///
    /// #[derive(Debug, Eq, PartialEq, Clone, Copy)]
    /// pub struct OpWr(pub usize);
    ///
    /// impl LogMapper for OpWr {
    ///     // Only one log used for the example, hence returning 0.
    ///     fn hash(&self) -> usize { 0 }
    /// }
    ///
    /// #[derive(Debug, Eq, PartialEq, Clone, Copy)]
    /// pub struct OpRd(());
    ///
    /// impl LogMapper for OpRd {
    ///     // Only one log used for the example, hence returning 0.
    ///     fn hash(&self) -> usize { 0 }
    /// }
    ///
    /// impl Dispatch for Data {
    ///     type ReadOperation = OpRd;
    ///     type WriteOperation = OpWr;
    ///     type Response = Option<usize>;
    ///
    ///     fn dispatch(
    ///         &self,
    ///         _op: Self::ReadOperation,
    ///     ) -> Self::Response {
    ///         Some(self.junk.load(Ordering::Relaxed))
    ///     }
    ///
    ///     fn dispatch_mut(
    ///         &self,
    ///         op: Self::WriteOperation,
    ///     ) -> Self::Response {
    ///         self.junk.store(op.0, Ordering::Relaxed);
    ///         None
    ///     }
    /// }
    ///
    /// let log = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
    /// let replica = Replica::<Data>::new(vec![log]);
    /// let idx = replica.register().expect("Failed to register with replica.");
    ///
    /// // execute_mut() can be used to write to the replicated data structure.
    /// let res = replica.execute_mut(OpWr(100), idx);
    /// assert_eq!(None, res);
    pub fn execute_mut(
        &self,
        op: <D as Dispatch>::WriteOperation,
        idx: ReplicaToken,
    ) -> <D as Dispatch>::Response {
        let hash = op.hash() % self.slog.len();

        // Enqueue the operation onto the thread local batch and then try to flat combine.
        self.make_pending(op.clone(), idx.0, hash);

        // A thread becomes combiner for operations with hash same as its own operation.
        self.try_combine(idx.0, hash);

        // Return the response to the caller function.
        self.get_response(idx.0, hash)
    }

    /// Executes a read-only operation against this replica and returns a response.
    /// `idx` is an identifier for the thread performing the execute operation.
    ///
    /// # Example
    ///
    /// ```
    /// use cnr::Dispatch;
    /// use cnr::Log;
    /// use cnr::LogMapper;
    /// use cnr::Replica;
    ///
    /// use core::sync::atomic::{AtomicUsize, Ordering};
    /// use std::sync::Arc;
    ///
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: AtomicUsize,
    /// }
    ///
    /// #[derive(Debug, Eq, PartialEq, Clone, Copy)]
    /// pub struct OpWr(pub usize);
    ///
    /// impl LogMapper for OpWr {
    ///     // Only one log used for the example, hence returning 0.
    ///     fn hash(&self) -> usize { 0 }
    /// }
    ///
    /// #[derive(Debug, Eq, PartialEq, Clone, Copy)]
    /// pub struct OpRd(());
    ///
    /// impl LogMapper for OpRd {
    ///     // Only one log used for the example, hence returning 0.
    ///     fn hash(&self) -> usize { 0 }
    /// }
    ///
    /// impl Dispatch for Data {
    ///     type ReadOperation = OpRd;
    ///     type WriteOperation = OpWr;
    ///     type Response = Option<usize>;
    ///
    ///     fn dispatch(
    ///         &self,
    ///         _op: Self::ReadOperation,
    ///     ) -> Self::Response {
    ///         Some(self.junk.load(Ordering::Relaxed))
    ///     }
    ///
    ///     fn dispatch_mut(
    ///         &self,
    ///         op: Self::WriteOperation,
    ///     ) -> Self::Response {
    ///         self.junk.store(op.0, Ordering::Relaxed);
    ///         None
    ///     }
    /// }
    ///
    /// let log = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
    /// let replica = Replica::<Data>::new(vec![log]);
    /// let idx = replica.register().expect("Failed to register with replica.");
    /// let _wr = replica.execute_mut(OpWr(100), idx);
    ///
    /// // execute() can be used to read from the replicated data structure.
    /// let res = replica.execute(OpRd(()), idx);
    /// assert_eq!(Some(100), res);
    pub fn execute(
        &self,
        op: <D as Dispatch>::ReadOperation,
        idx: ReplicaToken,
    ) -> <D as Dispatch>::Response {
        self.read_only(op, idx.0)
    }

    /// Busy waits until a response is available within the thread's context.
    /// `idx` identifies this thread.
    fn get_response(&self, idx: usize, hash: usize) -> <D as Dispatch>::Response {
        let mut iter = 0;
        let interval = 1 << 29;

        // Keep trying to retrieve a response from the thread context. After trying `interval`
        // times with no luck, try to perform flat combining to make some progress.
        loop {
            let r = self.contexts[idx - 1].res();
            if r.is_some() {
                return r.unwrap();
            }

            iter += 1;

            if iter == interval {
                self.try_combine(idx, hash);
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
    /// TODO: find a way to pass hashidx here.
    #[doc(hidden)]
    pub fn verify<F: FnMut(&D)>(&self, mut v: F) {
        // Acquire the combiner lock before attempting anything on the data structure.
        // Use an idx greater than the maximum that can be allocated.
        while self.combiners[0].compare_exchange_weak(
            0,
            MAX_THREADS_PER_REPLICA + 2,
            Ordering::Acquire,
            Ordering::Acquire,
        ) != Ok(0)
        {
            spin_loop();
        }

        let mut f = |o: <D as Dispatch>::WriteOperation, _i: usize| {
            self.data.dispatch_mut(o);
        };

        self.slog[0].exec(self.idx[0], &mut f);

        v(&self.data);

        self.combiners[0].store(0, Ordering::Release);
    }

    /// This method is useful when a replica stops making progress and some threads
    /// on another replica are still active. The active replica will use all the entries
    /// in the log and won't be able perform garbage collection because of the inactive
    /// replica. So, this method syncs up the replica against the underlying log.
    pub fn sync(&self, idx: ReplicaToken) {
        let nlogs = self.slog.len();
        for i in 0..nlogs {
            let ctail = self.slog[i].get_ctail();
            while !self.slog[i].is_replica_synced_for_reads(self.idx[i], ctail) {
                self.try_combine(idx.0, i);
                spin_loop();
            }
        }
    }

    /// This method is useful to when a replica stops consuming a particular
    /// log and some other replica is still using this a log.
    ///
    /// No need to run in a loop because the replica will
    /// be synced for log_id if there is an active combiner.
    pub fn sync_log(&self, idx: ReplicaToken, log_id: usize) {
        self.try_combine(idx.0, log_id - 1);
    }

    /// Issues a read-only operation against the replica and returns a response.
    /// Makes sure the replica is synced up against the log before doing so.
    fn read_only(
        &self,
        op: <D as Dispatch>::ReadOperation,
        tid: usize,
    ) -> <D as Dispatch>::Response {
        // Calculate the hash of the operation to map the operation to a log.
        let hash_idx = op.hash() % self.slog.len();

        // We can perform the read only if our replica is synced up against
        // the shared log. If it isn't, then try to combine until it is synced up.
        let ctail = self.slog[hash_idx].get_ctail();
        while !self.slog[hash_idx].is_replica_synced_for_reads(self.idx[hash_idx], ctail) {
            self.try_combine(tid, hash_idx);
            spin_loop();
        }

        self.data.dispatch(op)
    }

    /// Enqueues an operation inside a thread local context. Returns a boolean
    /// indicating whether the operation was enqueued (true) or not (false).
    #[inline(always)]
    fn make_pending(&self, op: <D as Dispatch>::WriteOperation, tid: usize, hash: usize) -> bool {
        loop {
            if self.contexts[tid - 1].enqueue(op.clone(), hash) {
                self.pending[hash][tid - 1].store(true, Ordering::Release);
                break;
            }
        }
        true
    }

    /// Appends an operation to the log and attempts to perform flat combining.
    /// Accepts a thread `tid` as an argument. Required to acquire the combiner lock.
    fn try_combine(&self, tid: usize, hashidx: usize) {
        // First, check if there already is a flat combiner. If there is no active flat combiner
        // then try to acquire the combiner lock. If there is, then just return.
        for _i in 0..4 {
            if unsafe {
                core::ptr::read_volatile(
                    &self.combiners[hashidx]
                        as *const crossbeam_utils::CachePadded<core::sync::atomic::AtomicUsize>
                        as *const usize,
                )
            } != 0
            {
                return;
            };
        }

        // Try to become the combiner here. If this fails, then simply return.
        if self.combiners[hashidx].compare_exchange_weak(
            0,
            tid,
            Ordering::Acquire,
            Ordering::Acquire,
        ) != Ok(0)
        {
            return;
        }

        // Successfully became the combiner; perform one round of flat combining.
        self.combine(hashidx);

        // Allow other threads to perform flat combining once we have finished all our work.
        // At this point, we've dropped all mutable references to thread contexts and to
        // the staging buffer as well.
        self.combiners[hashidx].store(0, Ordering::Release);
    }

    /// Performs one round of flat combining. Collects, appends and executes operations.
    #[inline(always)]
    fn combine(&self, hashidx: usize) {
        //  TODO: may need to be in a per-log state context
        let mut buffer = self.buffer[hashidx].borrow_mut();
        let mut operations = self.inflight[hashidx].borrow_mut();
        let mut results = self.result[hashidx].borrow_mut();
        let pending = &self.pending[hashidx];

        buffer.clear();
        results.clear();

        let next = self.next.load(Ordering::Relaxed);

        // Collect operations from each thread registered with this replica.
        for tid in 1..next {
            if pending[tid - 1].compare_exchange_weak(
                true,
                false,
                Ordering::Release,
                Ordering::Relaxed,
            ) == Ok(true)
            {
                // pass hash of current op to contexts, only get ops from context that have the same hash/log id
                operations[tid - 1] = self.contexts[tid - 1].ops(&mut buffer, hashidx);
            }
        }

        // Append all collected operations into the shared log. We pass a closure
        // in here because operations on the log might need to be consumed for GC.
        {
            let f = |o: <D as Dispatch>::WriteOperation, i: usize| {
                let resp = self.data.dispatch_mut(o);
                if i == self.idx[hashidx] {
                    results.push(resp);
                }
            };
            self.slog[hashidx].append(&buffer, self.idx[hashidx], f);
        }

        // Execute any operations on the shared log against this replica.
        {
            let mut f = |o: <D as Dispatch>::WriteOperation, i: usize| {
                let resp = self.data.dispatch_mut(o);
                if i == self.idx[hashidx] {
                    results.push(resp)
                };
            };
            self.slog[hashidx].exec(self.idx[hashidx], &mut f);
        }

        // Return/Enqueue responses back into the appropriate thread context(s).
        let (mut s, mut f) = (0, 0);
        // TODO: hashing makes this non-linear, need to take into account which operations
        // belong to our current combiner round...
        for i in 1..next {
            if operations[i - 1] == 0 {
                continue;
            };

            f += operations[i - 1];
            self.contexts[i - 1].enqueue_resps(&results[s..f]);
            s += operations[i - 1];
            operations[i - 1] = 0;
        }
    }
}

#[cfg(test)]
mod test {
    extern crate std;

    use super::*;
    use std::vec;
    use std::{thread, time};

    // Really dumb data structure to test against the Replica and shared log.
    #[derive(Default)]
    struct Data {
        junk: AtomicUsize,
    }

    #[derive(Debug, Eq, PartialEq, Clone, Copy)]
    pub struct OpWr(usize);

    impl LogMapper for OpWr {
        fn hash(&self) -> usize {
            0
        }
    }

    #[derive(Debug, Eq, PartialEq, Clone, Copy)]
    pub struct OpRd(usize);

    impl LogMapper for OpRd {
        fn hash(&self) -> usize {
            0
        }
    }

    impl Dispatch for Data {
        type ReadOperation = OpRd;
        type WriteOperation = OpWr;
        type Response = Result<usize, ()>;

        fn dispatch(&self, _op: Self::ReadOperation) -> Self::Response {
            Ok(self.junk.load(Ordering::Relaxed))
        }

        fn dispatch_mut(&self, _op: Self::WriteOperation) -> Self::Response {
            self.junk.fetch_add(1, Ordering::Relaxed);
            return Ok(107);
        }
    }

    // Tests whether we can construct a Replica given a log.
    #[test]
    fn test_replica_create() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::new(1024, 1));
        let repl = Replica::<Data>::new(vec![slog]);
        assert_eq!(repl.idx[0], 1);
        assert_eq!(repl.combiners[0].load(Ordering::SeqCst), 0);
        assert_eq!(repl.next.load(Ordering::SeqCst), 1);
        assert_eq!(repl.contexts.len(), MAX_THREADS_PER_REPLICA);
        assert_eq!(
            repl.buffer[0].borrow().capacity(),
            MAX_THREADS_PER_REPLICA * Context::<u64, Result<u64, ()>>::batch_size()
        );
        assert_eq!(repl.inflight[0].borrow().len(), MAX_THREADS_PER_REPLICA);
        assert_eq!(
            repl.result[0].borrow().capacity(),
            MAX_THREADS_PER_REPLICA * Context::<u64, Result<u64, ()>>::batch_size()
        );
        assert_eq!(repl.data.junk.load(Ordering::Relaxed), 0);
    }

    // Tests whether we can register with this replica and receive an idx.
    #[test]
    fn test_replica_register() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::new(1024, 1));
        let repl = Replica::<Data>::new(vec![slog]);
        assert_eq!(repl.register(), Some(ReplicaToken(1)));
        assert_eq!(repl.next.load(Ordering::SeqCst), 2);
        repl.next.store(17, Ordering::SeqCst);
        assert_eq!(repl.register(), Some(ReplicaToken(17)));
        assert_eq!(repl.next.load(Ordering::SeqCst), 18);
    }

    // Tests whether registering more than the maximum limit of threads per replica is disallowed.
    #[test]
    fn test_replica_register_none() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::new(1024, 1));
        let repl = Replica::<Data>::new(vec![slog]);
        repl.next
            .store(MAX_THREADS_PER_REPLICA + 1, Ordering::SeqCst);
        assert!(repl.register().is_none());
    }

    // Tests that we can successfully allow operations to go pending on this replica.
    #[test]
    fn test_replica_make_pending() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::new(1024, 1));
        let repl = Replica::<Data>::new(vec![slog]);
        let mut o = vec![];

        assert!(repl.make_pending(OpWr(121), 8, 0));
        assert_eq!(repl.contexts[7].ops(&mut o, 0), 1);
        assert_eq!(o.len(), 1);
        assert_eq!(o[0], OpWr(121));
    }

    // Tests that we can append and execute operations using try_combine().
    #[test]
    fn test_replica_try_combine() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(vec![slog]);
        let _idx = repl.register();

        repl.make_pending(OpWr(121), 1, 0);
        repl.try_combine(1, 0);

        assert_eq!(repl.combiners[0].load(Ordering::SeqCst), 0);
        assert_eq!(repl.data.junk.load(Ordering::Relaxed), 1);
        assert_eq!(repl.contexts[0].res(), Some(Ok(107)));
    }

    // Tests whether try_combine() also applies pending operations on other threads to the log.
    #[test]
    fn test_replica_try_combine_pending() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(vec![slog]);

        repl.next.store(9, Ordering::SeqCst);
        repl.make_pending(OpWr(121), 8, 0);
        repl.try_combine(1, 0);

        assert_eq!(repl.data.junk.load(Ordering::Relaxed), 1);
        assert_eq!(repl.contexts[7].res(), Some(Ok(107)));
    }

    // Tests whether try_combine() fails if someone else is currently flat combining.
    #[test]
    fn test_replica_try_combine_fail() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::new(1024, 1));
        let repl = Replica::<Data>::new(vec![slog]);

        repl.next.store(9, Ordering::SeqCst);
        repl.combiners[0].store(8, Ordering::SeqCst);
        repl.make_pending(OpWr(121), 1, 0);
        repl.try_combine(1, 0);

        assert_eq!(repl.data.junk.load(Ordering::Relaxed), 0);
        assert_eq!(repl.contexts[0].res(), None);
    }

    // Tests whether we can execute an operation against the log using execute_mut().
    #[test]
    fn test_replica_execute_combine() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(vec![slog]);
        let idx = repl.register().unwrap();

        assert_eq!(Ok(107), repl.execute_mut(OpWr(121), idx));
        assert_eq!(1, repl.data.junk.load(Ordering::Relaxed));
    }

    // Tests whether get_response() retrieves a response to an operation that was executed
    // against a replica.
    #[test]
    fn test_replica_get_response() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(vec![slog]);
        let _idx = repl.register();

        let op = OpWr(121);
        let hash = op.hash();
        repl.make_pending(op, 1, hash);

        assert_eq!(repl.get_response(1, hash), Ok(107));
    }

    // Tests whether we can issue a read-only operation against the replica.
    #[test]
    fn test_replica_execute() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(vec![slog]);
        let idx = repl.register().expect("Failed to register with replica.");

        assert_eq!(Ok(107), repl.execute_mut(OpWr(121), idx));
        assert_eq!(Ok(1), repl.execute(OpRd(121), idx));
    }

    // Tests that execute() syncs up the replica with the log before
    // executing the read against the data structure.
    #[test]
    fn test_replica_execute_not_synced() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(vec![slog.clone()]);

        // Add in operations to the log off the side, not through the replica.
        let o = [OpWr(121), OpWr(212)];
        slog.append(&o, 2, |_o: OpWr, _i: usize| {});
        slog.exec(2, &mut |_o: OpWr, _i: usize| {});

        let t1 = repl.register().expect("Failed to register with replica.");
        assert_eq!(Ok(2), repl.execute(OpRd(11), t1));
    }

    // Tests if there are log number of combiners and all of
    // them can acquire the combiner lock in parallel.
    #[test]
    fn test_multiple_combiner() {
        let slog1 = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let slog2 = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let slog3 = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let slog4 = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let logs = vec![slog1, slog2, slog3, slog4];

        let repl = Replica::<Data>::new(logs.clone());

        for i in 0..logs.len() {
            repl.combiners[i].store(i + 1, Ordering::Relaxed);
        }

        for i in 0..logs.len() {
            assert_eq!(repl.combiners[i].load(Ordering::Relaxed), i + 1);
        }
    }

    // Tests if there are log number of combiners and the test panic if we try
    // to acquire more number of combiner than the number of logs.
    #[test]
    #[should_panic]
    fn test_more_than_nlogs_combiner() {
        let slog1 = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let slog2 = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let slog3 = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let slog4 = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let logs = vec![slog1, slog2, slog3, slog4];

        let repl = Replica::<Data>::new(logs.clone());

        for i in 0..logs.len() + 1 {
            repl.combiners[i].store(i + 1, Ordering::Relaxed);
        }

        for i in 0..logs.len() {
            assert_eq!(repl.combiners[i].load(Ordering::Relaxed), i + 1);
        }
    }

    // Tests if there are log number of combiners and all of
    // them can acquire the combiner lock in parallel.
    #[test]
    fn test_multiple_parallel_combiner() {
        // Really dumb data structure to test against the Replica and shared log.
        #[derive(Default)]
        struct Block {
            junk: AtomicUsize,
        }

        impl Dispatch for Block {
            type ReadOperation = OpRd;
            type WriteOperation = OpWr;
            type Response = Result<usize, ()>;

            fn dispatch(&self, _op: Self::ReadOperation) -> Self::Response {
                Ok(self.junk.load(Ordering::Relaxed))
            }

            fn dispatch_mut(&self, _op: Self::WriteOperation) -> Self::Response {
                // sleep for some time so that test thread can check the combiners status
                thread::sleep(time::Duration::from_secs(2));
                self.junk.fetch_add(1, Ordering::Relaxed);
                return Ok(107);
            }
        }

        let slog1 = Arc::new(Log::<<Block as Dispatch>::WriteOperation>::default());
        let slog2 = Arc::new(Log::<<Block as Dispatch>::WriteOperation>::default());
        let slog3 = Arc::new(Log::<<Block as Dispatch>::WriteOperation>::default());
        let slog4 = Arc::new(Log::<<Block as Dispatch>::WriteOperation>::default());
        let logs = vec![slog1, slog2, slog3, slog4];

        let repl = Replica::<Block>::new(logs.clone());
        let mut threads = Vec::with_capacity(logs.len());
        let nlogs = logs.len();

        for i in 0..nlogs {
            let r = repl.clone();
            threads.push(thread::spawn(move || {
                let t = r.register().unwrap();
                let hash = t.0 % nlogs;
                r.make_pending(OpWr(i), t.0, hash);

                r.try_combine(t.0, hash);
            }));
        }

        // Test thread, sleep for some times and checks the combiner status
        let r = repl.clone();
        threads.push(thread::spawn(move || {
            thread::sleep(time::Duration::from_secs(1));
            for i in 0..nlogs {
                let tid = if i > 0 { i } else { nlogs };
                assert_eq!(r.combiners[i].load(Ordering::SeqCst), tid);
            }
        }));

        for thread in threads.into_iter() {
            thread.join().unwrap();
        }

        assert_eq!(repl.data.junk.load(Ordering::Relaxed), 4);
        for i in 0..logs.len() {
            assert_eq!(repl.contexts[i].res(), Some(Ok(107)));
        }
    }
}
