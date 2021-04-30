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

/// An instance of per log state maintained by each replica.
pub(self) struct LogState<'a, D>
where
    D: Sized + Default + Dispatch + Sync,
{
    /// References to the shared logs that operations will be appended to and the
    /// data structure will be updated from.
    slog: Arc<Log<'a, <D as Dispatch>::WriteOperation>>,

    /// A replica receives a replica-identifier when it registers against
    /// a log. Each replica registers itself against all the shared logs.
    /// It is required when consuming operations from the log.
    idx: usize,

    /// Thread idx of the thread currently responsible for flat combining. Zero
    /// if there isn't any thread actively performing flat combining on the log.
    /// This also doubles up as the combiner lock. The replica matains combiner lock
    /// per log.
    combiner: CachePadded<AtomicUsize>,

    /// Number of pending operations for each thread per log.
    pending: [CachePadded<AtomicBool>; MAX_THREADS_PER_REPLICA],

    /// A buffer of operations for flat combining. The combiner stages operations in
    /// here and then batch appends them into the shared log. This helps amortize
    /// the cost of the compare_and_swap() on the tail of the log.
    buffer: CachePadded<RefCell<Vec<<D as Dispatch>::WriteOperation>>>,

    /// A buffer of scan type operations for flat combining.
    scan_buffer: CachePadded<RefCell<Vec<<D as Dispatch>::WriteOperation>>>,

    /// Number of operations collected by the combiner from each thread at any
    /// given point of time. Index `i` holds the number of operations collected from
    /// thread with identifier `i + 1`.
    inflight: CachePadded<RefCell<[usize; MAX_THREADS_PER_REPLICA]>>,

    /// A buffer of results collected after flat combining. With the help of `inflight`,
    /// the combiner enqueues these results into the appropriate thread context.
    result: CachePadded<RefCell<Vec<<D as Dispatch>::Response>>>,
}

impl<'a, D> LogState<'a, D>
where
    D: Sized + Default + Dispatch + Sync,
{
    pub fn new(log: Arc<Log<'a, <D as Dispatch>::WriteOperation>>) -> LogState<'a, D> {
        let idx = log.register().unwrap();
        LogState {
            slog: log.clone(),
            idx,
            combiner: CachePadded::new(AtomicUsize::new(0)),
            pending: arr![CachePadded::new(AtomicBool::new(false)); 256],
            buffer:
                CachePadded::new(
                    RefCell::new(
                        Vec::with_capacity(
                            MAX_THREADS_PER_REPLICA
                                * Context::<
                                    <D as Dispatch>::WriteOperation,
                                    <D as Dispatch>::Response,
                                >::batch_size(),
                        ),
                    ),
                ),
            inflight: CachePadded::new(RefCell::new(arr![Default::default(); 256])),
            scan_buffer: CachePadded::new(RefCell::new(Vec::with_capacity(1))),
            result:
                CachePadded::new(
                    RefCell::new(
                        Vec::with_capacity(
                            MAX_THREADS_PER_REPLICA
                                * Context::<
                                    <D as Dispatch>::WriteOperation,
                                    <D as Dispatch>::Response,
                                >::batch_size(),
                        ),
                    ),
                ),
        }
    }
}

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
    D: Sized + Default + Dispatch + Sync,
{
    /// Idx that will be handed out to the next thread that registers with the replica.
    next: CachePadded<AtomicUsize>,

    /// The underlying replicated data structure. Shared between threads registered
    /// with this replica. Each replica maintains its own copy of the data structure.
    data: CachePadded<D>,

    /// List of per-thread contexts. Threads buffer write operations in here when they
    /// cannot perform flat combining (because another thread might be doing so).
    ///
    /// The vector is initialized with `MAX_THREADS_PER_REPLICA` elements.
    contexts: Vec<CachePadded<Context<<D as Dispatch>::WriteOperation, <D as Dispatch>::Response>>>,

    /// Scan operation
    offsets: Vec<RefCell<Vec<usize>>>,

    /// An instance of per log state maintained by each replica.
    logstate: Vec<CachePadded<LogState<'a, D>>>,

    /// A scan lock, needed to atomically append scan entries in multiple logs;
    /// otherwise there might be a circular dependency that can cause a deadlock.
    scanlock: CachePadded<AtomicUsize>,
}

/// The Replica is Sync. Member variables are protected by a CAS on `combiner`.
/// Contexts are thread-safe.
unsafe impl<'a, D> Sync for Replica<'a, D> where D: Sized + Default + Sync + Dispatch {}

impl<'a, D> core::fmt::Debug for Replica<'a, D>
where
    D: Sized + Default + Sync + Dispatch,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "Replica")
    }
}

impl<'a, D> Replica<'a, D>
where
    D: Sized + Default + Dispatch + Sync,
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
        let mut uninit_replica: Arc<MaybeUninit<Replica<D>>> = Arc::new_zeroed();

        // This is the preferred but unsafe mode of initialization as it avoids
        // putting the (often big) Replica object on the stack first.
        unsafe {
            let uninit_ptr = Arc::get_mut_unchecked(&mut uninit_replica).as_mut_ptr();

            uninit_ptr.write(Replica {
                next: CachePadded::new(AtomicUsize::new(1)),
                data: CachePadded::new(D::default()),
                logstate: Vec::with_capacity(logs.len()),
                contexts: Vec::with_capacity(MAX_THREADS_PER_REPLICA),
                offsets: Vec::with_capacity(MAX_THREADS_PER_REPLICA),
                scanlock: CachePadded::new(AtomicUsize::new(0)),
            });

            let mut replica = uninit_replica.assume_init();
            // Add `MAX_THREADS_PER_REPLICA` contexts
            for _idx in 0..MAX_THREADS_PER_REPLICA {
                let replica_mut = Arc::get_mut(&mut replica).unwrap();
                replica_mut.contexts.push(Default::default());
                replica_mut
                    .offsets
                    .push(RefCell::new(Vec::with_capacity(logs.len())));
            }

            // Add per-log state
            for log in logs.iter() {
                Arc::get_mut(&mut replica)
                    .unwrap()
                    .logstate
                    .push(CachePadded::new(LogState::new(log.clone())));
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
        let nthreads = self.next.load(Ordering::Relaxed) - 1;
        let hash = (op.hash() % nthreads) % self.logstate.len();

        // Enqueue the operation onto the thread local batch and then try to flat combine.
        self.make_pending(op.clone(), idx.0, hash, false);

        // A thread becomes combiner for operations with hash same as its own operation.
        self.try_combine(idx.0, hash);

        // Return the response to the caller function.
        self.get_response(idx.0, hash)
    }

    pub fn execute_mut_scan(
        &self,
        op: <D as Dispatch>::WriteOperation,
        idx: ReplicaToken,
    ) -> <D as Dispatch>::Response {
        let nlogs = self.logstate.len();
        let nthreads = self.next.load(Ordering::Relaxed) - 1;

        // If there is only one log in the system, then execute
        // scan operation as a mutable operations.
        if nlogs == 1 || nthreads == 1 {
            return self.execute_mut(op, idx);
        }

        // Acquire the scan lock.
        self.acquire_scan_lock(idx.0);
        let hash = 0; /* Fake hash; scan op is appended to each log.*/

        // Enqueue the operation onto the thread local batch and then try to flat combine.
        self.make_pending(op.clone(), idx.0, 0, true);

        // A thread becomes combiner for operations with hash same as its own operation.
        self.try_combine(idx.0, hash);

        // Return the response to the caller function.
        let resp = self.get_response(idx.0, hash);
        self.release_scan_lock();

        //error!("{}", idx.0);
        resp
    }

    pub fn append_scan(&self, op: <D as Dispatch>::WriteOperation, tid: usize) {
        let nlogs = self.logstate.len();

        let mut entries = self.offsets[tid].borrow_mut();
        entries.clear();

        // We don't have response buffer here, which means that we cannot execute exec()
        // inside `append_scan` function. if there is no space in the log for a single
        // entry, call try_combine first.
        for logidx in 0..nlogs {
            let entry = loop {
                match self.logstate[logidx].slog.try_append_scan(
                    &op,
                    self.logstate[logidx].idx,
                    &entries,
                ) {
                    Ok(entry) => break entry,
                    Err(_) => {
                        self.try_combine(tid, logidx);
                        continue;
                    }
                }
            };
            entries.push(entry);
        }
        //error!("{} - {:?}", tid, entries);

        // Update scan entry depends_on.
        self.logstate[0]
            .slog
            .fix_scan_entry(&op, self.logstate[0].idx, &entries);
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
        while self.logstate[0].combiner.compare_exchange_weak(
            0,
            MAX_THREADS_PER_REPLICA + 2,
            Ordering::Acquire,
            Ordering::Acquire,
        ) != Ok(0)
        {
            spin_loop();
        }

        let mut f = |o: <D as Dispatch>::WriteOperation, _i: usize, _is_scan, _depends_on| {
            self.data.dispatch_mut(o);
        };

        self.logstate[0].slog.exec(self.logstate[0].idx, &mut f);

        v(&self.data);

        self.logstate[0].combiner.store(0, Ordering::Release);
    }

    fn sync_for_scan(
        &self,
        tid: usize,
        start: usize,
        end: usize,
        try_combine: bool,
        tails: &Vec<usize>,
    ) {
        loop {
            let mut is_synced = true;
            for logidx in start..end {
                if !self.logstate[logidx]
                    .slog
                    .is_replica_synced_for_reads(self.logstate[logidx].idx, tails[logidx])
                {
                    is_synced = false;
                    if try_combine == true {
                        self.try_combine(tid, logidx);
                    }
                    spin_loop();
                }
            }

            // If all the log have reached to until scan entry; return.
            if is_synced {
                return;
            }
        }
    }

    /// This method is useful when a replica stops making progress and some threads
    /// on another replica are still active. The active replica will use all the entries
    /// in the log and won't be able perform garbage collection because of the inactive
    /// replica. So, this method syncs up the replica against the underlying log.
    pub fn sync(&self, idx: ReplicaToken) {
        let nlogs = self.logstate.len();
        for i in 0..nlogs {
            let ctail = self.logstate[i].slog.get_ctail();
            while !self.logstate[i]
                .slog
                .is_replica_synced_for_reads(self.logstate[i].idx, ctail)
            {
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
        let nthreads = self.next.load(Ordering::Relaxed) - 1;
        let hash_idx = (op.hash() % nthreads) % self.logstate.len();

        // We can perform the read only if our replica is synced up against
        // the shared log. If it isn't, then try to combine until it is synced up.
        let ctail = self.logstate[hash_idx].slog.get_ctail();
        while !self.logstate[hash_idx]
            .slog
            .is_replica_synced_for_reads(self.logstate[hash_idx].idx, ctail)
        {
            self.try_combine(tid, hash_idx);
            spin_loop();
        }

        self.data.dispatch(op)
    }

    /// Enqueues an operation inside a thread local context. Returns a boolean
    /// indicating whether the operation was enqueued (true) or not (false).
    #[inline(always)]
    fn make_pending(
        &self,
        op: <D as Dispatch>::WriteOperation,
        tid: usize,
        hash: usize,
        is_scan: bool,
    ) -> bool {
        loop {
            if self.contexts[tid - 1].enqueue(op.clone(), hash, is_scan) {
                self.logstate[hash].pending[tid - 1].store(true, Ordering::Release);
                break;
            }
        }
        true
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
            .compare_exchange_weak(0, tid, Ordering::Acquire, Ordering::Acquire)
            != Ok(0)
        {
            /* cas failed, we don't hold the lock */
            return false;
        }

        /* successfully acquired the lock */
        return true;
    }

    /// Acquire the scan lock.
    fn acquire_scan_lock(&self, tid: usize) {
        while !self.try_scan_lock(tid) {
            // Try to make progress while waiting for scan lock.
            for logidx in 0..self.logstate.len() {
                self.try_combine(tid, logidx);
                spin_loop();
            }
        }
    }

    /// Release the scan lock.
    fn release_scan_lock(&self) {
        self.scanlock.store(0, Ordering::Release);
    }

    /// Appends an operation to the log and attempts to perform flat combining.
    /// Accepts a thread `tid` as an argument. Required to acquire the combiner lock.
    fn try_combine(&self, tid: usize, hashidx: usize) {
        // First, check if there already is a flat combiner. If there is no active flat combiner
        // then try to acquire the combiner lock. If there is, then just return.
        for _i in 0..4 {
            if unsafe {
                core::ptr::read_volatile(
                    &self.logstate[hashidx].combiner
                        as *const crossbeam_utils::CachePadded<core::sync::atomic::AtomicUsize>
                        as *const usize,
                )
            } != 0
            {
                return;
            };
        }

        // Try to become the combiner here. If this fails, then simply return.
        if self.logstate[hashidx].combiner.compare_exchange_weak(
            0,
            tid,
            Ordering::Acquire,
            Ordering::Acquire,
        ) != Ok(0)
        {
            return;
        }

        // Successfully became the combiner; perform one round of flat combining.
        self.combine(tid, hashidx);

        // Allow other threads to perform flat combining once we have finished all our work.
        // At this point, we've dropped all mutable references to thread contexts and to
        // the staging buffer as well.
        self.logstate[hashidx].combiner.store(0, Ordering::Release);
    }

    /// Performs one round of flat combining. Collects, appends and executes operations.
    #[inline(always)]
    fn combine(&self, thread_id: usize, hashidx: usize) {
        //  TODO: may need to be in a per-log state context
        let mut buffer = self.logstate[hashidx].buffer.borrow_mut();
        let mut scan_buffer = self.logstate[hashidx].scan_buffer.borrow_mut();
        let mut operations = self.logstate[hashidx].inflight.borrow_mut();
        let mut results = self.logstate[hashidx].result.borrow_mut();
        let pending = &self.logstate[hashidx].pending;

        buffer.clear();
        scan_buffer.clear();
        results.clear();

        let next = self.next.load(Ordering::Relaxed);

        // Collect operations from each thread registered with this replica.
        let mut scan_tid = 0;
        for tid in 1..next {
            if pending[tid - 1].compare_exchange_weak(
                true,
                false,
                Ordering::Release,
                Ordering::Relaxed,
            ) == Ok(true)
            {
                // pass hash of current op to contexts, only get ops from context that have the same hash/log id
                operations[tid - 1] =
                    self.contexts[tid - 1].ops(&mut buffer, &mut scan_buffer, hashidx);
                if scan_buffer.len() > 0 {
                    scan_tid = tid;
                }
            }
        }

        // Append all collected operations into the shared log. We pass a closure
        // in here because operations on the log might need to be consumed for GC.
        {
            let f = |o: <D as Dispatch>::WriteOperation,
                     i: usize,
                     is_scan,
                     depends_on: Option<Arc<Vec<usize>>>| match is_scan {
                false => {
                    let resp = self.data.dispatch_mut(o);
                    if i == self.logstate[hashidx].idx {
                        results.push(resp);
                    }
                }
                true => {
                    let depends_on = depends_on.as_ref().unwrap();
                    // TODO1: Can there be an infinite loop here, combiner is waiting on the self combining?
                    // TODO2: What happens when there is/are only 2 logs in the system?
                    match depends_on.len() != self.logstate.len() {
                        true => self.sync_for_scan(thread_id, 0, 1, false, depends_on),
                        false => {
                            self.sync_for_scan(
                                thread_id,
                                1,
                                self.logstate.len(),
                                false,
                                depends_on,
                            );
                            let resp = self.data.dispatch_mut(o);
                            if i == self.logstate[hashidx].idx {
                                results.push(resp)
                            };
                        }
                    }
                }
            };
            self.logstate[hashidx]
                .slog
                .append(&buffer, self.logstate[hashidx].idx, f);

            if scan_tid != 0 {
                // Only one outstanding scan operation per log is allowed for now.
                assert_eq!(scan_buffer.len(), 1);
                self.append_scan(scan_buffer[0].clone(), thread_id);
            }
        }

        // Execute any operations on the shared log against this replica.
        {
            let mut f = |o: <D as Dispatch>::WriteOperation,
                         i: usize,
                         is_scan,
                         depends_on: Option<Arc<Vec<usize>>>| {
                match is_scan {
                    false => {
                        let resp = self.data.dispatch_mut(o);
                        if i == self.logstate[hashidx].idx {
                            results.push(resp)
                        };
                    }
                    true => {
                        let depends_on = depends_on.as_ref().unwrap();
                        // TODO1: Can there be an infinite loop here, combiner is waiting on the self combining?
                        // TODO2: What happens when there is/are only 2 logs in the system?
                        match depends_on.len() != self.logstate.len() {
                            true => self.sync_for_scan(thread_id, 0, 1, false, depends_on),
                            false => {
                                self.sync_for_scan(
                                    thread_id,
                                    1,
                                    self.logstate.len(),
                                    false,
                                    depends_on,
                                );
                                let resp = self.data.dispatch_mut(o);
                                if i == self.logstate[hashidx].idx {
                                    results.push(resp)
                                };
                            }
                        }
                    }
                }
            };
            self.logstate[hashidx]
                .slog
                .exec(self.logstate[hashidx].idx, &mut f);
        }

        // Return/Enqueue responses back into the appropriate thread context(s).
        let (mut s, mut f) = (0, 0);
        // TODO: hashing makes this non-linear, need to take into account which operations
        // belong to our current combiner round...
        for i in 1..next {
            if operations[i - 1] == 0 || i == scan_tid {
                continue;
            };

            f += operations[i - 1];
            self.contexts[i - 1].enqueue_resps(&results[s..f]);
            s += operations[i - 1];
            operations[i - 1] = 0;
        }

        // Add scan-op results at the end
        if scan_tid != 0 {
            f += operations[scan_tid - 1];
            self.contexts[scan_tid - 1].enqueue_resps(&results[s..f]);
            operations[scan_tid - 1] = 0;
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
        assert_eq!(repl.logstate[0].idx, 1);
        assert_eq!(repl.logstate[0].combiner.load(Ordering::SeqCst), 0);
        assert_eq!(repl.next.load(Ordering::SeqCst), 1);
        assert_eq!(repl.contexts.len(), MAX_THREADS_PER_REPLICA);
        assert_eq!(
            repl.logstate[0].buffer.borrow().capacity(),
            MAX_THREADS_PER_REPLICA * Context::<u64, Result<u64, ()>>::batch_size()
        );
        assert_eq!(
            repl.logstate[0].inflight.borrow().len(),
            MAX_THREADS_PER_REPLICA
        );
        assert_eq!(
            repl.logstate[0].result.borrow().capacity(),
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

        assert_eq!(repl.logstate[0].combiner.load(Ordering::SeqCst), 0);
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
        repl.logstate[0].combiner.store(8, Ordering::SeqCst);
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
            repl.logstate[i].combiner.store(i + 1, Ordering::Relaxed);
        }

        for i in 0..logs.len() {
            assert_eq!(repl.logstate[i].combiner.load(Ordering::Relaxed), i + 1);
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
            repl.logstate[i].combiner.store(i + 1, Ordering::Relaxed);
        }

        for i in 0..logs.len() {
            assert_eq!(repl.logstate[i].combiner.load(Ordering::Relaxed), i + 1);
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
                assert_eq!(r.logstate[i].combiner.load(Ordering::SeqCst), tid);
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
