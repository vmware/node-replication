// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! The Replica implementation for Node Replication.
//!
//! A replica holds one instance of a data-structure and ensures all accesses to
//! the data-structure are synchronized with respect to the order in the shared
//! [`Log`].

use alloc::vec::Vec;
use core::cell::RefCell;
use core::fmt::{self, Debug};
use core::hint::spin_loop;
#[cfg(not(loom))]
use core::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};

use super::context::Context;
use super::log::{Log, LogToken};
use super::rwlock::RwLock;
use super::Dispatch;

pub use crate::replica::ReplicaId;
pub use crate::replica::ReplicaToken;
pub use crate::replica::MAX_THREADS_PER_REPLICA;

/// Errors a replica can encounter (and return to clients) when they execute
/// operations.
///
/// Note that these errors are not fatal and are resolved as part of the
/// [`crate::nr::NodeReplicated`] logic and not passed on to clients. Therefore,
/// clients of the library don't need to worry about this if they don't
/// implement their own version of [`crate::nr::NodeReplicated`].
pub enum ReplicaError<'r, D>
where
    D: Sized + Dispatch + Sync,
{
    /// We don't have space in the log to enqueue our batch of operations.
    ///
    /// This can happen if one or more replicas (not our own) stopped making
    /// progress and have not applied the outstanding operations in the log. If
    /// they fall behind too much we will eventually run out of space since the
    /// log is implemented as a bounded, circular buffer.
    ///
    /// If this happens the [`ReplicaId`] reported in this error is one of the
    /// replicas that is behind (it can definitely happen that more than one
    /// replicas are slow and behind and need to be poked, then the [`Replica`]
    /// will just return this error multiple times, until we re-tried enough
    /// times and have adanved all the replicas which are behind). The system
    /// should "poke" replicas which are behind using [`Replica::sync`].
    ///
    /// After poking a replica, the system should resume the original operation
    /// on the current replica using the already acquired (and returned, as part
    /// of this error) [`CombinerLock`] of our local replica. A client is
    /// supposed to call [`Replica::execute_locked`] or
    /// [`Replica::execute_mut_locked`] with the combiner lock.
    NoLogSpace(ReplicaId, CombinerLock<'r, D>),

    /// After we enqueue operations in the log there is a process known as
    /// garbage-collection for old entries in the log. It tries to occasionally
    /// advance the (global) log head pointer.
    ///
    /// If we can't do this (because a replica is behind and has it's own head
    /// pointer at the same index as the global head pointer of the log), we
    /// report an error back to the client. The error contains the ID of the
    /// replica that is behind so we can go and poke it e.g., with
    /// [`Replica::sync`].
    ///
    /// If we get this error during [`Replica::execute_mut`] it means that the
    /// system did manage to execute the operations since GC happens afterwards.
    GcFailed(ReplicaId),
}

impl<D> Debug for ReplicaError<'_, D>
where
    D: Sized + Dispatch + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplicaError::NoLogSpace(rid, _cl) => {
                write!(f, "ReplicaError::NoLogSpace(rid = {})", rid)
            }
            ReplicaError::GcFailed(rid) => {
                write!(f, "ReplicaError::GcFailed(rid = {})", rid)
            }
        }
    }
}

/// An instance of a replicated data structure which uses a shared [`Log`] to
/// scale operations on the data structure across cores and processors.
///
/// Takes in one generic type argument: `D` which is the underlying sequential
/// data structure. `D` must implement the [`Dispatch`] trait.
///
/// - A thread can be registered against the replica by calling
///   [`Replica::register()`].
///
/// - A mutable operation can be issued by calling [`Replica::execute_mut()`]. A
///   mutable operation will be eventually executed against `D` by calling
///   [`Dispatch::dispatch_mut`] along with any operations that we received from
///   other replicas/threads that share the same underlying log.
///
/// - A immutable operation uses [`Replica::execute`] and eventually calls D's
///   [`Dispatch::dispatch`] method.
///
/// # When to use Replica
///
/// In most common cases, a client of this library doesn't need to interact
/// directly with a [`Replica`] object, but instead should use
/// [`crate::nr::NodeReplicated`] which encapsulates multiple replica objects
/// and a log, handles registration and ensures liveness when using multiple
/// replicas.
pub struct Replica<D>
where
    D: Sized + Dispatch + Sync,
{
    /// An identifier that we got from the Log when the replica was registered
    /// against the shared-log ([`Log::register()`]). Required to pass to the
    /// log when consuming operations from the log.
    log_tkn: LogToken,

    /// Stores the index of the thread currently doing flat combining. Field is
    /// zero if there isn't any thread actively performing flat-combining.
    /// Atomic since this acts as the combiner lock.
    combiner: CachePadded<AtomicUsize>,

    /// Thread index that will be handed out to the next thread that registers
    /// with the replica when calling [`Replica::register()`].
    next: CachePadded<AtomicUsize>,

    /// List of per-thread contexts. Threads buffer write operations here when
    /// they cannot perform flat combining (because another thread might already
    /// be doing so).
    ///
    /// The vector is initialized with [`MAX_THREADS_PER_REPLICA`] [`Context`]
    /// elements.
    contexts: Vec<Context<<D as Dispatch>::WriteOperation, <D as Dispatch>::Response>>,

    /// A buffer of operations for flat combining.
    ///
    /// The combiner stages operations in this vector and then batch appends
    /// them in the shared log. This helps amortize the cost of the
    /// `compare_and_swap` on the tail of the log.
    buffer: RefCell<Vec<<D as Dispatch>::WriteOperation>>,

    /// Number of operations collected by the combiner from each thread at any
    /// given point of time. Index `i` holds the number of operations collected
    /// from thread with [`crate::replica::ThreadIdx`] `i + 1`.
    inflight: RefCell<[usize; MAX_THREADS_PER_REPLICA]>,

    /// A buffer of results collected after flat combining. With the help of
    /// `inflight`, the combiner enqueues these results into the appropriate
    /// thread context.
    result: RefCell<Vec<<D as Dispatch>::Response>>,

    /// The underlying data structure. This is shared among all threads that are
    /// registered with this replica. Each replica maintains its own copy of
    /// `data`.
    data: CachePadded<RwLock<D>>,
}

/// The Replica is [`Sync`].
///
/// Member variables are protected by the combiner lock of the replica
/// (`combiner`). Contexts are thread-safe.
unsafe impl<D> Sync for Replica<D> where D: Sized + Sync + Dispatch {}

impl<D> core::fmt::Debug for Replica<D>
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
    /// Takes a token to the shared log as an argument. Note that the [`Log`]
    /// itself is passed as an argument to the operations that will need to
    /// modify it.
    ///
    /// The data-structure `D` will be instantiated using its [`Default`]
    /// constructor.
    ///
    /// # Example
    ///
    /// ```
    /// #![feature(generic_associated_types)]
    /// use node_replication::nr::Dispatch;
    /// use node_replication::nr::Log;
    /// use node_replication::nr::Replica;
    ///
    /// // The data structure we want replicated.
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: u64,
    /// }
    ///
    /// // This trait allows the `Data` to be used with node-replication.
    /// impl Dispatch for Data {
    ///     type ReadOperation<'rop> = ();
    ///     type WriteOperation = u64;
    ///     type Response = Option<u64>;
    ///
    ///     // A read returns the underlying u64.
    ///     fn dispatch<'rop>(
    ///         &self,
    ///         _op: Self::ReadOperation<'rop>,
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
    /// let log = Log::<<Data as Dispatch>::WriteOperation>::default();
    ///
    /// // Create a replica that uses the above log.
    /// let ltkn = log.register().unwrap();
    /// let replica = Replica::<Data>::new(ltkn);
    /// ```
    pub fn new(log_tkn: LogToken) -> Replica<D> {
        Replica::with_data(log_tkn, Default::default())
    }
}

/// The CombinerLock object indicates that we succesfully hold the combiner lock of the
/// [`Replica`].
///
/// The atomic `combiner` field is set to the [`crate::replica::ThreadIdx`] of the owner. On `drop` we have
/// to reset it to 0.
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
    /// Inidcates we're holding the CombinerLock.
    ///
    /// # Safety
    /// This should basically only ever be called in [`Replica::acquire_combiner_lock()`]
    /// if the compare exchange succeeds.
    unsafe fn new(replica: &'a Replica<D>) -> Self {
        Self { replica }
    }
}

impl<D> Drop for CombinerLock<'_, D>
where
    D: Sized + Dispatch + Sync,
{
    /// Allow other threads to perform flat combining once we have finished all
    /// our work.
    ///
    /// # TODO for better type safety
    ///
    /// Unfortunately this isn't a traditional lock that "owns" the underlying
    /// data.
    ///
    /// So we must ensure, we've dropped all mutable references to thread
    /// contexts and to the staging buffer in [`Replica`] before this is
    /// dropped. Right now if the [`Replica`] code accidentially drops this it
    /// would be a disaster.
    fn drop(&mut self) {
        self.replica.combiner.store(0, Ordering::Release);
    }
}

impl<D> Debug for CombinerLock<'_, D>
where
    D: Sized + Dispatch + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CombinerLock")
    }
}

impl<D> Replica<D>
where
    D: Sized + Dispatch + Sync,
{
    /// Similar to [`Replica::new`], but we pass an existing data-structure as
    /// an argument (`d`) rather than relying on the [`Default`] trait to create
    /// one for us.
    ///
    /// - [`Replica::new`] is the safest method to create a Replica.
    /// - If [`Replica::with_data`] is used, care must be taken that an exact
    ///   [`Copy`] of `d` is passed to every Replica object of the replicated
    ///   data-structure. If not, operations when executed on different replicas
    ///   may give different results.
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

    /// Registers a thread with this replica. Returns a [`ReplicaToken`] if the
    /// registration was successfull. None if the registration failed.
    ///
    /// The [`ReplicaToken`] is used to identify which thread issues the
    /// operation for subsequent [`Replica::execute()`] and
    /// [`Replica::execute_mut`] calls.
    ///
    /// # Example
    ///
    /// ```
    /// #![feature(generic_associated_types)]
    /// use node_replication::nr::Dispatch;
    /// use node_replication::nr::Log;
    /// use node_replication::nr::Replica;
    ///
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: u64,
    /// }
    ///
    /// impl Dispatch for Data {
    ///     type ReadOperation<'rop> = ();
    ///     type WriteOperation = u64;
    ///     type Response = Option<u64>;
    ///
    ///     fn dispatch<'rop>(
    ///         &self,
    ///         _op: Self::ReadOperation<'rop>,
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
    /// let log = Log::<<Data as Dispatch>::WriteOperation>::default();
    /// let logtkn = log.register().unwrap();
    /// let replica = Replica::<Data>::new(logtkn);
    ///
    /// // Calling register() returns a thread token that can be used to execute
    /// // operations against the replica.
    /// let thrtkn = replica.register().expect("Failed to register with replica.");
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

    /// Executes a mutable operation against this replica and returns a
    /// response.
    ///
    /// # Arguments
    /// - `slog`: Is a reference to the shared log. It is a bug to supply a log
    ///    reference that does not match the log-token supplied to the
    ///    constructor of the Replica. Ideally, this is runtime checked and
    ///    panics in the future.
    /// - `op`: The operation we want to execute.
    /// - `idx`: Is the identifier for the thread performing the execute
    ///   operation obtained from [`Replica::register`].
    ///
    /// # Returns
    /// If the operation was able to execute we return the result wrapped in a
    /// [`Result::Ok`]. If the operation could not be executed (because another
    /// [`Replica`] was lagging behind) we return a [`ReplicaError`] with more
    /// information on why we're stalled.
    ///
    /// # Example
    ///
    /// ```
    /// #![feature(generic_associated_types)]
    /// use node_replication::nr::Dispatch;
    /// use node_replication::nr::Log;
    /// use node_replication::nr::Replica;
    ///
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: u64,
    /// }
    ///
    /// impl Dispatch for Data {
    ///     type ReadOperation<'rop> = ();
    ///     type WriteOperation = u64;
    ///     type Response = Option<u64>;
    ///
    ///     fn dispatch<'rop>(
    ///         &self,
    ///         _op: Self::ReadOperation<'rop>,
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
    /// let log = Log::<<Data as Dispatch>::WriteOperation>::default();
    /// let logtkn = log.register().unwrap();
    /// let replica = Replica::<Data>::new(logtkn);
    /// let thrtkn = replica.register().expect("Failed to register with replica.");
    ///
    /// // execute_mut() can be used to write to the replicated data structure.
    /// let res = replica.execute_mut(&log, 100, thrtkn);
    /// assert_eq!(None, res.unwrap());
    /// ```
    pub fn execute_mut(
        &self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
        op: <D as Dispatch>::WriteOperation,
        idx: ReplicaToken,
    ) -> Result<<D as Dispatch>::Response, ReplicaError<D>> {
        // Enqueue the operation onto the thread local batch and then try to flat combine.
        while !self.make_pending(op.clone(), idx.tid()) {}
        self.try_combine(slog)?;

        // Return the response to the caller function.
        self.get_response(slog, idx.tid())
    }

    /// See [`Replica::execute_mut()`] for a general description of this method.
    ///
    /// # Note
    /// This method is only to be called in case we got a [`ReplicaError`] from
    /// an earlier `execute_mut` call which contained the combiner lock as part
    /// of the error.
    ///
    /// Before calling, the client should have ensured that progress was made on
    /// the replica that was reported as stuck. Study [`crate::nr::NodeReplicated`]
    /// for an example on how to use this method.
    pub fn execute_mut_locked<'lock>(
        &'lock self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
        _op: <D as Dispatch>::WriteOperation,
        idx: ReplicaToken,
        combiner_lock: CombinerLock<'lock, D>,
    ) -> Result<<D as Dispatch>::Response, ReplicaError<D>> {
        // Enqueue the operation onto the thread local batch and then try to flat combine.
        self.combine(slog, combiner_lock)?;
        self.get_response(slog, idx.tid())
    }

    /// Executes an immutable operation against this replica and returns a
    /// response.
    ///
    /// # Arguments
    /// - `slog`: Is a reference to the shared log. It is a bug to supply a log
    ///    reference that does not match the log-token supplied to the
    ///    constructor of the Replica. Ideally, this is runtime checked and
    ///    panics in the future.
    /// - `op`: The operation we want to execute.
    /// - `idx`: Is the identifier for the thread performing the execute
    ///   operation obtained from [`Replica::register`].
    ///
    /// # Returns
    /// If the operation was able to execute we return the result wrapped in a
    /// [`Result::Ok`]. If the operation could not be executed (because another
    /// [`Replica`] was lagging behind) we return a [`ReplicaError`] with more
    /// information on why we're stalled.
    ///
    /// # Example
    ///
    /// ```
    /// #![feature(generic_associated_types)]
    /// use node_replication::nr::Dispatch;
    /// use node_replication::nr::Log;
    /// use node_replication::nr::Replica;
    ///
    /// use std::sync::Arc;
    ///
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: u64,
    /// }
    ///
    /// impl Dispatch for Data {
    ///     type ReadOperation<'rop> = ();
    ///     type WriteOperation = u64;
    ///     type Response = Option<u64>;
    ///
    ///     fn dispatch<'rop>(
    ///         &self,
    ///         _op: Self::ReadOperation<'rop>,
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
    /// let logtkn = log.register().unwrap();
    /// let replica = Replica::<Data>::new(logtkn);
    /// let thrtkn = replica.register().expect("Failed to register with replica.");
    /// let _wr = replica.execute_mut(&log, 100, thrtkn);
    ///
    /// // execute() can be used to read from the replicated data structure.
    /// let res = replica.execute(&log, (), thrtkn);
    /// assert_eq!(Some(100), res.unwrap());
    /// ```
    ///
    /// # Implementation details
    /// Issues a read-only operation against the replica and returns a response.
    /// Makes sure the replica is synced up against the log before doing so.
    pub fn execute<'rop>(
        &self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
        op: <D as Dispatch>::ReadOperation<'rop>,
        idx: ReplicaToken,
    ) -> Result<<D as Dispatch>::Response, (ReplicaError<D>, <D as Dispatch>::ReadOperation<'rop>)>
    {
        // We can perform the read only if our replica is synced up against
        // the shared log. If it isn't, then try to combine until it is synced up.
        let ctail = slog.get_ctail();
        while !slog.is_replica_synced_for_reads(&self.log_tkn, ctail) {
            if let Err(e) = self.try_combine(slog) {
                return Err((e, op));
            }
            spin_loop();
        }

        return Ok(self.data.read(idx.tid() - 1).dispatch(op));
    }

    /// See [`Replica::execute()`] for a general description of this method.
    ///
    /// # Note
    /// This method is only to be called in case we got a [`ReplicaError`] from
    /// an earlier [`Replica::execute`] call which contained the combiner lock
    /// as part of the error.
    ///
    /// Before calling, the client should have ensured that progress was made on
    /// the replica that was reported as stuck. Study [`crate::nr::NodeReplicated`]
    /// for an example on how to use this method.
    pub fn execute_locked<'rop, 'lock>(
        &'lock self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
        op: <D as Dispatch>::ReadOperation<'rop>,
        idx: ReplicaToken,
        combiner_lock: CombinerLock<'lock, D>,
    ) -> Result<<D as Dispatch>::Response, (ReplicaError<D>, <D as Dispatch>::ReadOperation<'rop>)>
    {
        // We can perform the read only if our replica is synced up against
        // the shared log. If it isn't, then try to combine until it is synced up.
        let ctail = slog.get_ctail();
        if !slog.is_replica_synced_for_reads(&self.log_tkn, ctail) {
            if let Err(e) = self.combine(slog, combiner_lock) {
                return Err((e, op));
            }
        }
        // TODO(performance): If we're convinced this assert never fails
        // (because we return errors in some cases now, all of the ones that
        // make this assert fail?), we can get rid of the while below...
        assert!(slog.is_replica_synced_for_reads(&self.log_tkn, ctail));
        while !slog.is_replica_synced_for_reads(&self.log_tkn, ctail) {
            if let Err(e) = self.try_combine(slog) {
                return Err((e, op));
            }
            spin_loop();
        }

        Ok(self.data.read(idx.tid() - 1).dispatch(op))
    }

    /// Busy waits until a response is available within the thread's context.
    ///
    /// # Arguments
    /// - `slog`: The shared log.
    /// - `idx`: identifies this thread.
    pub(crate) fn get_response(
        &self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
        idx: usize,
    ) -> Result<<D as Dispatch>::Response, ReplicaError<D>> {
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

    /// Executes a passed in closure against the replica's underlying data structure.
    /// Useful for unit testing; can be used to verify certain properties of the data
    /// structure after issuing a bunch of operations against it.
    ///
    /// # Note
    /// There is no need for a regular client to ever call this function. Only use for
    /// testing.
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
        let mut f = |o: <D as Dispatch>::WriteOperation, _mine: bool| {
            data.dispatch_mut(o);
        };

        slog.exec(&self.log_tkn, &mut f);

        v(&data);

        self.combiner.store(0, Ordering::Release);
    }

    /// Synchronizes the replica by applying the outstanding operations in the
    /// log without submitting any new operation from our own thread(s).
    ///
    /// This method is useful in the following scenarion: If a replica stops
    /// making progress and some threads on another replicas are very active,
    /// the active replicas will eventually use all the available space in the
    /// log and won't be able perform garbage collection because the inactive
    /// replica needs to see the updates before they can be discarded in the
    /// log. This method can "nudge" an inactive replica to make progress.
    ///
    /// # Arguments
    ///
    /// - `slog`: The corresponding operation log. It is a bug to supply a log
    /// reference that does not match the log-token supplied to the constructor
    /// of the Replica. Ideally, this is runtime checked and panics in the
    /// future.
    pub fn sync(&self, slog: &Log<<D as Dispatch>::WriteOperation>) -> Result<(), usize> {
        let ctail = slog.get_ctail();
        while !slog.is_replica_synced_for_reads(&self.log_tkn, ctail) {
            self.try_exec(slog);
            spin_loop();
        }
        Ok(())
    }

    /// Enqueues an operation inside a thread local context. Returns a boolean
    /// indicating whether the operation was enqueued (true) or not (false).
    #[inline(always)]
    fn make_pending(&self, op: <D as Dispatch>::WriteOperation, idx: usize) -> bool {
        self.contexts[idx - 1].enqueue(op, ())
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
    ) -> Result<(), ReplicaError<D>> {
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

    #[inline(always)]
    fn try_exec(&self, slog: &Log<<D as Dispatch>::WriteOperation>) {
        // First, check if there already is a flat combiner. If there is no active flat combiner
        // then try to acquire the combiner lock. If there is, then just return.
        for _ in 0..4 {
            if self.combiner.load(Ordering::Relaxed) != 0 {
                return;
            }
        }

        // Try to become the combiner here. If this fails, then simply return.
        if let Some(_combiner_lock) = self.acquire_combiner_lock() {
            // Successfully became the combiner; perform one round of flat combining.
            self.exec(slog);
        }
    }

    #[inline(always)]
    fn exec(&self, slog: &Log<<D as Dispatch>::WriteOperation>) {
        // Execute any operations on the shared log against this replica.
        let next = self.next.load(Ordering::Relaxed);
        {
            let mut data = self.data.write(next);
            let mut f = |o: <D as Dispatch>::WriteOperation, mine: bool| {
                let _resp = data.dispatch_mut(o);
                if mine {
                    panic!("Ups -- we just lost a result?");
                }
            };
            slog.exec(&self.log_tkn, &mut f);
        }
    }

    #[inline(always)]
    fn collect_thread_ops(&self, buffer: &mut Vec<D::WriteOperation>, operations: &mut [usize]) {
        let num_registered_threads = self.next.load(Ordering::Relaxed);

        // Collect operations from each thread registered with this replica.
        for i in 1..num_registered_threads {
            let ctxt_iter = self.contexts[i - 1].iter();
            operations[i - 1] = ctxt_iter.len();
            // meta-data is (), throw it away
            buffer.extend(ctxt_iter.map(|op| op.0));
        }
    }

    /// Performs one round of flat combining. Collects, appends and executes operations.
    #[inline(always)]
    pub(crate) fn combine<'r>(
        &'r self,
        slog: &Log<<D as Dispatch>::WriteOperation>,
        combiner_lock: CombinerLock<'r, D>,
    ) -> Result<(), ReplicaError<D>> {
        let num_registered_threads = self.next.load(Ordering::Relaxed);
        let mut results = self.result.borrow_mut();
        let mut buffer = self.buffer.borrow_mut();
        let mut operations = self.inflight.borrow_mut();
        results.clear();
        buffer.clear();

        self.collect_thread_ops(&mut buffer, &mut *operations);

        // Append all collected operations into the shared log. We pass a closure
        // in here because operations on the log might need to be consumed for GC.
        let res = {
            let mut data = self.data.write(num_registered_threads);
            let f = |o: <D as Dispatch>::WriteOperation, mine: bool| {
                #[cfg(not(loom))]
                let resp = data.dispatch_mut(o);
                #[cfg(loom)]
                let resp = data.dispatch_mut(o);
                if mine {
                    results.push(resp);
                }
            };
            match slog.append(&buffer, &self.log_tkn, f) {
                Ok(None) => Ok(()),
                Ok(Some(r)) => {
                    // We inserted the entries (and can apply them below), but
                    // we want to also notify about the slow `r` so it can be
                    // forced to make some progress
                    Err(ReplicaError::GcFailed(r))
                }
                Err(r) => {
                    // return here because we couldn't insert our entries and
                    // need to try again later
                    return Err(ReplicaError::NoLogSpace(r, combiner_lock));
                }
            }
        };

        // Execute outstanding operations on the shared log against this replica
        {
            let mut data = self.data.write(num_registered_threads);
            let mut f = |o: <D as Dispatch>::WriteOperation, mine: bool| {
                let resp = data.dispatch_mut(o);
                if mine {
                    results.push(resp)
                }
            };
            slog.exec(&self.log_tkn, &mut f);
        }

        // Return/Enqueue responses back into the appropriate thread context(s).
        let (mut s, mut f) = (0, 0);
        for i in 1..num_registered_threads {
            if operations[i - 1] == 0 {
                continue;
            };

            f += operations[i - 1];
            self.contexts[i - 1].enqueue_resps(&results[s..f]);
            s += operations[i - 1];
            operations[i - 1] = 0;
        }

        res
    }
}

#[cfg(test)]
pub(crate) mod test {
    extern crate std;

    use super::*;
    use std::vec;

    // Really dumb data structure to test against the Replica and shared log.
    #[derive(Default)]
    pub(crate) struct Data {
        pub(crate) junk: u64,
    }

    impl Dispatch for Data {
        type ReadOperation<'rop> = u64;
        type WriteOperation = u64;
        type Response = Result<u64, ()>;

        fn dispatch<'rop>(&self, _op: Self::ReadOperation<'rop>) -> Self::Response {
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
        let slog = Log::<<Data as Dispatch>::WriteOperation>::new_with_bytes(1024, ());
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);
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
        let slog = Log::<<Data as Dispatch>::WriteOperation>::new_with_bytes(1024, ());
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
        let slog = Log::<<Data as Dispatch>::WriteOperation>::new_with_bytes(1024, ());
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);
        repl.next
            .store(MAX_THREADS_PER_REPLICA + 1, Ordering::SeqCst);
        assert!(repl.register().is_none());
    }

    // Tests that we can successfully allow operations to go pending on this replica.
    #[test]
    fn test_replica_make_pending() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::new_with_bytes(1024, ());
        let lt = slog.register().unwrap();
        let repl = Replica::<Data>::new(lt);
        let mut o = vec![];
        assert!(repl.make_pending(121, 8));
        let ctxt_iter = repl.contexts[7].iter();
        assert_eq!(ctxt_iter.len(), 1);
        o.extend(ctxt_iter.map(|o| o.0));
        assert_eq!(o.len(), 1);
        assert_eq!(o[0], 121);
    }

    // Tests that we can't pend operations on a context that is already full of operations.
    #[test]
    fn test_replica_make_pending_false() {
        let slog = Log::<<Data as Dispatch>::WriteOperation>::new_with_bytes(1024, ());
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
        let slog = Log::<<Data as Dispatch>::WriteOperation>::new_with_bytes(1024, ());
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

        let lt = slog.register().unwrap();
        // Add in operations to the log off the side, not through the replica.
        let o = [121, 212];
        assert!(slog.append(&o, &lt, |_o, _mine| {}).is_ok());
        slog.exec(&lt, &mut |_o, _mine| {});

        let t1 = repl.register().expect("Failed to register with replica.");
        assert_eq!(Ok(2), repl.execute(&slog, 11, t1).unwrap());
    }
}
