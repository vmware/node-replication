// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Node-Replication (NR) creates linearizable NUMA-aware concurrent data
//! structures from black-box sequential data structures.
//!
//! NR replicates the sequential data structure on each NUMA node and uses an
//! operation log to maintain consistency between the replicas. Each replica
//! benefits from read concurrency using a readers-writer lock and from write
//! concurrency using a technique called flat combining. In a nutshell, flat
//! combining batches operations from multiple threads to be executed by a
//! single thread (the combiner). This thread also appends the batched
//! operations to the log; other replicas read the log to update their internal
//! states with the new operations.
//!
//! # How does it work
//! To replicate a single-threaded data structure, one needs to implement the
//! [`Dispatch`] trait for it. The following snippet implements [`Dispatch`] for
//! `HashMap` as an example. The full example (using [`NodeReplicated`] and
//! [`Dispatch`] can be found in the
//! [examples](https://github.com/vmware/node-replication/tree/master/nr/examples/hashmap.rs)
//! folder.
//!
//! ```
//! #![feature(generic_associated_types)]
//! use node_replication::nr::Dispatch;
//! use std::collections::HashMap;
//!
//! /// The node-replicated hashmap uses a std hashmap internally.
//! pub struct NrHashMap {
//!    storage: HashMap<u64, u64>,
//! }
//!
//! /// We support a mutable put operation on the hashmap.
//! #[derive(Debug, PartialEq, Clone)]
//! pub enum Modify {
//!    Put(u64, u64),
//! }
//!
//! /// We support an immutable read operation to lookup a key from the hashmap.
//! #[derive(Debug, PartialEq, Clone)]
//! pub enum Access {
//!    Get(u64),
//! }
//!
//! /// The Dispatch traits executes `ReadOperation` (our Access enum)
//! /// and `WriteOperation` (our Modify enum) against the replicated
//! /// data-structure.
//! impl Dispatch for NrHashMap {
//!    type ReadOperation<'rop> = Access;
//!    type WriteOperation = Modify;
//!    type Response = Option<u64>;
//!
//!    /// The `dispatch` function applies the immutable operations.
//!    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
//!        match op {
//!            Access::Get(key) => self.storage.get(&key).map(|v| *v),
//!        }
//!    }
//!
//!    /// The `dispatch_mut` function applies the mutable operations.
//!    fn dispatch_mut(
//!        &mut self,
//!        op: Self::WriteOperation,
//!    ) -> Self::Response {
//!        match op {
//!            Modify::Put(key, value) => self.storage.insert(key, value),
//!        }
//!    }
//! }
//! ```

extern crate std;
use std::collections::HashMap;

use alloc::boxed::Box;
use core::fmt::Debug;
use core::marker::Sync;
use core::num::NonZeroUsize;

#[cfg(feature = "async")]
use reusable_box::ReusableBoxFuture;

use arrayvec::ArrayVec;

use core::sync::atomic::Ordering;

mod context;
pub mod log;
pub mod replica;
#[cfg(feature = "async")]
pub mod reusable_box;

#[cfg(not(loom))]
#[path = "rwlock.rs"]
pub mod rwlock;
#[cfg(loom)]
#[path = "loom_rwlock.rs"]
pub mod rwlock;

pub use log::{Log, MAX_REPLICAS_PER_LOG};
pub use replica::{CombinerLock, Replica, ReplicaError, ReplicaId, ReplicaToken};

/// Trait that a (single-threaded) data structure must implement to be usable
/// with NR.
///
/// When NR executes a read-only operation against the data structure, it
/// invokes the [`Dispatch::dispatch`] method with the `ReadOperation` as an
/// argument.
///
/// When NR executes a write operation against the data structure, it invokes
/// the [`Dispatch::dispatch_mut`] method with the `WriteOperation` as an
/// argument.
pub trait Dispatch {
    /// A read-only operation. When executed against the data structure, an
    /// operation of this type must not mutate the data structure in any way.
    /// Otherwise, the assumptions made by NR no longer hold.
    ///
    /// # For feature `async`
    /// - [`Send`] is currently needed for async operations
    #[cfg(not(feature = "async"))]
    type ReadOperation<'a>: Sized;
    #[cfg(feature = "async")]
    type ReadOperation<'a>: Sized + Send;

    /// A write operation. When executed against the data structure, an
    /// operation of this type is allowed to mutate state. The library ensures
    /// that this is done so in a thread-safe manner.
    type WriteOperation: Sized + Clone + PartialEq + Send;

    /// The type on the value returned by the data structure when a
    /// `ReadOperation` or a `WriteOperation` successfully executes against it.
    type Response: Sized + Clone;

    /// Method on the data structure that allows a read-only operation to be
    /// executed against it.
    fn dispatch(&self, op: Self::ReadOperation<'_>) -> Self::Response;

    /// Method on the data structure that allows a write operation to be
    /// executed against it.
    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response;
}

/// A token handed out to threads registered with replicas.
///
/// # Implementation detail for potential future API
/// For maximum type-safety this would be an affine type, then we'd have to
/// return it again in `execute` and `execute_mut`. However it feels like this
/// would hurt API ergonomics a lot.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ThreadToken {
    /// The replica this thread is registered with (reading from).
    ///
    /// # Note
    /// Usually this would represent e.g., the NUMA node of the thread.
    rid: ReplicaId,
    /// The registration token for this thread that we got from the replica
    /// (through [`Replica::register`]) identified by `rid`.
    rtkn: ReplicaToken,
}

impl ThreadToken {
    /// Creates a new ThreadToken
    ///
    /// # Safety
    /// This method should only ever be used for the benchmark harness to create
    /// additional, fake replica implementations. If we had something like `pub(test)` we
    /// should declare it like that instead of just `pub`.
    #[doc(hidden)]
    pub fn new(rid: ReplicaId, rtkn: ReplicaToken) -> Self {
        Self { rid, rtkn }
    }
}

/// To make it harder to use the same ThreadToken on multiple threads.
#[cfg(not(feature = "async"))]
impl !Send for ThreadToken {}

/// Argument that is passed to a user specified function (in
/// [`NodeReplicated::new`]) to indicate that our thread will change the replica
/// it's operating on.
///
/// This means we change and operate on a replica that is *not* the replica
/// which our thread originally registered with (e.g., not the one identified by
/// `rid` in [`ThreadToken`]).
///
/// This can happen if a replica is behind and the current thread decides to
/// make progress on that (remote) replica.
///
/// Getting these notifications is useful for example to maintain correct NUMA
/// affinity: If each replica is on a different NUMA node, then we need to tell
/// the OS that the current thread should allocate memory from a different NUMA
/// node temporarily.
///
/// # Workflow
///
/// The pattern for the enum arguments that are passed to the affinity function
/// always comes in pairs of `Replica` followed by `Revert`:
///
/// 1. `old = affinty_function(AffinityChange::Replica(some_non_local_rid))`.
/// 2. library remembers `old` and does some stuff with different affinity...
/// 3. `affinity_change_function(AffinityChange::Revert(old))`.
/// 4. library continues to do work with original affinity of thread.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AffinityChange {
    /// Indicates that the system will execute operation on behalf of a
    /// different replica. The user-specified function likely should either
    /// migrate the thread this is called on to a NUMA node that's local to the
    /// replica or otherwise change the memory affinity.
    Replica(ReplicaId),
    /// Indicates that we're done and the thread should revert back to it's
    /// previous/original affinity. The `usize` that was returned by the
    /// user-provided affinity change function (when we called it with
    /// [`AffinityChange::Replica`] as an argument) is passed back as the
    /// argument of [`AffinityChange::Revert`].
    ///
    /// For example, this can be useful to "remember" the original core id where
    /// the thread was running on in case of an affinity change by thread
    /// migration.
    Revert(usize),
}

/// User provided function to inform that the system should change the memory
/// allocation affinity for a current thread, to wherever the memory from the
/// replica (passed as an argument) should come from.
///
/// See also [`AffinityChange`] and [`AffinityToken`].
type AffinityChangeFn = dyn Fn(AffinityChange) -> usize + Send + Sync;

/// The [`AffinityManager`] creates affinity tokens whenever we request to
/// change the memory allocation affinity for a given thread.
///
/// The tokens take care of calling the `af_change_fn` that's usually provided
/// by a user.
struct AffinityManager {
    af_change_fn: Box<AffinityChangeFn>,
}

impl AffinityManager {
    /// Creates a new instance of the AffinityManager.
    ///
    /// # Arguments
    /// - `af_change_fn`: User provided function, or can be some default for
    /// e.g., Linux that relies on migrating threads a NUMA aware mallocs and
    /// the first-touch policy.
    fn new(af_change_fn: Box<AffinityChangeFn>) -> Self {
        Self { af_change_fn }
    }

    /// Creates an [`AffinityToken`] for the given `rid`.
    ///
    /// The token will call the user-provided function to change the memory
    /// affinity and once it gets dropped, it will tell the user to revert the
    /// change.
    fn switch(&self, rid: ReplicaId) -> AffinityToken<'_> {
        AffinityToken::new(&self.af_change_fn, rid)
    }
}

/// A token that is in charge of orchestrating memory affinity changes for a
/// thread.
struct AffinityToken<'f> {
    af_chg_fn: &'f dyn Fn(AffinityChange) -> usize,
    old: usize,
}

impl<'f> AffinityToken<'f> {
    /// Creating the token will request the memory affinity to be changes to to
    /// match the memory affinity of `rid`.
    fn new(af_chg_fn: &'f dyn Fn(AffinityChange) -> usize, rid: ReplicaId) -> Self {
        let old = af_chg_fn(AffinityChange::Replica(rid));
        Self { af_chg_fn, old }
    }
}

impl<'f> Drop for AffinityToken<'f> {
    /// Dropping the token will request to revert the affinity change that was
    /// made during creation.
    fn drop(&mut self) {
        (self.af_chg_fn)(AffinityChange::Revert(self.old));
    }
}

/// Errors that can be encountered by interacting with [`NodeReplicated`].
#[derive(Debug, PartialEq, Eq)]
pub enum NodeReplicatedError {
    /// Not enough memory to create a [`NodeReplicated`] instance.
    OutOfMemory,
    DuplicateReplica,
    UnableToRemoveReplica,
}

impl From<core::alloc::AllocError> for NodeReplicatedError {
    fn from(_: core::alloc::AllocError) -> Self {
        NodeReplicatedError::OutOfMemory
    }
}

impl From<alloc::collections::TryReserveError> for NodeReplicatedError {
    fn from(_: alloc::collections::TryReserveError) -> Self {
        NodeReplicatedError::OutOfMemory
    }
}

/// The "main" type of NR which users interact with.
///
/// It is used to wrap a single threaded data-structure that implements
/// [`Dispatch`]. It will create a configurable number of [`Replica`] and
/// allocate a [`Log`] to synchronize replicas. It also hands out
/// [`ThreadToken`] for each thread that wants to interact with the
/// [`NodeReplicated`] instance. Finally, it routes threads to the correct
/// replica and handles liveness of replicas by making sure to advance replicas
/// which are behind automatically.
pub struct NodeReplicated<D: Dispatch + Sync + Clone> {
    log: Log<D::WriteOperation>,
    replicas: HashMap<usize, Replica<D>>,
    // thread_routing: HashMap<ThreadIdx, ReplicaId>,
    affinity_mngr: AffinityManager,
}

impl<D> NodeReplicated<D>
where
    D: Default + Dispatch + Sized + Sync + Clone,
{
    /// Creates a new, replicated data-structure from a single-threaded
    /// data-structure that implements [`Dispatch`]. It uses the [`Default`]
    /// constructor to create a initial data-structure for `D` on all replicas.
    ///
    /// # Arguments
    /// - `num_replicas`: How many replicas you want to create. Typically the
    ///   number of NUMA nodes in your system.
    /// - `chg_mem_affinity`: A user-provided function that is called whenever
    ///   the code operates on a certain [`Replica`] that is not local to the
    ///   thread that we're running on (can happen if a replica falls behind and
    ///   we're temporarily executing operation on behalf of this replica so we
    ///   can make progress on our own replica). This function can be used to
    ///   ensure that memory will still be allocated from the right NUMA node.
    ///   See [`AffinityChange`] for more information on how implement this
    ///   function and handle its arguments.
    ///
    /// # Example
    ///
    /// Test ignored for lack of access to `MACHINE_TOPOLOGY` (see benchmark code
    /// for an example).
    ///
    /// ```ignore
    /// /// A function to change affinity to a given NUMA node on Linux
    /// /// (it works by migrating the current thread to the cores of the given NUMA node)
    /// fn linux_chg_affinity(af: AffinityChange) -> usize {
    ///     match af {
    ///         // System requests to change affinity to replica with `rid`
    ///         AffinityChange::Replica(rid) => {
    ///             // figure out where we're currently running:
    ///             let mut cpu: usize = 0;
    ///             let mut node: usize = 0;
    ///             unsafe { nix::libc::syscall(nix::libc::SYS_getcpu, &mut cpu, &mut node, 0) };
    ///
    ///             // figure out all cores we can potentially on run for
    ///             // correct affinity with new rid:
    ///             let mut cpu_set = nix::sched::CpuSet::new();
    ///             for ncpu in MACHINE_TOPOLOGY.cpus_on_node(rid as u64) {
    ///                 cpu_set.set(ncpu.cpu as usize);
    ///             }
    ///             // pin current thread to these cores
    ///             nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &cpu_set);
    ///             // return the cpu id where we were originally running on
    ///             cpu as usize
    ///         }
    ///         // System requests to revert affinity to original replica (`old`)
    ///         AffinityChange::Revert(core_id) => {
    ///             // `core_id` is the cpu number we returned above
    ///             let mut cpu_set = nix::sched::CpuSet::new();
    ///             cpu_set.set(core_id);
    ///             // Migrate thread back to old core_id
    ///             nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &cpu_set);
    ///             0x0 // return value is ignored for `Revert`
    ///         }
    ///     }
    /// }
    ///
    /// let replicas = NonZeroUsize::new(2).unwrap();
    /// let nrht = NodeReplicated::<NrHashMap>::new(replicas, linux_chg_affinity).unwrap();
    /// ```
    pub fn new(
        num_replicas: NonZeroUsize,
        chg_mem_affinity: impl Fn(AffinityChange) -> usize + Send + Sync + 'static,
    ) -> Result<Self, NodeReplicatedError> {
        Self::with_log_size(num_replicas, chg_mem_affinity, log::DEFAULT_LOG_BYTES)
    }

    /// Same as [`NodeReplicated::new`], but in addition use a non-default size
    /// (provided in bytes) for the [`Log`].
    pub fn with_log_size(
        num_replicas: NonZeroUsize,
        chg_mem_affinity: impl Fn(AffinityChange) -> usize + Send + Sync + 'static,
        log_size: usize,
    ) -> Result<Self, NodeReplicatedError> {
        assert!(num_replicas.get() < MAX_REPLICAS_PER_LOG);
        let affinity_mngr = AffinityManager::new(Box::try_new(chg_mem_affinity)?);
        let log = Log::new_with_bytes(log_size, ());

        let mut replicas = HashMap::with_capacity(MAX_REPLICAS_PER_LOG);

        for replica_id in 0..num_replicas.get() {
            let log_token = log
                .register()
                .expect("Succeeds (num_replicas < MAX_REPLICAS_PER_LOG)");

            let r = {
                // Allocate the replica on the proper NUMA node
                let _aff_tkn = affinity_mngr.switch(replica_id);
                Replica::new(log_token)
                // aff_tkn is dropped here
            };

            replicas.insert(replica_id, r);
        }

        Ok(NodeReplicated {
            log,
            replicas,
            affinity_mngr,
        })
    }

    /// Adds a new replica to the NodeReplicated. It returns the index of the added replica within
    /// NodeReplicated.replicas[x].
    ///
    /// # Example
    ///
    /// Test ignored for lack of access to `MACHINE_TOPOLOGY` (see benchmark code
    /// for an example).
    ///
    /// ```ignore
    /// let replicas = NonZeroUsize::new(1).unwrap();
    /// let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
    /// let ttkn_a = async_ds.register(0).expect("Unable to register with log");
    /// let _ = async_ds.execute_mut(1, ttkn_a);
    /// let _ = async_ds.execute_mut(2, ttkn_a);
    /// let added_replica = async_ds.add_replica().unwrap();
    /// let added_replica_data = async_ds.replicas[added_replica].data.read(0).junk;
    /// assert_eq!(2, added_replica_data);
    /// ```
    pub fn add_replica(&mut self) -> Result<ThreadToken, NodeReplicatedError> {
        let log_token = self
            .log
            .register()
            .expect("Succeeds (num_replicas < MAX_REPLICAS_PER_LOG)");

        let r = {
            // Allocate the replica on the proper NUMA node
            let _aff_tkn = self.affinity_mngr.switch(self.replicas.len());
            Replica::new(log_token.clone())
            // aff_tkn is dropped here
        };

        let replica_id = self.new_replica_id();

        if self.replicas.contains_key(&replica_id) {
            return core::prelude::v1::Err(NodeReplicatedError::DuplicateReplica);
        }

        self.replicas.insert(replica_id, r);

        // get the most up to date replica
        let (max_replica_idx, max_local_tail) = self.log.find_max_tail();

        // copy data from existing replica
        let replica_locked = self.replicas[&max_replica_idx]
            .data
            .read(log_token.0)
            .clone();
        let new_replica_data = &mut self.replicas[&replica_id].data.write(log_token.0);
        **new_replica_data = replica_locked;

        // push ltail entry for new replica
        self.log.ltails[&replica_id].store(max_local_tail, Ordering::Relaxed);

        // find and push existing lmask entry for new replica
        let lmask_status = self.log.lmasks[&max_replica_idx].get();
        self.log.lmasks[&replica_id].set(lmask_status);

        // Register the the replica with a thread_id and return the ThreadToken
        let registered_replica = self.register(replica_id);

        match registered_replica {
            Some(thread_token) => Ok(thread_token),
            None => Err(NodeReplicatedError::DuplicateReplica),
        }
    }

    pub fn remove_replica(
        &mut self,
        replica_id: ReplicaId,
    ) -> Result<ReplicaId, NodeReplicatedError> {
        self.log.remove_log_replica(log::LogToken(replica_id));

        if self.replicas.contains_key(&replica_id) {
            self.replicas.remove(&replica_id);
        } else {
            return Err(NodeReplicatedError::UnableToRemoveReplica);
        }

        Ok(replica_id)
    }

    fn new_replica_id(&self) -> usize {
        // identify max replica id
        let max_key = self.replicas.iter().max_by_key(|&(k, _v)| k);
        if let Some(key) = max_key {
            *key.0 + 1
        } else {
            0
        }
    }
}

impl<D> NodeReplicated<D>
where
    D: Clone + Dispatch + Sized + Sync,
{
    /// Same as [`NodeReplicated::new`], but provide the initial data-structure
    /// `ds` (which may not have a [`Default`] constructor).
    ///
    /// `ds` will be cloned for each replica.
    pub fn with_data(
        _num_replicas: NonZeroUsize,
        _chg_mem_affinity: impl Fn(AffinityChange) -> usize + Send + Sync + 'static,
        _ds: D,
    ) -> Result<Self, NodeReplicatedError> {
        unimplemented!("complete me")
    }
}

impl<D> NodeReplicated<D>
where
    D: Dispatch + Sized + Sync + Clone,
{
    /// Registers a thread with a given replica in the [`NodeReplicated`]
    /// data-structure. Returns an Option containing a [`ThreadToken`] if the
    /// registration was successful. None if the registration failed.
    ///
    /// The [`ThreadToken`] is used to identify the thread to issue the
    /// operation for subsequent [`NodeReplicated::execute`] and
    /// [`NodeReplicated::execute_mut`] calls.
    ///
    /// # Arguments
    ///
    /// - `replica_id`: Which replica the thread should be registered with.
    ///
    /// # Example
    ///
    /// ```
    /// #![feature(generic_associated_types)]
    /// use core::num::NonZeroUsize;
    /// use node_replication::nr::NodeReplicated;
    /// use node_replication::nr::Dispatch;
    ///
    /// #[derive(Default, Clone)]
    /// struct Void;
    /// impl Dispatch for Void {
    ///     type ReadOperation<'rop> = ();
    ///     type WriteOperation = ();
    ///     type Response = ();
    ///
    ///     fn dispatch<'rop>(&self, op: <Self as Dispatch>::ReadOperation<'rop>) -> <Self as Dispatch>::Response {}
    ///     fn dispatch_mut(&mut self, op: <Self as Dispatch>::WriteOperation) -> <Self as Dispatch>::Response {}
    /// }
    ///
    /// let replicas = NonZeroUsize::new(2).unwrap();
    /// let nrht = NodeReplicated::<Void>::new(replicas, |_| { 0 }).unwrap();
    /// assert!(nrht.register(0).is_some());
    /// ```
    pub fn register(&self, replica_id: ReplicaId) -> Option<ThreadToken> {
        if self.replicas.len() < MAX_REPLICAS_PER_LOG {
            let rtkn = self.replicas[&replica_id].register()?;
            Some(ThreadToken::new(replica_id, rtkn))
        } else {
            None
        }
    }

    fn try_execute_mut<'a>(
        &'a self,
        op: <D as Dispatch>::WriteOperation,
        tkn: ThreadToken,
        cl: Option<CombinerLock<'a, D>>,
    ) -> Result<<D as Dispatch>::Response, ReplicaError<D>> {
        if let Some(combiner_lock) = cl {
            // We expect to have already enqueued the op (it's a re-try since have the combiner lock),
            // so technically its not needed to supply it again (but we currently do it anyways...)
            self.replicas[&tkn.rid].execute_mut_locked(&self.log, op, tkn.rtkn, combiner_lock)
        } else {
            self.replicas[&tkn.rid].execute_mut(&self.log, op, tkn.rtkn)
        }
    }

    /// Executes a mutable operation against the data-structure.
    ///
    /// Thanks to the [`Log`] that [`NodeReplicated`] uses, all replicas will
    /// execute all mutable operations in the same order.
    ///
    ///  This method is similar to the one found in [`Replica::execute_mut`],
    /// but in addition, it handles liveness issues due to lagging replicas
    /// (which a single replica can not).
    ///
    /// # Arguments
    /// - `op`: Which operation to execute.
    /// - `tkn`: Which thread executes the operation (see also
    ///   [`NodeReplicated::register`]).
    ///
    /// # Flow
    /// Eventually, this method calls [`Replica::execute_mut`] which will call
    /// into [`Dispatch::dispatch_mut`].
    ///
    /// # Example
    /// ```
    /// #![feature(generic_associated_types)]
    /// use core::num::NonZeroUsize;
    /// use node_replication::nr::NodeReplicated;
    /// use node_replication::nr::Dispatch;
    ///
    /// #[derive(Default,Clone)]
    /// struct Void;
    /// impl Dispatch for Void {
    ///     type ReadOperation<'rop> = ();
    ///     type WriteOperation = usize;
    ///     type Response = usize;
    ///
    ///     fn dispatch<'rop>(&self, op: <Self as Dispatch>::ReadOperation<'rop>) -> <Self as Dispatch>::Response {
    ///         unreachable!("no read-op is issued")
    ///     }
    ///     fn dispatch_mut(&mut self, op: <Self as Dispatch>::WriteOperation) -> <Self as Dispatch>::Response {
    ///         assert!(op == 99);
    ///         // "eventually", because if we just do one `execute_mut` call
    ///         // we won't immediately advance the 2nd replica
    ///         println!("Having two replicas means I'm (eventually) called twice");
    ///         0xbeef
    ///     }
    /// }
    ///
    /// let replicas = NonZeroUsize::new(2).unwrap();
    /// let nrht = NodeReplicated::<Void>::new(replicas, |_| { 0 }).unwrap();
    /// let ttkn = nrht.register(0).unwrap();
    ///
    /// assert_eq!(nrht.execute_mut(99, ttkn), 0xbeef);
    /// ```
    pub fn execute_mut(
        &self,
        op: <D as Dispatch>::WriteOperation,
        tkn: ThreadToken,
    ) -> <D as Dispatch>::Response {
        /// An enum to keep track of a stack of operations we should do on Replicas.
        ///
        /// e.g., either `Sync` an out-of-date, behind replica, or call `execute_locked` or
        /// `execute_mut_locked` to resume the operation with a combiner lock.
        enum ResolveOp<'a, D: core::marker::Sync + Dispatch + Sized + Clone> {
            /// Resumes a replica that earlier returned with an Error (and the CombinerLock).
            Exec(Option<CombinerLock<'a, D>>),
            /// Indicates need to [`Replica::sync()`] a replica with the given ID.
            Sync(ReplicaId),
        }

        let mut q = ArrayVec::<ResolveOp<D>, { crate::log::MAX_REPLICAS_PER_LOG }>::new();
        loop {
            match q.pop().unwrap_or(ResolveOp::Exec(None)) {
                ResolveOp::Exec(cl) => match self.try_execute_mut(op.clone(), tkn, cl) {
                    Ok(resp) => {
                        assert!(q.is_empty());
                        return resp;
                    }
                    Err(ReplicaError::NoLogSpace(stuck_ridx, cl_acq)) => {
                        assert_ne!(stuck_ridx, tkn.rid);
                        q.push(ResolveOp::Exec(Some(cl_acq)));
                        q.push(ResolveOp::Sync(stuck_ridx));
                    }
                    Err(ReplicaError::GcFailed(stuck_ridx)) => {
                        {
                            assert_ne!(stuck_ridx, tkn.rid);
                            let _aftkn = self.affinity_mngr.switch(stuck_ridx);
                            self.replicas[&stuck_ridx].sync(&self.log);
                            // Affinity is reverted here, _aftkn is dropped.
                        }
                        return self.replicas[&tkn.rid]
                            .get_response(&self.log, tkn.rtkn.tid())
                            .expect("GcFailed has to produced a response");
                    }
                },
                ResolveOp::Sync(ridx) => {
                    // Holds trivially because of all the other asserts in this function
                    debug_assert_ne!(ridx, tkn.rid);
                    //warn!("execute_mut ResolveOp::Sync {}", ridx);
                    let _aftkn = self.affinity_mngr.switch(ridx);
                    self.replicas[&ridx].try_sync(&self.log);
                    // _aftkn is dropped here, reverting affinity change
                }
            }
        }
    }

    fn try_execute<'a, 'rop>(
        &'a self,
        op: <D as Dispatch>::ReadOperation<'rop>,
        tkn: ThreadToken,
        cl: Option<CombinerLock<'a, D>>,
    ) -> Result<<D as Dispatch>::Response, (ReplicaError<D>, <D as Dispatch>::ReadOperation<'rop>)>
    {
        if let Some(combiner_lock) = cl {
            self.replicas[&tkn.rid].execute_locked(&self.log, op, tkn.rtkn, combiner_lock)
        } else {
            self.replicas[&tkn.rid].execute(&self.log, op, tkn.rtkn)
        }
    }

    /// Executes a immutable operation against the data-structure.
    ///
    /// Multiple threads can read from the data-structure in parallel.
    ///
    ///  This method is similar to the one found in [`Replica::execute`], but in
    /// addition, it handles liveness issues due to lagging replicas (which a
    /// single replica can not).
    ///
    /// # Arguments
    /// - `op`: Which operation to execute.
    /// - `tkn`: Which thread executes the operation (see also
    ///   [`NodeReplicated::register`]).
    ///
    /// # Flow
    /// Eventually, this method calls [`Replica::execute`] which will call into
    /// [`Dispatch::dispatch`].
    ///
    /// # Example
    /// ```
    /// #![feature(generic_associated_types)]
    /// use core::num::NonZeroUsize;
    /// use node_replication::nr::NodeReplicated;
    /// use node_replication::nr::Dispatch;
    ///
    /// #[derive(Default, Clone)]
    /// struct Void;
    /// impl Dispatch for Void {
    ///     type ReadOperation<'rop> = usize;
    ///     type WriteOperation = ();
    ///     type Response = usize;
    ///
    ///     fn dispatch<'rop>(&self, op: <Self as Dispatch>::ReadOperation<'rop>) -> <Self as Dispatch>::Response {
    ///         assert!(op == 99);
    ///         0xbeef
    ///     }
    ///     fn dispatch_mut(&mut self, op: <Self as Dispatch>::WriteOperation) -> <Self as Dispatch>::Response {
    ///         unreachable!("no immutable op is issued")
    ///     }
    /// }
    ///
    /// let replicas = NonZeroUsize::new(2).unwrap();
    /// let nrht = NodeReplicated::<Void>::new(replicas, |_| { 0 }).unwrap();
    /// let ttkn = nrht.register(0).unwrap();
    ///
    /// assert_eq!(nrht.execute(99, ttkn), 0xbeef);
    /// ```
    pub fn execute(
        &self,
        op: <D as Dispatch>::ReadOperation<'_>,
        tkn: ThreadToken,
    ) -> <D as Dispatch>::Response {
        /// An enum to keep track of a stack of operations we should do on Replicas.
        ///
        /// e.g., either `Sync` an out-of-date, behind replica, or call `execute_locked` or
        /// `execute_mut_locked` to resume the operation with a combiner lock.
        enum ResolveOp<'a, 'rop, D: core::marker::Sync + Dispatch + Sized + Clone> {
            /// Resumes a replica that earlier returned with an Error (and the CombinerLock).
            Exec(Option<CombinerLock<'a, D>>, D::ReadOperation<'rop>),
            /// Indicates need to [`Replica::sync()`] a replica with the given ID.
            Sync(ReplicaId),
        }

        let mut q = ArrayVec::<ResolveOp<D>, { crate::log::MAX_REPLICAS_PER_LOG }>::new();
        q.push(ResolveOp::Exec(None, op));
        loop {
            match q.pop().unwrap() {
                ResolveOp::Exec(cl, op) => match self.try_execute(op, tkn, cl) {
                    Ok(resp) => {
                        assert!(q.is_empty());
                        return resp;
                    }
                    Err((ReplicaError::NoLogSpace(stuck_ridx, cl_acq), op)) => {
                        assert!(stuck_ridx != tkn.rid);
                        q.push(ResolveOp::Exec(Some(cl_acq), op));
                        q.push(ResolveOp::Sync(stuck_ridx));
                    }
                    Err((ReplicaError::GcFailed(stuck_ridx), op)) => {
                        assert_ne!(stuck_ridx, tkn.rid);
                        q.push(ResolveOp::Exec(None, op));
                        q.push(ResolveOp::Sync(stuck_ridx));
                    }
                },
                ResolveOp::Sync(ridx) => {
                    // Holds trivially because of all the other asserts in this function
                    debug_assert_ne!(ridx, tkn.rid);
                    let _aftkn = self.affinity_mngr.switch(ridx);
                    self.replicas[&ridx].try_sync(&self.log);
                    // _aftkn is dropped here, reverting affinity change
                }
            }
        }
    }

    /// Executes a mutable operation asynchronously on a replica, and returns
    /// the response in `resp`
    ///
    /// # Note
    /// Currently a trivial "async" implementation as we just call the blocking
    /// call [`NodeReplicated::execute`] and wrap it in `async`.
    #[cfg(feature = "async")]
    pub async fn async_execute_mut<'a>(
        &'a self,
        op: <D as Dispatch>::WriteOperation,
        tkn: ThreadToken,
        resp: &mut ReusableBoxFuture<'a, <D as Dispatch>::Response>,
    ) {
        resp.set(async move { self.execute_mut(op, tkn) });
    }

    /// Executes an immutable operation asynchronously on a replica, and returns
    /// the response in `resp`.
    ///
    /// # Note
    /// Currently a trivial "async" implementation as we just call the blocking
    /// call [`NodeReplicated::execute`] and wrap it in `async`.
    #[cfg(feature = "async")]
    pub fn async_execute<'a, 'rop: 'a>(
        &'a self,
        op: <D as Dispatch>::ReadOperation<'rop>,
        tkn: ThreadToken,
        resp: &mut ReusableBoxFuture<'a, <D as Dispatch>::Response>,
    ) {
        resp.set(async move { self.execute(op, tkn) });
    }

    #[doc(hidden)]
    pub fn sync(&self, tkn: ThreadToken) {
        self.replicas[&tkn.rid].sync(&self.log)
    }
}

#[cfg(feature = "async")]
#[cfg(test)]
mod test {
    use super::replica::test::Data;
    use super::reusable_box::ReusableBoxFuture;
    use super::*;
    use core::num::NonZeroUsize;

    #[tokio::test]
    async fn test_box_reuse() {
        use futures::executor::block_on;

        let replicas = NonZeroUsize::new(1).unwrap();
        let async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        let ttkn = async_ds.register(0).expect("Unable to register with log");

        let op = 0;
        let mut resp: ReusableBoxFuture<<Data as Dispatch>::Response> =
            ReusableBoxFuture::new(async move { Ok(0) });
        async_ds.async_execute_mut(op, ttkn, &mut resp).await;
        let res = block_on(&mut resp).unwrap();
        assert_eq!(res, 107);

        async_ds.async_execute(op, ttkn, &mut resp);
        let res = block_on(resp).unwrap();
        assert_eq!(res, 1);
    }

    #[test]
    fn test_add_replica_increments_replica_count() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        assert_eq!(async_ds.replicas.len(), 1);
        let _ = async_ds.add_replica();
        assert_eq!(async_ds.replicas.len(), 2);
        let _ = async_ds.add_replica();
        assert_eq!(async_ds.replicas.len(), 3);
    }

    #[test]
    #[should_panic(expected = "Succeeds (num_replicas < MAX_REPLICAS_PER_LOG")]
    fn test_add_replica_does_not_exceed_max_replicas() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        for _ in 0..MAX_REPLICAS_PER_LOG {
            let _ = async_ds.add_replica();
        }
    }

    #[test]
    fn test_add_replica_syncs_replica_data() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        //add a few iterations of log entries
        let _ = async_ds.execute_mut(1, ttkn_a);
        let _ = async_ds.execute_mut(5, ttkn_a);
        let _ = async_ds.execute_mut(2, ttkn_a);

        let added_replica = async_ds.add_replica().unwrap();

        let added_replica_data = async_ds.replicas[&added_replica.rid].data.read(0).junk;

        assert_eq!(3, added_replica_data);
    }

    #[test]
    fn test_add_replica_syncs_replica_lmask() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");
        //add a few iterations of log entries
        let _ = async_ds.execute_mut(1, ttkn_a);
        let _ = async_ds.execute_mut(5, ttkn_a);
        let _ = async_ds.execute_mut(2, ttkn_a);

        let replica_lmask = async_ds.log.lmasks[&0].get();
        let added_replica = async_ds.add_replica().unwrap();
        let added_replica_lmask = async_ds.log.lmasks[&added_replica.rid].get();
        assert_eq!(replica_lmask, added_replica_lmask);
    }

    #[test]
    fn test_add_replica_syncs_replica_ltail() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        //add a few iterations of log entries
        let _ = async_ds.execute_mut(1, ttkn_a);
        let _ = async_ds.execute_mut(5, ttkn_a);
        let _ = async_ds.execute_mut(2, ttkn_a);

        let replica_ltails = async_ds.log.ltails[&0].load(Ordering::Relaxed);
        let added_replica = async_ds.add_replica().unwrap();
        let added_replica_ltails = async_ds.log.ltails[&added_replica.rid].load(Ordering::Relaxed);
        assert_eq!(replica_ltails, added_replica_ltails);
    }

    #[test]
    fn test_add_replica_adds_replicas_in_order() {
        let replicas = NonZeroUsize::new(4).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        let ttkn_a = async_ds.register(0).expect("Unable to register with log");
        let ttkn_b = async_ds.register(1).expect("Unable to register with log");
        let ttkn_c = async_ds.register(2).expect("Unable to register with log");
        let _ttkn_d = async_ds.register(3).expect("Unable to register with log");

        let _ = async_ds.execute_mut(1, ttkn_a);
        let _ = async_ds.execute_mut(5, ttkn_b);
        let _ = async_ds.execute_mut(2, ttkn_c);

        let _ = async_ds.remove_replica(1);
        let _ = async_ds.remove_replica(2);

        let added_replica_four = async_ds.add_replica().unwrap();

        assert_eq!(added_replica_four.rid, 4);
    }

    #[test]
    fn test_remove_replica_returns_replica_id() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        let _ = async_ds.register(0).expect("Unable to register with log");
        let replica_id = async_ds.remove_replica(0);
        assert_eq!(replica_id.unwrap(), 0);
    }

    #[test]
    fn test_remove_replica_noop_on_invalid_replica_id_removal() {
        let replicas = NonZeroUsize::new(2).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        let ttkn_a = async_ds.register(0).expect("Unable to register with log");
        let ttkn_b = async_ds.register(1).expect("Unable to register with log");

        let _ = async_ds.execute_mut(1, ttkn_a);
        let _ = async_ds.execute_mut(4, ttkn_b);

        let _ = async_ds.remove_replica(15);

        assert_eq!(async_ds.replicas.len(), 2);
    }

    #[test]
    fn test_remove_replica_syncs_replica_data() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        //add a few iterations of log entries
        let _ = async_ds.execute_mut(1, ttkn_a);
        let _ = async_ds.execute_mut(5, ttkn_a);
        let _ = async_ds.execute_mut(2, ttkn_a);

        let added_replica = async_ds.add_replica().unwrap();

        let _ = async_ds.remove_replica(0);
        let _ = async_ds.execute_mut(5, added_replica);

        let added_replica_data = async_ds.replicas[&added_replica.rid].data.read(0).junk;
        assert_eq!(4, added_replica_data);
    }
    #[test]
    fn test_remove_replica_() {
        let replicas = NonZeroUsize::new(2).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        let ttkn_a = async_ds.register(0).expect("Unable to register with log");
        let ttkn_b = async_ds.register(1).expect("Unable to register with log");

        let _ = async_ds.execute_mut(1, ttkn_a);
        let _ = async_ds.execute_mut(4, ttkn_b);

        let _ = async_ds.remove_replica(15);

        assert_eq!(async_ds.replicas.len(), 2);
    }

    // Check Lock before removing
    // Check replica integrity after deletion
}
