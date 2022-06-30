// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Node Replication (NR) is a library which can be used to implement a concurrent version
//! of any single-threaded data structure. It takes in a single threaded implementation of
//! said data structure, and scales it out to multiple cores and NUMA nodes by combining
//! three techniques: reader-writer locks, operation logging and flat combining.
//!
//! # How does it work
//! To replicate a single-threaded data structure, one needs to implement the [`Dispatch`]
//! trait for it. The following snippet implements [`Dispatch`] for
//! `std::collections::HashMap` as an example. The full example (using [`NodeReplicated`]
//! and [`Dispatch`] can be found in the
//! [examples](https://github.com/vmware/node-replication/tree/master/nr/examples/hashmap.rs)
//! folder.
//!
//! ```
//! use node_replication::Dispatch;
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
//!    type ReadOperation = Access;
//!    type WriteOperation = Modify;
//!    type Response = Option<u64>;
//!
//!    /// The `dispatch` function applies the immutable operations.
//!    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
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
#![no_std]
#![cfg_attr(
    feature = "unstable",
    feature(new_uninit, get_mut_unchecked, negative_impls)
)]
#![feature(box_syntax)]
#![feature(allocator_api)]
#![feature(nonnull_slice_from_raw_parts)]

#[cfg(test)]
extern crate std;

extern crate alloc;
extern crate core;

extern crate crossbeam_utils;

#[macro_use]
extern crate log as logging;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::fmt::Debug;
use core::marker::Sync;
use core::num::NonZeroUsize;

use arrayvec::ArrayVec;

mod context;
mod log;
mod replica;
mod reusable_box;

#[cfg(not(loom))]
#[path = "rwlock.rs"]
pub mod rwlock;
#[cfg(loom)]
#[path = "loom_rwlock.rs"]
pub mod rwlock;

use crate::log::{Log, MAX_REPLICAS_PER_LOG};
use replica::{CombinerLock, Replica, ReplicaError, ReplicaId, ReplicaToken};

/// Trait that a data structure must implement to be usable with this library.
///
/// When this library executes a read-only operation against the data structure,
/// it invokes the [`Dispatch::dispatch`] method with the `ReadOperation` as an
/// argument.
///
/// When this library executes a write operation against the data structure, it
/// invokes the [`Dispatch::dispatch_mut`] method with the `WriteOperation` as
/// an argument.
pub trait Dispatch {
    /// A read-only operation. When executed against the data structure, an
    /// operation of this type must not mutate the data structure in anyway.
    /// Otherwise, the assumptions made by this library no longer hold.
    type ReadOperation: Sized;

    /// A write operation. When executed against the data structure, an
    /// operation of this type is allowed to mutate state. The library ensures
    /// that this is done so in a thread-safe manner.
    type WriteOperation: Sized + Clone + PartialEq + Send;

    /// The type on the value returned by the data structure when a
    /// `ReadOperation` or a `WriteOperation` successfully executes against it.
    type Response: Sized + Clone;

    /// Method on the data structure that allows a read-only operation to be
    /// executed against it.
    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response;

    /// Method on the data structure that allows a write operation to be
    /// executed against it.
    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response;
}

/// A token handed out to threads registered with replicas.
///
/// # Note
/// For maximum type-safety this would be an affine type, then we'd have to
/// return it again in `execute` and `execute_mut`. However it feels like this
/// would hurt API ergonomics a lot.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct ThreadToken {
    /// Which replica we're registered with.
    rid: ReplicaId,
    /// The registration token for this thread that we got from the replica
    /// (through [`Replica::register`]) identified by `rid`.
    rtkn: ReplicaToken,
}

/// Passed to the user specified function to indicate that we change the replica
/// we're operating on to one that is *not* the replica which we're registered
/// (e.g., not the one identified by `rid` in [`ThreadToken`]).
///
/// # Note
/// The pattern for the enum arguments that are passed to the affinity function
/// always comes in pairs of `Replica` followed by `Revert`:
///
/// 1. `old = affinty_function(AffinityChange::Replica(some_non_local_rid))`
/// 2. library does some stuff with different affinity...
/// 3. `affinity_change_function(AffinityChange::Revert(old))`
#[derive(Copy, Clone, Debug, PartialEq)]
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

/// To make it harder to use the same ThreadToken on multiple threads.
#[cfg(features = "unstable")]
impl !Send for ThreadToken {}

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

/// Erros that can be encountered when interacting with [`NodeReplicated`].
#[derive(Debug)]
pub enum NodeReplicatedError {
    OutOfMemory,
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

pub struct NodeReplicated<D: Dispatch + Sync> {
    log: Log<D::WriteOperation>,
    replicas: Vec<Box<Replica<D>>>,
    affinity_mngr: AffinityManager,
}

impl<D> NodeReplicated<D>
where
    D: Default + Dispatch + Sized + Sync,
{
    /// Creates a new, replicated data-structure from a single-threaded
    /// data-structure that implements [`Dispatch`]. It also uses the
    /// [`Default`] constructor to create a initial data-structure on all
    /// replicas (`num_replicas` times).
    ///
    /// # Arguments
    /// - `num_replicas`: How many replicas you want to create. Typically the
    ///   number of NUMA nodes in your system.
    /// - `chg_mem_affinity`: A user-provided function that is called whenever
    ///   the code operates on a certain [`Replica`] that is not local to the
    ///   thread that we're running on (can happen if a replica falls behind and
    ///   we're temporarily executing operation on behalf of this replica so we
    ///   can make progress on our own replica). This function is used to ensure
    ///   that memory will still be allocated from the right NUMA node. See
    ///   [`AffinityChange`] for more information on how implement this function
    ///   and handle its arguments.
    pub fn new(
        num_replicas: NonZeroUsize,
        chg_mem_affinity: impl Fn(AffinityChange) -> usize + Send + Sync + 'static,
    ) -> Result<Self, NodeReplicatedError> {
        assert!(num_replicas.get() < MAX_REPLICAS_PER_LOG);
        let affinity_mngr = AffinityManager::new(Box::try_new(chg_mem_affinity)?);
        let log = Log::default();

        let mut replicas = Vec::new();
        replicas.try_reserve(num_replicas.get())?;

        for replica_id in 0..num_replicas.get() {
            let log_token = log
                .register()
                .expect("Succeeds (num_replicas < MAX_REPLICAS_PER_LOG)");

            let r = {
                // Allocate the replica on the proper NUMA node
                let _aff_tkn = affinity_mngr.switch(replica_id);
                Box::try_new(Replica::new(log_token))?
                // aff_tkn is dropped here
            };

            // This succeeds, we did `try_reserve` earlier so no `try_push` is
            // necessary.
            replicas.push(r);
        }

        Ok(NodeReplicated {
            replicas,
            log,
            affinity_mngr,
        })
    }
}

impl<D> NodeReplicated<D>
where
    D: Clone + Dispatch + Sized + Sync,
{
    /// Creates a new, replicated data-structure from a single-threaded data-structure
    /// (`ds`) that implements [`Dispatch`].
    ///
    /// # Arguments
    /// See [`NodeReplicated::new`].
    ///
    /// # Example
    /// TBD.
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
    D: Dispatch + Sized + Sync,
{
    /// Registers a thread with a given replica in the [`NodeReplicated`]
    /// data-structure. Returns an Option containing a [`ThreadToken`] if the
    /// registration was successful. None if the registration failed.
    ///
    /// The [`ThreadToken`] is used to identify the thread to issue the
    /// operation for subsequent [`NodeReplicated::execute()`] and
    /// [`NodeReplicated::execute_mut`] calls.
    ///
    /// # Arguments
    /// - `replica_id`: Which replica the thread should be registered with. This
    /// should be less than the `num_replicas` argument provided in the
    /// constructor (see [`NodeReplicated::new()`]). In most cases, `replica_id`
    /// will correspond to the NUMA node that the thread is running on.
    ///
    /// # Example
    /// TBD.
    pub fn register(&self, replica_id: ReplicaId) -> Option<ThreadToken> {
        if replica_id < self.replicas.len() {
            let rtkn = self.replicas[replica_id].register()?;
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
            self.replicas[tkn.rid].execute_mut_locked(&self.log, op, tkn.rtkn, combiner_lock)
        } else {
            self.replicas[tkn.rid].execute_mut(&self.log, op, tkn.rtkn)
        }
    }

    pub fn execute_mut(
        &self,
        op: <D as Dispatch>::WriteOperation,
        tkn: ThreadToken,
    ) -> <D as Dispatch>::Response {
        /// An enum to keep track of a stack of operations we should do on Replicas.
        ///
        /// e.g., either `Sync` an out-of-date, behind replica, or call `execute_locked` or
        /// `execute_mut_locked` to resume the operation with a combiner lock.
        enum ResolveOp<'a, D: core::marker::Sync + Dispatch + Sized> {
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
                            let _r = self.replicas[stuck_ridx].sync(&self.log);
                            // Affinity is reverted here, _aftkn is dropped.
                        }
                        return self.replicas[tkn.rid]
                            .get_response(&self.log, tkn.rtkn.tid())
                            .expect("GcFailed has to produced a response");
                    }
                },
                ResolveOp::Sync(ridx) => {
                    // Holds trivially because of all the other asserts in this function
                    debug_assert_ne!(ridx, tkn.rid);
                    let _aftkn = self.affinity_mngr.switch(ridx);
                    match self.replicas[ridx].sync(&self.log) {
                        Ok(()) => continue,
                        Err(stuck_ridx) => {
                            assert_ne!(stuck_ridx, tkn.rid);
                            // This doesn't do an allocation so it's ok that
                            // we're in a different affinity.
                            q.push(ResolveOp::Sync(stuck_ridx));
                        }
                    }
                    // _aftkn is dropped here, reverting affinity change
                }
            }
        }
    }

    fn try_execute<'a>(
        &'a self,
        op: <D as Dispatch>::ReadOperation,
        tkn: ThreadToken,
        cl: Option<CombinerLock<'a, D>>,
    ) -> Result<<D as Dispatch>::Response, (ReplicaError<D>, <D as Dispatch>::ReadOperation)> {
        if let Some(combiner_lock) = cl {
            self.replicas[tkn.rid].execute_locked(&self.log, op, tkn.rtkn, combiner_lock)
        } else {
            self.replicas[tkn.rid].execute(&self.log, op, tkn.rtkn)
        }
    }

    pub fn execute(
        &self,
        op: <D as Dispatch>::ReadOperation,
        tkn: ThreadToken,
    ) -> <D as Dispatch>::Response {
        /// An enum to keep track of a stack of operations we should do on Replicas.
        ///
        /// e.g., either `Sync` an out-of-date, behind replica, or call `execute_locked` or
        /// `execute_mut_locked` to resume the operation with a combiner lock.
        enum ResolveOp<'a, D: core::marker::Sync + Dispatch + Sized> {
            /// Resumes a replica that earlier returned with an Error (and the CombinerLock).
            Exec(Option<CombinerLock<'a, D>>, D::ReadOperation),
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
                    Err((ReplicaError::GcFailed(stuck_ridx), _op)) => {
                        assert_ne!(stuck_ridx, tkn.rid);
                        q.push(ResolveOp::Sync(stuck_ridx));
                        unreachable!("GC should not fail for reads");
                    }
                },
                ResolveOp::Sync(ridx) => {
                    // Holds trivially because of all the other asserts in this function
                    debug_assert_ne!(ridx, tkn.rid);
                    let _aftkn = self.affinity_mngr.switch(ridx);
                    match self.replicas[ridx].sync(&self.log) {
                        Ok(()) => continue,
                        Err(stuck_ridx) => {
                            assert!(stuck_ridx != tkn.rid);
                            q.push(ResolveOp::Sync(stuck_ridx));
                        }
                    }
                    // _aftkn is dropped here, reverting affinity change
                }
            }
        }
    }

    pub fn sync(&self, tkn: ThreadToken) {
        match self.replicas[tkn.rid].sync(&self.log) {
            Ok(r) => r,
            Err(stuck_ridx) => panic!("replica#{} is stuck", stuck_ridx),
        }
    }
}

#[cfg(doctest)]
mod test_readme {
    macro_rules! external_doc_test {
        ($x:expr) => {
            #[doc = $x]
            extern "C" {}
        };
    }

    external_doc_test!(include_str!("../README.md"));
}
