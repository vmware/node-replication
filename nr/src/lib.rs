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

#[macro_use]
extern crate static_assertions;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::marker::Sync;
use core::num::NonZeroUsize;

use arrayvec::ArrayVec;

use replica::CombinerLock;

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
use replica::{Replica, ReplicaToken};

use core::fmt::Debug;

/// Trait that a data structure must implement to be usable with this library.
///
/// When this library executes a read-only operation against the data structure, it
/// invokes the `dispatch()` method with the operation as an argument.
///
/// When this library executes a write operation against the data structure, it invokes
/// the `dispatch_mut()` method with the operation as an argument.
pub trait Dispatch {
    /// A read-only operation. When executed against the data structure, an operation
    /// of this type must not mutate the data structure in anyway. Otherwise, the
    /// assumptions made by this library no longer hold.
    type ReadOperation: Sized + Clone + PartialEq + Debug + Send;

    /// A write operation. When executed against the data structure, an operation of
    /// this type is allowed to mutate state. The library ensures that this is done so
    /// in a thread-safe manner.
    type WriteOperation: Sized + Clone + PartialEq + Debug + Send;

    /// The type on the value returned by the data structure when a `ReadOperation` or a
    /// `WriteOperation` successfully executes against it.
    type Response: Sized + Clone;

    /// Method on the data structure that allows a read-only operation to be
    /// executed against it.
    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response;

    /// Method on the data structure that allows a write operation to be
    /// executed against it.
    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response;
}

/// Unique identifier for the given replica (it's probably the same as the NUMA
/// node that this replica corresponds to).
pub type ReplicaId = usize;

/// A token handed out to threads registered with replicas.
///
/// # Note
/// Ideally this would be an affine type and returned again by `execute` and `execute_ro`.
/// However it feels like this would hurt API ergonomics a lot.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct ThreadToken {
    rid: ReplicaId,
    rtkn: ReplicaToken,
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

/// An enum to keep track of a stack of operations we should do on Replicas.
///
/// e.g., either `Sync` an out-of-date, behind replica, or call `execute_locked` or
/// `execute_mut_locked` to resume the operation with a combiner lock.
enum ResolveOp<'a, D: core::marker::Sync + Dispatch + Sized> {
    /// Resumes a replica that earlier returned with an Error (and the CombinerLock).
    Exec(Option<CombinerLock<'a, D>>),
    /// Indicates need to [`Replica::sync()`] a replica with the given ID.
    Sync(usize),
}

/// Erros that can be encountered when interacting with [`NodeReplicated`].
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
}

impl<D> NodeReplicated<D>
where
    D: Default + Dispatch + Sized + Sync,
{
    /// Creates a new, replicated data-structure from a single-threaded data-structure
    /// that implements [`Dispatch`]. It also uses the [`Default`] constructor to create
    /// an initial data-structure on all replicas.
    ///
    /// # Arguments
    /// - `num_replicas`: How many replicas you want to create. Typically the number of
    ///   NUMA nodes in your system.
    /// - `_set_mem_affinity`: A user-provided function that is called whenever the code
    /// operates on a certain [`Replica`] to give the client the ability to e.g., change
    /// the memory affinity of the underlying thread.
    ///
    /// # Example
    /// TBD.
    pub fn new<AffChgFn>(
        num_replicas: NonZeroUsize,
        _set_mem_affinity: AffChgFn,
    ) -> Result<Self, NodeReplicatedError>
    where
        AffChgFn: Fn(Option<usize>) + Sized + 'static + Send + Sync,
    {
        assert!(num_replicas.get() < MAX_REPLICAS_PER_LOG);
        let log = Log::default();

        let mut replicas = Vec::new();
        replicas.try_reserve(num_replicas.get())?;

        for _replica_id in 0..num_replicas.get() {
            let log_token = log
                .register()
                .expect("Succeeds (num_replicas < MAX_REPLICAS_PER_LOG)");

            // Allocate replica on the proper NUMA node
            let r = {
                // TODO: change affinity
                Box::try_new(Replica::new(log_token))?
                // reset affinity
            };

            // Succeeds (try_reserve)
            replicas.push(r);
        }

        Ok(NodeReplicated { replicas, log })
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
    /// - `num_replicas`: How many replicas you want to create. Typically the number of
    ///   NUMA nodes in your system.
    /// - `set_mem_affinity`: A user-provided function that is called whenever the code
    /// operates on a certain [`Replica`] to give the client the ability to e.g., change
    /// the memory affinity of the underlying thread.
    /// - `ds`: The initial version of the data-structure. Will be cloned for each
    ///   replica.
    ///
    /// # Example
    /// TBD.
    pub fn with_data<AffChgFn>(
        _num_replicas: NonZeroUsize,
        _set_mem_affinity: AffChgFn,
        _ds: D,
    ) -> Result<Self, NodeReplicatedError>
    where
        AffChgFn: Fn(Option<usize>) + Sized + 'static + Send + Sync,
    {
        unreachable!("complete me")
    }
}

impl<D> NodeReplicated<D>
where
    D: Dispatch + Sized + Sync,
{
    /// Registers a thread with a given replica in the [`NodeReplicated`] data-structure.
    /// Returns an Option containing a [`ThreadToken`] if the registration was
    /// successfull. None if the registration failed.
    ///
    /// The [`ThreadToken`] is used to identify the thread to issue the operation for
    /// subsequent [`NodeReplicated::execute()`] and [`NodeReplicated::execute_mut`]
    /// calls.
    ///
    /// # Arguments
    /// - `replica_id`: Which replica the thread should be registered with. This should be
    /// less than the `num_replicas` argument provided in the constructor
    /// ([`NodeReplicated::new()`]). In most cases, this will probably correspond to the
    /// NUMA node that the thread is running on.
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
    ) -> Result<<D as Dispatch>::Response, (usize, CombinerLock<'a, D>)> {
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
        let mut q = ArrayVec::<ResolveOp<D>, { crate::log::MAX_REPLICAS_PER_LOG }>::new();
        loop {
            match q.pop().unwrap_or(ResolveOp::Exec(None)) {
                ResolveOp::Exec(cl) => match self.try_execute_mut(op.clone(), tkn, cl) {
                    Ok(resp) => {
                        assert!(q.is_empty());
                        return resp;
                    }
                    Err((stuck_ridx, cl_acq)) => {
                        assert_ne!(stuck_ridx, tkn.rid);
                        q.push(ResolveOp::Exec(Some(cl_acq)));
                        q.push(ResolveOp::Sync(stuck_ridx));
                    }
                },
                ResolveOp::Sync(ridx) => match self.replicas[ridx].sync(&self.log) {
                    Ok(()) => continue,
                    Err(stuck_ridx) => {
                        assert_ne!(stuck_ridx, tkn.rid);
                        if stuck_ridx != tkn.rid {
                            q.push(ResolveOp::Sync(stuck_ridx));
                        }
                    }
                },
            }
        }
    }

    fn try_execute<'a>(
        &'a self,
        op: <D as Dispatch>::ReadOperation,
        tkn: ThreadToken,
        cl: Option<CombinerLock<'a, D>>,
    ) -> Result<<D as Dispatch>::Response, (usize, CombinerLock<'a, D>)> {
        if let Some(combiner_lock) = cl {
            self.replicas[tkn.rid].execute_locked(&self.log, op.clone(), tkn.rtkn, combiner_lock)
        } else {
            self.replicas[tkn.rid].execute(&self.log, op.clone(), tkn.rtkn)
        }
    }

    pub fn execute(
        &self,
        op: <D as Dispatch>::ReadOperation,
        tkn: ThreadToken,
    ) -> <D as Dispatch>::Response {
        let mut q = ArrayVec::<ResolveOp<D>, { crate::log::MAX_REPLICAS_PER_LOG }>::new();
        loop {
            match q.pop().unwrap_or(ResolveOp::Exec(None)) {
                ResolveOp::Exec(cl) => match self.try_execute(op.clone(), tkn, cl) {
                    Ok(resp) => {
                        assert!(q.is_empty());
                        return resp;
                    }
                    Err((stuck_ridx, cl_acq)) => {
                        assert!(stuck_ridx != tkn.rid);
                        q.push(ResolveOp::Exec(Some(cl_acq)));
                        q.push(ResolveOp::Sync(stuck_ridx));
                    }
                },
                ResolveOp::Sync(ridx) => match self.replicas[ridx].sync(&self.log) {
                    Ok(()) => continue,
                    Err(stuck_ridx) => {
                        assert!(stuck_ridx != tkn.rid);
                        q.push(ResolveOp::Sync(stuck_ridx));
                    }
                },
            }
        }
    }

    fn sync(&self, tkn: ThreadToken) {
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
