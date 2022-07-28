// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Concurrent Node Replication (CNR) is a variant of node-replication which can
//! be used to implement a NUMA-aware version of concurrent and/or partitionable
//! data structures.
//!
//! It takes in a concurrent implementation of said data structure, and scales
//! it out to multiple cores and NUMA nodes by combining three techniques:
//! commutativity based work partitioning, operation logging, and flat
//! combining..
//!
//! # How does it work
//! To replicate a concurrent data structure, one needs to implement the
//! [`Dispatch`] trait for it. To map the operation to a log, each operation
//! ([`Dispatch::ReadOperation`] and [`Dispatch::WriteOperation`]) needs to
//! implement [`LogMapper`] trait. The following snippet implements [`Dispatch`]
//! for concurrent
//! [HashMap](https://docs.rs/chashmap/2.2.2/chashmap/struct.CHashMap.html) as
//! an example. A complete example (using [`Replica`] and [`Log`]) can be found
//! in the
//! [examples](https://github.com/vmware/node-replication/tree/master/cnr/examples/hashmap.rs)
//! folder.
//!
//! ```rust
//! #![feature(generic_associated_types)]
//! use node_replication::cnr::Dispatch;
//! use node_replication::cnr::LogMapper;
//! use chashmap::CHashMap;
//!
//! /// The replicated hashmap uses a concurrent hashmap internally.
//! pub struct CNRHashMap {
//!    storage: CHashMap<usize, usize>,
//! }
//!
//! /// We support a mutable put operation on the hashmap.
//! #[derive(Debug, PartialEq, Clone)]
//! pub enum Modify {
//!    Put(usize, usize),
//! }
//!
//! /// Application developer implements LogMapper for each mutable operation.
//! /// It is used to map the operation to one of the many logs. Commutative
//! /// operations can map to same or different log and conflicting operations
//! /// must map to same log.
//! impl LogMapper for Modify {
//!    fn hash(&self, nlogs: usize, logs: &mut Vec<usize>) {
//!       match self {
//!          Modify::Put(key, _val) => logs.push(*key % nlogs),
//!       }
//!    }
//! }
//!
//! /// We support an immutable read operation to lookup a key from the hashmap.
//! #[derive(Debug, PartialEq, Clone)]
//! pub enum Access {
//!    Get(usize),
//! }
//!
//! /// Application developer implements LogMapper for each immutable operation. It
//! /// is used to map the operation to one of the many log. Commutative operations
//! /// can go to same or different log and conflicts operations must map to same log.
//! impl LogMapper for Access {
//!    fn hash(&self, nlogs: usize, logs: &mut Vec<usize>) {
//!       match self {
//!          Access::Get(key) => logs.push(*key % nlogs),
//!       }
//!    }
//! }
//!
//! /// The Dispatch traits executes `ReadOperation` (our Access enum)
//! /// and `WriteOperation` (our Modify enum) against the replicated
//! /// data-structure.
//! impl Dispatch for CNRHashMap {
//!    type ReadOperation<'rop> = Access;
//!    type WriteOperation = Modify;
//!    type Response = Option<usize>;
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
//!        &self,
//!        op: Self::WriteOperation,
//!    ) -> Self::Response {
//!        match op {
//!            Modify::Put(key, value) => self.storage.insert(key, value),
//!        }
//!    }
//! }
//! ```

mod context;
mod log;
mod replica;

pub use crate::log::MAX_REPLICAS_PER_LOG;
pub use crate::replica::ReplicaToken;
pub use log::Log;
pub use replica::{Replica, MAX_THREADS_PER_REPLICA};

use alloc::vec::Vec;
use core::fmt::Debug;

/// Every data structure must implement [`LogMapper`] trait for
/// [`Dispatch::ReadOperation`] and [`Dispatch::WriteOperation`].
///
/// Data structure implement `hash` that is used to map each operation to a log.
/// All the conflicting operations must map to a single log and the commutative
/// operations can map to same or different logs based on the operation
/// argument.
///
/// [`Replica`] internally performs a modulo operation on `hash` return value
/// with the total number of logs. The data structure can implement trait to
/// return a value between 0 and (#logs-1) to avoid the modulo operation.
///
/// When the replica calls `hash`, the implementor can assume that the capacity
/// of `logs` >= `nlogs` and that `logs` is empty.
pub trait LogMapper {
    /// Method to convert the operation and it's arguments to a log number.
    fn hash(&self, nlogs: usize, logs: &mut Vec<usize>);
}

/// Trait that a data structure must implement to be usable with this library.
///
/// When this library executes a read-only operation against the data structure,
/// it invokes the `dispatch()` method with the operation as an argument.
///
/// When this library executes a write operation against the data structure, it
/// invokes the `dispatch_mut()` method with the operation as an argument.
pub trait Dispatch {
    /// A read-only operation. When executed against the data structure, an
    /// operation of this type must not mutate the data structure in anyway.
    /// Otherwise, the assumptions made by this library no longer hold.
    type ReadOperation<'a>: Sized + LogMapper;

    /// A write operation. When executed against the data structure, an
    /// operation of this type is allowed to mutate state. The library ensures
    /// that this is done so in a thread-safe manner.
    type WriteOperation: Sized + Clone + PartialEq + Debug + Send + LogMapper;

    /// The type on the value returned by the data structure when a
    /// `ReadOperation` or a `WriteOperation` successfully executes against it.
    type Response: Sized + Clone;

    /// Method on the data structure that allows a read-only operation to be
    /// executed against it.
    fn dispatch<'a>(&self, op: Self::ReadOperation<'a>) -> Self::Response;

    /// Method on the data structure that allows a write operation to be
    /// executed against it.
    fn dispatch_mut(&self, op: Self::WriteOperation) -> Self::Response;
}
