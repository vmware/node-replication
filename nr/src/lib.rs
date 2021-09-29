// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Node Replication (NR) is a library which can be used to implement a
//! concurrent version of any single threaded data structure. It takes in a
//! single threaded implementation of said data structure, and scales it out to
//! multiple cores and NUMA nodes by combining three techniques: reader-writer
//! locks, operation logging and flat combining.
//!
//! # How does it work
//! To replicate a single-threaded data structure, one needs to implement the
//! [Dispatch](trait.Dispatch.html) trait for it. The following snippet
//! implements [Dispatch](trait.Dispatch.html) for
//! [HashMap](https://doc.rust-lang.org/std/collections/struct.HashMap.html)
//! as an example. A complete example
//! (using [Replica](struct.Replica.html) and [Log](struct.Log.html)) can be found in the
//! [examples](https://github.com/vmware/node-replication/tree/master/examples/hashmap.rs)
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

#[cfg(test)]
extern crate std;

extern crate alloc;
extern crate core;

extern crate crossbeam_utils;

#[macro_use]
extern crate log as logging;

#[macro_use]
extern crate static_assertions;

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

pub use crate::log::{Log, MAX_REPLICAS_PER_LOG};
pub use context::MAX_PENDING_OPS;
pub use replica::{Replica, ReplicaToken, MAX_THREADS_PER_REPLICA};
pub use reusable_box::ReusableBoxFuture;

use core::fmt::Debug;

/// Trait that a data structure must implement to be usable with this library.
///
/// When this library executes a read-only operation against the data structure,
/// it invokes the `dispatch()` method with the operation as an argument.
///
/// When this library executes a write operation against the data structure, it
/// invokes the `dispatch_mut()` method with the operation as an argument.
pub trait Dispatch {
    /// A read-only operation. When executed against the data structure, an operation
    /// of this type must not mutate the data structure in anyway. Otherwise, the
    /// assumptions made by this library no longer hold.
    type ReadOperation: Sized + Clone + PartialEq + Debug + Send;

    /// A write operation. When executed against the data structure, an operation of
    /// this type is allowed to mutate state. The library ensures that this is done so
    /// in a thread-safe manner.
    type WriteOperation: Sized + Clone + PartialEq + Debug + Send + Sync;

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
