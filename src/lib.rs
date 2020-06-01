// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Node Replication(NR) is a library which can be used to implement a concurrent version of any
//! single threaded data structure: It takes in a single threaded implementation of said data
//! structure, and scales it out to multiple cores and NUMA nodes by combining two techniques:
//! operation logging and flat combining.
//!
//! # How does it work
//! To replicate a single-threaded data structure, one needs to implement [Dispatch](trait.Dispatch.html) trait.
//! The following example uses hashmap as an example.
//! ```
//! use node_replication::Dispatch;
//! use std::collections::HashMap;
//!
//! /// The single-threaded NrHashMap which internally uses std HashMap.
//! pub struct NrHashMap {
//!    storage: HashMap<u64, u64>,
//! }
//!
//! /// We support mutable Put() operation on the NrHashMap.
//! #[derive(Debug, Eq, PartialEq, Clone)]
//! pub enum OpWr {
//!    Put(u64, u64),
//! }
//!
//! /// We support immutable Get() operation on the NrHashMap.
//! #[derive(Debug, Eq, PartialEq, Clone)]
//! pub enum OpRd {
//!    Get(u64),
//! }
//!
//! /// The Dispatch traits executes `ReadOperation` (our OpRd enum)
//! /// and `WriteOperation` (our `OpWr` enum) against the replicated
//! /// data-structure.
//! impl Dispatch for NrHashMap {
//!    type ReadOperation = OpRd;
//!    type WriteOperation = OpWr;
//!    type Response = Option<u64>;
//!    type ResponseError = ();
//!
//!    /// The `dispatch` function applies the immutable operations.
//!    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
//!        match op {
//!            OpRd::Get(key) => Ok(self.storage.get(&key).map(|v| *v)),
//!        }
//!    }
//!
//!    /// The `dispatch_mut` function applies the mutable operations.
//!    fn dispatch_mut(
//!        &mut self,
//!        op: Self::WriteOperation,
//!    ) -> Result<Self::Response, Self::ResponseError> {
//!        match op {
//!            OpWr::Put(key, value) => Ok(self.storage.insert(key, value)),
//!        }
//!    }
//! }
//! ```
#![no_std]
#![feature(atomic_min_max)]
#![feature(new_uninit)]
#![feature(get_mut_unchecked)]

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
pub mod rwlock;

pub mod log;
pub mod replica;

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
    type ReadOperation: Sized + Clone + PartialEq + Debug;

    /// A write operation. When executed against the data structure, an operation of
    /// this type is allowed to mutate state. The library ensures that this is done so
    /// in a thread-safe manner.
    type WriteOperation: Sized + Clone + PartialEq + Debug + Send;

    /// The type on the value returned by the data structure when a `ReadOperation` or a
    /// `WriteOperation` successfully executes against it.
    type Response: Sized + Clone + Default;

    /// The type on the value returned by the data structure when a `ReadOperation` or a
    /// `WriteOperation` unsuccessfully executes against it.
    type ResponseError: Sized + Clone + Default;

    /// Method on the data structure that allows a read-only operation to be
    /// executed against it.
    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError>;

    /// Method on the data structure that allows a write operation to be
    /// executed against it.
    fn dispatch_mut(
        &mut self,
        op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError>;
}
