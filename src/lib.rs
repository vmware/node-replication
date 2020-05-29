// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! An operation-log based approach for data replication.
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
    type WriteOperation: Sized + Clone + PartialEq + Debug;

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
