// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! An operation-log based approach for data replication.
//#![no_std]
#![feature(atomic_min_max)]

extern crate alloc;
extern crate core;

extern crate crossbeam_utils;

#[macro_use]
extern crate log as logging;

mod context;
pub mod rwlock;

pub mod log;
pub mod replica;

use core::fmt::Debug;

/// Trait that a data structure must implement to be usable with this library. When this
/// library executes an operation against the data structure, it invokes the `dispatch()`
/// method with the operation as an argument.
pub trait Dispatch {
    type ReadOperation: Sized + Clone + PartialEq + Debug;
    type WriteOperation: Sized + Clone + PartialEq + Debug;
    type Response: Sized + Clone + Default;
    type ResponseError: Sized + Clone + Default;

    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError>;
    fn dispatch_mut(
        &mut self,
        op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError>;
}
