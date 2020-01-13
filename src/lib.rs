// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! An operation-log based approach for data replication.
//#![no_std]

extern crate alloc;
extern crate core;

extern crate crossbeam_utils;

#[macro_use]
extern crate log as logging;

mod context;

pub mod log;
pub mod replica;

use core::fmt::Debug;

/// Trait that a data structure must implement to be usable with this library. When this
/// library executes an operation against the data structure, it invokes the `dispatch()`
/// method with the operation as an argument.
pub trait Dispatch {
    type Operation: Sized + Clone + PartialEq + Debug;
    type Response: Sized + Copy + Default;
    type ResponseError: Sized + Copy + Default;

    fn dispatch(&mut self, op: Self::Operation) -> Result<Self::Response, Self::ResponseError>;
}
