#![no_std]

extern crate alloc;
extern crate core;

extern crate crossbeam_utils;

mod context;

pub mod log;
pub mod replica;

/// Trait that a data structure must implement to be usable with this library. When this
/// library executes an operation against the data structure, it invokes the `dispatch()`
/// method with the operation as an argument.
pub trait Dispatch {
    type Operation: Sized + Copy + Default + PartialEq + core::fmt::Debug;
    type Response: Sized + Copy + Default;

    fn dispatch(&self, op: Self::Operation) -> Self::Response;
}
