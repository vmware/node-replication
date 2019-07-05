#![no_std]

extern crate alloc;
extern crate core;

mod context;
pub mod log;
pub mod replica;

/// Trait that a data structure must implement to be usable with this library. When this
/// library executes an operation against the data structure, it invokes the `dispatch()`
/// method with the operations' opcode and parameters as arguments.
pub trait Dispatch {
    fn dispatch<T, P>(&self, opcode: T, params: P)
    where
        T: Sized + Copy + Default,
        P: Sized + Copy + Default;
}
