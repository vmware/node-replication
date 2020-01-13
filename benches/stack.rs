// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Defines a stack data-structure that can be replicated.

use node_replication::Dispatch;
use rand::{thread_rng, Rng};
use std::cell::RefCell;

/// Operations we can perform on the stack.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Op {
    /// Add item to stack
    Push(u32),
    /// Pop item from stack
    Pop,
}

/// Single-threaded implementation of the stack
///
/// We just use a vector.
#[derive(Debug, Clone)]
pub struct Stack {
    storage: Vec<u32>,
}

impl Stack {
    pub fn push(&mut self, data: u32) {
        self.storage.push(data);
    }

    pub fn pop(&mut self) -> Option<u32> {
        self.storage.pop()
    }
}

impl Default for Stack {
    /// Return a dummy stack with some initial (50k) elements.
    fn default() -> Stack {
        let mut s = Stack {
            storage: Default::default(),
        };

        for e in 0..50000 {
            s.push(e);
        }

        s
    }
}

impl Dispatch for Stack {
    type Operation = Op;
    type Response = Option<u32>;
    type ResponseError = ();

    /// Implements how we execute operation from the log against our local stack
    fn dispatch(&mut self, op: Self::Operation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            Op::Push(v) => {
                self.push(v);
                return Ok(None);
            }
            Op::Pop => return Ok(self.pop()),
        }
    }
}

/// Generate a random sequence of operations that we'll perform:
pub fn generate_operations(nop: usize) -> Vec<Op> {
    let mut orng = thread_rng();
    let mut arng = thread_rng();

    let mut ops = Vec::with_capacity(nop);
    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 2usize {
            0usize => ops.push(Op::Pop),
            1usize => ops.push(Op::Push(arng.gen())),
            _ => unreachable!(),
        }
    }

    ops
}
