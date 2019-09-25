// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Benchmark a stack data-structure.
//!
//! This benchmark is mainly evaluating the performance of the log and the replica code
//! as all stack operations add very little overhead.

mod mkbench;

use std::cell::RefCell;

use criterion::{criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};

use node_replication::Dispatch;

/// Operations we can perform on the stack.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
enum Op {
    /// Add item to stack
    Push(u32),
    /// Pop item from stack
    Pop,
    /// Invalid operation
    Invalid,
}

impl Default for Op {
    fn default() -> Op {
        Op::Invalid
    }
}

/// Single-threaded implementation of the stack
///
/// We just use a vector.
#[derive(Debug, Clone)]
struct Stack {
    storage: RefCell<Vec<u32>>,
}

impl Stack {
    pub fn push(&self, data: u32) {
        self.storage.borrow_mut().push(data);
    }

    pub fn pop(&self) -> Option<u32> {
        self.storage.borrow_mut().pop()
    }
}

impl Default for Stack {
    /// Return a dummy stack with some initial (50k) elements.
    fn default() -> Stack {
        let s = Stack {
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

    /// Implements how we execute operation from the log against our local stack
    fn dispatch(&self, op: Self::Operation) -> Self::Response {
        match op {
            Op::Push(v) => {
                self.push(v);
                return None;
            }
            Op::Pop => return self.pop(),
            Op::Invalid => unreachable!("Op::Invalid?"),
        }
    }
}

/// Generate a random sequence of operations that we'll perform:
fn generate_operations(nop: usize) -> Vec<Op> {
    let mut orng = thread_rng();
    let mut arng = thread_rng();

    let mut ops = Vec::with_capacity(nop);
    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 2usize {
            0usize => ops.push(Op::Pop),
            1usize => ops.push(Op::Push(arng.gen())),
            _ => ops.push(Op::Invalid),
        }
    }

    ops
}

/// Compare against a stack with and without a log in-front.
fn stack_single_threaded(c: &mut Criterion) {
    // Benchmark 500k operations per iteration
    const NOP: usize = 500_000;
    // Use a 10 GiB log size
    const LOG_SIZE_BYTES: usize = 10 * 1024 * 1024 * 1024;

    let ops = generate_operations(NOP);
    mkbench::baseline_comparison::<Stack>(c, "stack", ops, LOG_SIZE_BYTES);
}

/// Compare scalability of a node-replicated stack.
fn stack_scale_out(c: &mut Criterion) {
    // Benchmark 500k operations per iteration
    const NOP: usize = 500_000;
    // Use a 10 GiB log size
    const LOG_SIZE_BYTES: usize = 10 * 1024 * 1024 * 1024;

    let ops = generate_operations(NOP);
    mkbench::scaleout::<Stack>(c, "stackscale", ops, LOG_SIZE_BYTES);
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = stack_single_threaded, stack_scale_out
);

criterion_main!(benches);
