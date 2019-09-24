// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Benchmark to compare a single-threaded stack vs. a node-replicated stack.

use std::cell::RefCell;
use std::sync::Arc;

use rand::{thread_rng, Rng};
use criterion::{criterion_main, criterion_group, Criterion, Throughput};

use node_replication::{log::Log, replica::Replica, Dispatch};

/// Benchmark 500k operations per iteration
const NOP: usize = 500_000;

/// Use a 10 GiB log size
const LOG_SIZE_BYTES: usize = 10 * 1024 * 1024 * 1024;

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
fn st_setup(nop: usize) -> Vec<Op> {
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
/// 
/// Shows overhead of log approach against best possible
/// single-threaded implementation.
fn stack_single_threaded(c: &mut Criterion) {
    let nop = NOP;
    let ops = st_setup(nop);
    let s: Stack = Default::default();

    // First benchmark is just a stack on a single thread:
    let mut group = c.benchmark_group("stack");
    group.throughput(Throughput::Elements(nop as u64));
    group.bench_function("baseline", |b| {
        b.iter(|| {
            for i in 0..nop {
                s.dispatch(ops[i]);
            }
        })
    });

    // 2nd benchmark we compare the stack but now we put a log in front:
    let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(
        LOG_SIZE_BYTES,
    ));
    let r  = Replica::<Stack>::new(&log);
    let ridx = r.register().expect("Failed to register with Replica.");

    group.bench_function("log", |b| {
        b.iter(|| {
            for i in 0..nop {
                r.execute(ops[i], ridx);
            }
        })
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = stack_single_threaded, 
);

criterion_main!(benches);
