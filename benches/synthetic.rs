// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Benchmark of a synthetic node replicated data-structure.
//! 
//! The data-structure is configurable with 4 parameters: cold_reads, cold_writes, hot_reads, hot_writes
//! which simulates how many cold/random and hot/cached cache-lines are touched for every operation.
//! 
//! It evaluates the overhead of the log with an abstracted model of a generic data-structure
//! to measure the cache-impact.

use std::cell::RefCell;

use rand::{thread_rng, Rng};
use criterion::{criterion_main, criterion_group, Criterion};
use crossbeam_utils::CachePadded;

use node_replication::Dispatch;

mod mkbench;

/// Operations we can perform on the AbstractDataStructure.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
enum Op {
    /// Read a bunch of local memory.
    ReadOnly(usize, usize, usize),
    /// Write a bunch of local memory.
    WriteOnly(usize, usize, usize),
    /// Read some memory, then write some.
    ReadWrite(usize, usize, usize),
    /// Invalid operation.
    Invalid,
}

impl Op {
    #[allow(unused)]
    fn set_tid(&mut self, tid: usize) {
        match self {
            Op::ReadOnly(ref mut a, _b, _c) => *a = tid,
            Op::WriteOnly(ref mut a, _b, _c) => *a = tid,
            Op::ReadWrite(ref mut a, _b, _c) => *a = tid,
            _ => (),
        };
    }
}

impl Default for Op {
    fn default() -> Op {
        Op::Invalid
    }
}

#[derive(Debug, Clone)]
struct AbstractDataStructure {
    /// Total cache-lines
    n: usize,
    /// Amount of reads for cold-reads.
    cold_reads: usize,
    /// Amount of writes for cold-writes.
    cold_writes: usize,
    /// Amount of hot cache-lines read.
    hot_reads: usize,
    /// Amount of hot writes to cache-lines
    hot_writes: usize,
    /// Backing memory
    storage: RefCell<Vec<CachePadded<usize>>>,
}

impl Default for AbstractDataStructure {
    fn default() -> Self {
        AbstractDataStructure::new(100_000, 5, 5, 10, 10)
    }
}

impl AbstractDataStructure {
    
    fn new(n: usize, cold_reads: usize, cold_writes: usize, hot_reads: usize, hot_writes: usize) -> AbstractDataStructure {
        debug_assert!(hot_reads + cold_writes < n);
        debug_assert!(hot_reads + cold_reads < n);
        debug_assert!(hot_writes < hot_reads);
        
        // Maximum buffer space (within a data-structure).
        const MAX_BUFFER_SIZE: usize = 400_000;
        debug_assert!(n < MAX_BUFFER_SIZE);
        
        let storage = RefCell::new(Vec::with_capacity(n));
        {
            let mut storage = storage.borrow_mut();
            for i in 0..n {
                storage.push(CachePadded::from(i));
            }
        }
        
        AbstractDataStructure {
            n,
            cold_reads,
            cold_writes,
            hot_reads,
            hot_writes,
            storage
        }
    }

    pub fn read(&self, tid: usize, rnd1: usize, rnd2: usize) -> usize {
        let storage = self.storage.borrow();
        let mut sum = 0;

        // Hot cache-lines (reads)
        let begin = rnd2;
        let end = begin + self.hot_writes;
        for i in begin..end {
            let index = i % self.hot_reads;
            sum += *storage[index];
        }

        // Cold cache-lines (random stride reads)
        let mut begin = rnd1 * tid;
        for _i in 0..self.cold_reads {
            let index = begin % (self.n - self.hot_reads) + self.hot_reads;
            begin += rnd2;
            sum += *storage[index];
        }

        sum
    }

    pub fn write(&self, tid: usize, rnd1: usize, rnd2: usize) -> usize {
        let mut storage = self.storage.borrow_mut();

        // Hot cache-lines (updates)
        let begin = rnd2;
        let end = begin + self.hot_writes;
        for i in begin..end {
            let index = i % self.hot_reads;
            storage[index] = CachePadded::new(tid);
        }

        // Cold cache-lines (random stride updates)
        let mut begin = rnd1 * tid;
        for _i in 0..self.cold_writes {
            let index = begin % (self.n - self.hot_reads) + self.hot_reads;
            begin += rnd2;
            storage[index] = CachePadded::new(tid);
        }

        0
    }

    pub fn read_write(&self, tid: usize, rnd1: usize, rnd2: usize) -> usize {
        let mut storage = self.storage.borrow_mut();

        // Hot cache-lines (updates)
        let begin = rnd2;
        let end = begin + self.hot_writes;
        for i in begin..end {
            let index = i % self.hot_reads;
            storage[index] = CachePadded::new(*storage[index] + 1);
        }

        // Cold cache-lines (random stride updates)
        let mut sum = 0;
        let mut begin = rnd1 * tid;
        for _i in 0..self.cold_writes {
            let index = begin % (self.n - self.hot_reads) + self.hot_reads;
            begin += rnd2;
            sum += *storage[index];
            storage[index] = CachePadded::new(*storage[index] + 1);
        }

        sum
    }
}

impl Dispatch for AbstractDataStructure {
    type Operation = Op;
    type Response = usize;

    /// Implements how we execute operation from the log against abstract DS
    fn dispatch(&self, op: Self::Operation) -> Self::Response {
        match op {
            Op::ReadOnly(a, b, c) => return self.read(a, b, c),
            Op::WriteOnly(a, b, c) => return self.write(a, b, c),
            Op::ReadWrite(a, b, c) => return self.read_write(a, b, c),
            Op::Invalid => unreachable!("Op::Invalid?"),
        }
    }
}

/// Generate a random sequence of operations that we'll perform.
/// 
/// Flag determines which types of operation we allow on the data-structure.
/// The split is approximately equal among the operations we allow.
fn generate_random_operations(nop: usize, tid: usize, readonly: bool, writeonly: bool, readwrite: bool) -> Vec<Op> {
    let mut orng = thread_rng();
    let mut arng = thread_rng();

    let mut ops = Vec::with_capacity(nop);
    for _i in 0..nop {
        let op: usize = orng.gen();

        match (readonly, writeonly, readwrite) {
            (true, true, true) => match op % 3 {
                0 => ops.push(Op::ReadOnly(tid, arng.gen(), arng.gen())),
                1 => ops.push(Op::WriteOnly(tid, arng.gen(), arng.gen())),
                2 => ops.push(Op::ReadWrite(tid, arng.gen(), arng.gen())),
                _ => ops.push(Op::Invalid),
            },
            (false, true, true) => match op % 2 {
                0 => ops.push(Op::WriteOnly(tid, arng.gen(), arng.gen())),
                1 => ops.push(Op::ReadWrite(tid, arng.gen(), arng.gen())),
                _ => ops.push(Op::Invalid),
            },
            (true, true, false) => match op % 2 {
                0 => ops.push(Op::ReadOnly(tid, arng.gen(), arng.gen())),
                1 => ops.push(Op::WriteOnly(tid, arng.gen(), arng.gen())),
                _ => ops.push(Op::Invalid),
            },
            (true, false, true) => match op % 2 {
                0 => ops.push(Op::ReadOnly(tid, arng.gen(), arng.gen())),
                1 => ops.push(Op::ReadWrite(tid, arng.gen(), arng.gen())),
                _ => ops.push(Op::Invalid),
            },
            (true, false, false) => ops.push(Op::ReadOnly(tid, arng.gen(), arng.gen())),
            (false, true, false) => ops.push(Op::WriteOnly(tid, arng.gen(), arng.gen())),
            (false, false, true) => ops.push(Op::ReadWrite(tid, arng.gen(), arng.gen())),
            (false, false, false) => panic!("no operations selected"),
        };
    }

    ops
}

/// Compare a synthetic benchmark against a single-threaded implementation.
fn synthetic_single_threaded(c: &mut Criterion) {
    // How many operations per iteration
    const NOP: usize = 1_000;
    // Size of the log.
    const LOG_SIZE_BYTES: usize = 2 * 1024 * 1024 * 1024;

    let ops = generate_random_operations(NOP, 0, false, false, true);
    mkbench::baseline_comparison::<AbstractDataStructure>(c, "synthetic", ops, LOG_SIZE_BYTES);
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = synthetic_single_threaded,
);

criterion_main!(benches);
