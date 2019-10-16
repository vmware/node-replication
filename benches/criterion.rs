// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Defines all default criterion benchmarks we run.
#![allow(unused)]

#[macro_use]
extern crate criterion;
#[macro_use]
extern crate log;

mod mkbench;
mod utils;

mod nop;
mod stack;
mod synthetic;

use criterion::{criterion_group, criterion_main, Criterion};

/// Compare against a stack with and without a log in-front.
fn stack_single_threaded(c: &mut Criterion) {
    env_logger::try_init();

    // Benchmark operations per iteration
    const NOP: usize = 1_000;
    // Log size
    const LOG_SIZE_BYTES: usize = 2 * 1024 * 1024;

    let ops = stack::generate_operations(NOP);
    mkbench::baseline_comparison::<stack::Stack>(c, "stack", ops, LOG_SIZE_BYTES);
}

/// Compare scalability of a node-replicated stack.
fn stack_scale_out(c: &mut Criterion) {
    env_logger::try_init();

    // How many operations per iteration
    const NOP: usize = 10_000;
    // Operations to perform
    let ops = stack::generate_operations(NOP);

    mkbench::ScaleBenchBuilder::<stack::Stack>::new(ops)
        .machine_defaults()
        // ReplicaStrategy::Socket currently doesn't finish with a small log size, investigate with:
        // $ RUST_TEST_THREADS=1 cargo bench --bench criterion -- 'stack-scaleout/RS=Socket TM=Sequential BS=1/32'
        //.replica_strategy(mkbench::ReplicaStrategy::Socket)
        //.log_size(1024 * 1024 * 1024 * 5)
        .configure(
            c,
            "stack-scaleout",
            |_cid, rid, _log, replica, ops, _batch_size| {
                let mut o = vec![];
                for op in ops {
                    replica.execute(*op, rid);
                    let mut i = 1;
                    while replica.get_responses(rid, &mut o) == 0 {
                        if i % mkbench::WARN_THRESHOLD == 0 {
                            log::warn!(
                                "{:?} Waiting too long for get_responses",
                                std::thread::current().id()
                            );
                        }
                        i += 1;
                    }
                    o.clear();
                }
            },
        );
}

/// Compare a synthetic benchmark against a single-threaded implementation.
fn synthetic_single_threaded(c: &mut Criterion) {
    env_logger::try_init();

    // How many operations per iteration
    const NOP: usize = 1_000;
    // Size of the log.
    const LOG_SIZE_BYTES: usize = 2 * 1024 * 1024;

    let ops = synthetic::generate_operations(NOP, 0, false, false, true);
    mkbench::baseline_comparison::<synthetic::AbstractDataStructure>(
        c,
        "synthetic",
        ops,
        LOG_SIZE_BYTES,
    );
}

/// Compare scale-out behaviour of synthetic data-structure.
fn synthetic_scale_out(c: &mut Criterion) {
    env_logger::try_init();

    // How many operations per iteration
    const NOP: usize = 10_000;
    // Operations to perform
    let ops = synthetic::generate_operations(NOP, 0, false, false, true);

    mkbench::ScaleBenchBuilder::<synthetic::AbstractDataStructure>::new(ops)
        .machine_defaults()
        .configure(
            c,
            "synthetic-scaleout",
            |cid, rid, _log, replica, ops, _batch_size| {
                let mut o = vec![];
                for op in ops {
                    let mut op = *op;
                    op.set_tid(cid as usize);
                    replica.execute(op, rid);
                    let mut i = 1;
                    while replica.get_responses(rid, &mut o) == 0 {
                        if i % mkbench::WARN_THRESHOLD == 0 {
                            log::warn!(
                                "{:?} Waiting too long for get_responses",
                                std::thread::current().id()
                            );
                        }
                        i += 1;
                    }
                    o.clear();
                }
            },
        );
}

/// Compare scale-out behaviour of log.
fn log_scale_bench(c: &mut Criterion) {
    env_logger::try_init();

    /// Benchmark #operations per iteration
    const NOP: usize = 50_000;
    /// Log size (needs to be big as we don't have GC in this case but high tput)
    const LOG_SIZE_BYTES: usize = 5 * 1024 * 1024 * 1024;

    let mut operations = Vec::new();
    for e in 0..NOP {
        operations.push(e);
    }

    mkbench::ScaleBenchBuilder::<nop::Nop>::new(operations)
        .machine_defaults()
        .log_size(LOG_SIZE_BYTES)
        .add_batch(8)
        .configure(
            c,
            "log-append",
            |_cid, rid, log, _replica, ops, batch_size| {
                for batch_op in ops.rchunks(batch_size) {
                    let _r = log.append(batch_op, rid);
                }
            },
        );
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = stack_single_threaded, stack_scale_out, synthetic_single_threaded, synthetic_scale_out, log_scale_bench
);

criterion_main!(benches);
