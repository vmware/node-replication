// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Defines all default criterion benchmarks we run.
#![allow(unused)]

#[macro_use]
extern crate criterion;
#[macro_use]
extern crate log;
extern crate zipf;

use node_replication::Dispatch;
use rand::distributions::Distribution;
use rand::{Rng, RngCore};
use zipf::ZipfDistribution;

mod mkbench;
mod utils;

mod hashmap;
mod nop;
mod stack;
mod synthetic;

use utils::Operation;

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
                    match op {
                        Operation::ReadOperation(o) => {
                            replica.execute_ro(*o, rid);
                        }
                        Operation::WriteOperation(o) => {
                            replica.execute(*o, rid);
                        }
                    }
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
                    match op {
                        Operation::ReadOperation(mut o) => {
                            o.set_tid(cid as usize);
                            replica.execute_ro(o, rid);
                        }
                        Operation::WriteOperation(mut o) => {
                            o.set_tid(cid as usize);
                            replica.execute(o, rid);
                        }
                    }

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

/// Compare a replicated hashmap against a single-threaded implementation.
fn hashmap_single_threaded(c: &mut Criterion) {
    env_logger::try_init();

    // How many operations per iteration
    const NOP: usize = 1_000;
    // Size of the log.
    const LOG_SIZE_BYTES: usize = 2 * 1024 * 1024;
    // Biggest key in the hash-map
    const KEY_SPACE: usize = 10_000;
    // Key distribution
    const UNIFORM: &'static str = "uniform";
    //const SKEWED: &'static str = "skewed";
    // Read/Write ratio
    let write_ratio = 10; //% out of 100

    let ops = hashmap::generate_operations(NOP, write_ratio, KEY_SPACE, UNIFORM);
    mkbench::baseline_comparison::<hashmap::NrHashMap>(c, "hashmap", ops, LOG_SIZE_BYTES);
}

/// Compare scale-out behaviour of synthetic data-structure.
fn hashmap_scale_out(c: &mut Criterion) {
    env_logger::try_init();

    // How many operations per iteration
    const NOP: usize = 10_000;
    // Biggest key in the hash-map
    const KEY_SPACE: usize = 10_000;
    // Key distribution
    const UNIFORM: &'static str = "uniform";
    //const SKEWED: &'static str = "skewed";
    // Read/Write ratio

    mkbench::ScaleBenchBuilder::<hashmap::NrHashMap>::new(vec![Operation::ReadOperation(
        hashmap::OpRd::Get(1),
    )])
    .machine_defaults()
    .configure(
        c,
        "hashmap-scaleout",
        |cid, rid, _log, replica, ops, _batch_size| {
            let mut o = vec![];
            let mut t_rng = rand::thread_rng();
            let mut zipf = ZipfDistribution::new(KEY_SPACE, 1.03).unwrap();
            let distribution = UNIFORM;
            let writers = 0;

            let skewed = distribution == "skewed";
            let id = if skewed {
                zipf.sample(&mut t_rng) as u64
            } else {
                // uniform
                t_rng.gen_range(0, KEY_SPACE as u64)
            };

            if cid < writers {
                let op = hashmap::OpWr::Put(id, t_rng.next_u64());
                replica.execute(op, rid);
            } else {
                let op = hashmap::OpRd::Get(id);
                replica.execute_ro(op, rid);
            };

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
        },
    );
}

/// Compare scale-out behaviour of log.
fn log_scale_bench(c: &mut Criterion) {
    env_logger::try_init();

    /// Benchmark #operations per iteration
    const NOP: usize = 50_000;
    /// Log size (needs to be big as we don't have GC in this case but high tput)
    const LOG_SIZE_BYTES: usize = 4 * 1024 * 1024 * 1024;

    let mut operations = Vec::new();
    for e in 0..NOP {
        operations.push(Operation::WriteOperation(e));
    }

    mkbench::ScaleBenchBuilder::<nop::Nop>::new(operations)
        .machine_defaults()
        .log_size(LOG_SIZE_BYTES)
        .add_batch(8)
        .reset_log()
        .disable_sync()
        .configure(
            c,
            "log-append",
            |_cid, rid, log, _replica, ops, batch_size| {
                let mut op_batch: Vec<usize> = Vec::with_capacity(8);
                for batch_op in ops.rchunks(batch_size) {
                    op_batch.clear();
                    for op in batch_op {
                        match op {
                            Operation::WriteOperation(o) => op_batch.push(*o),
                            _ => unreachable!(),
                        }
                    }
                    let _r = log.append(
                        &op_batch[..],
                        rid,
                        |_o: <nop::Nop as Dispatch>::WriteOperation, _i: usize| {},
                    );
                }
            },
        );
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = hashmap_single_threaded, hashmap_scale_out,
              stack_single_threaded, stack_scale_out,
              synthetic_single_threaded, synthetic_scale_out,
              log_scale_bench
);

criterion_main!(benches);
