// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

///! A benchmark that evaluates the append performance (in terms of throughput ops/s)
///! of the log by varying the batch size and the amount of threads contending on the log.

use criterion::{criterion_group, criterion_main, Criterion, ParameterizedBenchmark, Throughput};
use node_replication::log::Log;
use node_replication::Dispatch;

use std::sync::Arc;

mod utils;
mod mkbench;

/// Benchmark 500k operations per iteration
const NOP: usize = 50_000;

/// Use a 2 GiB log size
const LOG_SIZE_BYTES: usize = 2 * 1024 * 1024 * 1024;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
struct Threads(usize);

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
struct BatchSize(usize);

#[derive(Debug, Default, Eq, PartialEq, Copy, Clone)]
struct Nop(usize);

impl Dispatch for Nop {
    type Operation = usize;
    type Response = ();

    fn dispatch(&self, op: Self::Operation) -> Self::Response {
        unreachable!()
    }
}

fn log_scale_bench(c: &mut Criterion) {
    env_logger::init();

    let mut operations = Vec::new();
    for e in 0..NOP {
        operations.push(e);
    }

    mkbench::ScaleBenchBuilder::<Nop>::new(operations)
        .log_size(LOG_SIZE_BYTES)
        .replica_strategy(mkbench::ReplicaStrategy::One)
        .thread_mapping(mkbench::ThreadMapping::Sequential)
        .threads(1)
        .threads(2)
        .threads(4)
        .add_batch(8)
        .configure(c, "log-append", |cid, rid, log, replica, ops, batch_size| {
            for batch_op in ops.rchunks(batch_size) {
                let _r = log.append(batch_op);
                //assert!(r.is_some());
            }
        });
}

criterion_group!(logscale, log_scale_bench);
criterion_main!(logscale);
