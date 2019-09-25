// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use criterion::{criterion_group, criterion_main, Criterion, ParameterizedBenchmark, Throughput};
use node_replication::log::Log;
///! A benchmark that evaluates the append performance (in terms of throughput ops/s)
///! of the log by varying the batch size and the amount of threads contending on the log.
use std::sync::Arc;

mod utils;

/// Benchmark 500k operations per iteration
const NOP: usize = 50_000;

/// Use a 2 GiB log size
const LOG_SIZE_BYTES: usize = 2 * 1024 * 1024 * 1024;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
struct Threads(usize);

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
struct BatchSize(usize);

fn scale_log(log: &Arc<Log<usize>>, ops: &Arc<Vec<usize>>, batch: usize) {
    for batch_op in ops.rchunks(batch) {
        let _r = log.append(batch_op);
        //assert!(r.is_some());
    }
}

fn log_scale_bench(c: &mut Criterion) {
    env_logger::init();
    utils::disable_dvfs();

    let mut operations = Vec::new();
    for e in 0..NOP {
        operations.push(e);
    }

    let operations = Arc::new(operations);
    let elements = operations.len();

    let configurations: Vec<(Threads, BatchSize)> = vec![
        (Threads(1), BatchSize(1)),
        (Threads(1), BatchSize(8)),
        (Threads(2), BatchSize(1)),
        (Threads(2), BatchSize(8)),
        (Threads(4), BatchSize(1)),
        (Threads(4), BatchSize(8)),
    ];

    let log = Arc::new(Log::<usize>::new(LOG_SIZE_BYTES));

    c.bench(
        "log",
        ParameterizedBenchmark::new(
            "append",
            move |b, (threads, batch)| {
                b.iter_custom(|iters| {
                    utils::generic_log_bench(
                        iters,
                        threads.0,
                        batch.0,
                        &log,
                        &operations,
                        scale_log,
                    )
                })
            },
            configurations,
        )
        .throughput(move |(t, _b)| Throughput::Elements((t.0 * elements) as u64)),
    );
}

criterion_group!(logscale, log_scale_bench);
criterion_main!(logscale);
