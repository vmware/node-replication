// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

///! A benchmark that evaluates the append performance (in terms of throughput ops/s)
///! of the log by varying the batch size and the amount of threads contending on the log.
use criterion::{criterion_group, criterion_main, Criterion};
use node_replication::Dispatch;

mod mkbench;
mod utils;


#[derive(Debug, Default, Eq, PartialEq, Copy, Clone)]
struct Nop(usize);

impl Dispatch for Nop {
    type Operation = usize;
    type Response = ();

    fn dispatch(&self, _op: Self::Operation) -> Self::Response {
        unreachable!()
    }
}

fn log_scale_bench(c: &mut Criterion) {
    env_logger::init();

    /// Benchmark #operations per iteration
    const NOP: usize = 50_000;

    /// Use a 2 GiB log size
    const LOG_SIZE_BYTES: usize = 2 * 1024 * 1024 * 1024;

    let mut operations = Vec::new();
    for e in 0..NOP {
        operations.push(e);
    }

    mkbench::ScaleBenchBuilder::<Nop>::new(operations)
        .log_size(LOG_SIZE_BYTES)
        .machine_defaults()
        .add_batch(8)
        .configure(
            c,
            "log-append",
            |_cid, _rid, log, _replica, ops, batch_size| {
                for batch_op in ops.rchunks(batch_size) {
                    let _r = log.append(batch_op);
                    //assert!(r.is_some());
                }
            },
        );
}
criterion_group!(
    name = logscale;
    config = Criterion::default().sample_size(10);
    targets = log_scale_bench
);
criterion_main!(logscale);
