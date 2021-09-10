// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Defines all default criterion benchmarks we run.
#![allow(unused)]
#![feature(test)]
#![feature(bench_black_box)]

#[macro_use]
extern crate log;
extern crate zipf;

use std::sync::Arc;

use node_replication::Dispatch;
use node_replication::Log;
use node_replication::Replica;
use rand::distributions::Distribution;
use rand::{Rng, RngCore};

use zipf::ZipfDistribution;

mod mkbench;
mod utils;

use mkbench::{ReplicaStrategy, ReplicaTrait, ThreadMapping};

use utils::benchmark::*;
use utils::Operation;

#[derive(Debug, Default, Eq, PartialEq, Copy, Clone)]
pub struct Nop(usize);

impl Dispatch for Nop {
    type ReadOperation = ();
    type WriteOperation = usize;
    type Response = ();

    fn dispatch(&self, _op: Self::ReadOperation) -> Self::Response {
        unreachable!();
    }

    fn dispatch_mut(&mut self, _op: Self::WriteOperation) -> Self::Response {
        unreachable!();
    }
}

/// Compare scale-out behaviour of log.
fn log_scale_bench(c: &mut TestHarness) {
    env_logger::try_init();

    /// Log size (needs to be big as we don't have GC in this case but high tput)
    const LOG_SIZE_BYTES: usize = 12 * 1024 * 1024 * 1024;

    /// Benchmark #operations per iteration
    const NOP: usize = 50_000;

    let mut operations = Vec::new();
    for e in 0..NOP {
        operations.push(Operation::WriteOperation(e));
    }

    mkbench::ScaleBenchBuilder::<Replica<Nop>>::new(operations)
        .thread_defaults()
        .thread_mapping(ThreadMapping::Sequential)
        .replica_strategy(ReplicaStrategy::One)
        .log_size(LOG_SIZE_BYTES)
        .log_strategy(mkbench::LogStrategy::One)
        .update_batch(8)
        .reset_log()
        .disable_sync()
        .configure(
            c,
            "log-append",
            |_cid, rid, log, replica, ops, nop, index, batch_size| {
                for i in 0..batch_size {
                    let op = &ops[(index + i) % nop];
                    match op {
                        Operation::WriteOperation(o) => {
                            let _r = log.append(&vec![*o], rid.id(), |_o, _i| {});
                        }
                        _ => unreachable!(),
                    }
                }
            },
        );
}

fn main() {
    let mut harness = TestHarness::new(std::time::Duration::from_secs(3));
    log_scale_bench(&mut harness);
}
