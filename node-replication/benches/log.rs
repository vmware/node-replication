// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Defines all default criterion benchmarks we run.
#![allow(unused)]
#![feature(generic_associated_types)]

use std::sync::Arc;

use bench_utils::benchmark::TestHarness;
use bench_utils::mkbench::{self, ReplicaStrategy, ThreadMapping};
use bench_utils::Operation;
use node_replication::nr::log::Log;
use node_replication::nr::replica::Replica;
use node_replication::nr::{Dispatch, NodeReplicated};
use rand::distributions::Distribution;
use rand::{Rng, RngCore};
use zipf::ZipfDistribution;

#[derive(Debug, Default, Eq, PartialEq, Copy, Clone)]
pub struct Nop(usize);

impl Dispatch for Nop {
    type ReadOperation<'rop> = ();
    type WriteOperation = usize;
    type Response = ();

    fn dispatch<'rop>(&self, _op: Self::ReadOperation<'rop>) -> Self::Response {
        ()
    }

    fn dispatch_mut(&mut self, _op: Self::WriteOperation) -> Self::Response {
        ()
    }
}

/// Compare scale-out behaviour of log.
fn log_scale_bench(c: &mut TestHarness) {
    env_logger::try_init();

    /// Log size (needs to be big as we don't want GC but max tput)
    const LOG_SIZE_BYTES: usize = 12 * 1024 * 1024 * 1024;

    /// Benchmark #operations per iteration
    const NOP: usize = 50_000;

    let mut operations = Vec::new();
    for e in 0..NOP {
        operations.push(Operation::WriteOperation(e));
    }

    mkbench::ScaleBenchBuilder::<NodeReplicated<Nop>>::new(operations)
        .thread_defaults()
        .thread_mapping(ThreadMapping::Sequential)
        .replica_strategy(ReplicaStrategy::One)
        .log_strategy(mkbench::LogStrategy::One)
        .update_batch(8)
        .configure(c, "log-append", |_cid, tkn, ds, op, batch_size| match op {
            Operation::WriteOperation(o) => {
                for _i in 0..batch_size {
                    let _r = ds.execute_mut(0xffff_ffff_ffff_ffff, tkn);
                }
            }
            _ => unreachable!(),
        });
}

fn main() {
    let mut harness = TestHarness::new(std::time::Duration::from_secs(3));
    log_scale_bench(&mut harness);
}
