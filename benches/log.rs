// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Defines all default criterion benchmarks we run.
#![allow(unused)]
#![feature(test)]

#[macro_use]
extern crate log;
extern crate zipf;

use node_replication::Dispatch;
use rand::distributions::Distribution;
use rand::{Rng, RngCore};
use zipf::ZipfDistribution;

mod mkbench;
mod utils;

use utils::benchmark::*;
use utils::Operation;

extern crate jemallocator;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, Default, Eq, PartialEq, Copy, Clone)]
pub struct Nop(usize);

impl Dispatch for Nop {
    type ReadOperation = ();
    type WriteOperation = usize;
    type Response = ();
    type ResponseError = ();

    fn dispatch(&self, _op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        unreachable!();
    }

    fn dispatch_mut(
        &mut self,
        _op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        unreachable!();
    }
}

/// Compare scale-out behaviour of log.
fn log_scale_bench(c: &mut TestHarness) {
    env_logger::try_init();

    /// Log size (needs to be big as we don't have GC in this case but high tput)
    const LOG_SIZE_BYTES: usize = 12 * 1024 * 1024 * 1024;

    mkbench::ScaleBenchBuilder::new()
        .machine_defaults()
        .log_size(LOG_SIZE_BYTES)
        .add_batch(8)
        .reset_log()
        .disable_sync()
        .configure::<Nop>(
            c,
            "log-append",
            |_cid, rid, log, _replica, batch_size, rng| {
                let mut op_batch: Vec<usize> = Vec::with_capacity(batch_size);
                for i in 0..batch_size {
                    op_batch.push(rng.gen::<usize>());
                }

                let _r = log.append(
                    &op_batch[..],
                    rid,
                    |_o: <Nop as Dispatch>::WriteOperation, _i: usize| {},
                );
            },
        );
}

fn main() {
    let mut harness = TestHarness::new(std::time::Duration::from_secs(3));
    log_scale_bench(&mut harness);
}
