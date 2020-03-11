// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Defines a hash-map that can be replicated.
#![feature(test)]

use std::cell::RefCell;
use std::collections::HashMap;

use rand::distributions::Distribution;
use rand::seq::SliceRandom;
use rand::{thread_rng, RngCore};
use zipf::ZipfDistribution;

use node_replication::Dispatch;

mod mkbench;
mod utils;

use utils::benchmark::*;
use utils::Operation;

/// Operations we can perform on the stack.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum OpWr {
    /// Add an item to the hash-map.
    Put(u64, u64),
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum OpRd {
    /// Get item from the hash-map.
    Get(u64),
}

/// Single-threaded implementation of the stack
///
/// We just use a vector.
#[derive(Debug, Clone)]
pub struct NrHashMap {
    storage: HashMap<u64, u64>,
}

impl NrHashMap {
    pub fn put(&mut self, key: u64, val: u64) {
        self.storage.insert(key, val);
    }

    pub fn get(&self, key: u64) -> Option<u64> {
        self.storage.get(&key).map(|v| *v)
    }
}

impl Default for NrHashMap {
    /// Return a dummy hash-map with initial capacity of 50M elements.
    fn default() -> NrHashMap {
        let capacity = 5_000_000;
        let mut storage = HashMap::with_capacity(capacity);
        for i in 0..capacity {
            storage.insert(i as u64, (i + 1) as u64);
        }
        NrHashMap { storage }
    }
}

impl Dispatch for NrHashMap {
    type ReadOperation = OpRd;
    type WriteOperation = OpWr;
    type Response = Option<u64>;
    type ResponseError = ();

    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpRd::Get(key) => return Ok(self.get(key)),
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(
        &mut self,
        op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpWr::Put(key, val) => {
                self.put(key, val);
                Ok(None)
            }
        }
    }
}

/// Generate a random sequence of operations
///
/// # Arguments
///  - `nop`: Number of operations to generate
///  - `write`: true will Put, false will generate Get sequences
///  - `span`: Maximum key
///  - `distribution`: Supported distribution 'uniform' or 'skewed'
pub fn generate_operations(
    nop: usize,
    write_ratio: usize,
    span: usize,
    distribution: &'static str,
) -> Vec<Operation<OpRd, OpWr>> {
    assert!(distribution == "skewed" || distribution == "uniform");

    use rand::Rng;
    let mut ops = Vec::with_capacity(nop);

    let skewed = distribution == "skewed";
    let mut t_rng = rand::thread_rng();
    let mut zipf = ZipfDistribution::new(span, 1.03).unwrap();

    for idx in 0..nop {
        let id = if skewed {
            zipf.sample(&mut t_rng) as u64
        } else {
            // uniform
            t_rng.gen_range(0, span as u64)
        };

        if idx % 100 < write_ratio {
            ops.push(Operation::WriteOperation(OpWr::Put(id, t_rng.next_u64())));
        } else {
            ops.push(Operation::ReadOperation(OpRd::Get(id)));
        }
    }

    ops.shuffle(&mut t_rng);
    ops
}

/// Compare a replicated hashmap against a single-threaded implementation.
fn hashmap_single_threaded(c: &mut TestHarness) {
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
fn hashmap_scale_out(c: &mut TestHarness) {
    env_logger::try_init();

    // How many operations per iteration
    const NOP: usize = 515_000;
    // Biggest key in the hash-map
    const KEY_SPACE: usize = 5_000_000;
    // Key distribution
    const UNIFORM: &'static str = "uniform";
    //const SKEWED: &'static str = "skewed";
    // Read/Write ratio
    const WRITE_RATIO: usize = 0; //% out of 100

    let ops = crate::hashmap::generate_operations(NOP, WRITE_RATIO, KEY_SPACE, UNIFORM);
    mkbench::ScaleBenchBuilder::<hashmap::NrHashMap>::new(ops)
        .machine_defaults()
        .configure(
            c,
            "hashmap-scaleout",
            |cid, rid, _log, replica, ops, _batch_size| {
                for op in ops {
                    match op {
                        Operation::ReadOperation(op) => {
                            replica.execute_ro(*op, rid).unwrap();
                        }
                        Operation::WriteOperation(op) => {
                            replica.execute(*op, rid).unwrap();
                        }
                    }
                }
            },
        );
}

fn main() {
    let mut harness = Default::default();

    hashmap_single_threaded(&mut harness);
    hashmap_scale_out(&mut harness);
}
