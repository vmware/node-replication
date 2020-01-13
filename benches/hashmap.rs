// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Defines a hash-map that can be replicated.

use std::cell::RefCell;
use std::collections::HashMap;

use rand::distributions::Distribution;
use rand::seq::SliceRandom;
use rand::{thread_rng, RngCore};
use zipf::ZipfDistribution;

use node_replication::Dispatch;

/// Operations we can perform on the stack.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Op {
    /// Add an item to the hash-map.
    Put(u64, u64),
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

    pub fn get(&mut self, key: u64) -> Option<u64> {
        self.storage.get(&key).map(|v| *v)
    }
}

impl Default for NrHashMap {
    /// Return a dummy hash-map with initial capacity of 50k elements.
    fn default() -> NrHashMap {
        NrHashMap {
            storage: HashMap::with_capacity(5_000_000),
        }
    }
}

impl Dispatch for NrHashMap {
    type Operation = Op;
    type Response = Option<u64>;
    type ResponseError = ();

    /// Implements how we execute operation from the log against our local stack
    fn dispatch(&mut self, op: Self::Operation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            Op::Put(key, val) => {
                self.put(key, val);
                Ok(None)
            }
            Op::Get(key) => return Ok(self.get(key)),
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
) -> Vec<Op> {
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
            ops.push(Op::Put(id, t_rng.next_u64()));
        } else {
            ops.push(Op::Get(id));
        }
    }

    ops.shuffle(&mut t_rng);
    ops
}
