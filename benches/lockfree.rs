// Copyright © VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Defines a hash-map that can be replicated.
#![feature(test)]
#![feature(get_mut_unchecked)]
#![feature(bench_black_box)]

use std::fmt::Debug;
use std::marker::Sync;

use rand::seq::SliceRandom;
use rand::{Rng, RngCore};
//use zipf::ZipfDistribution;

use cnr::{Dispatch, LogMapper, Replica};

//mod lockfree_comparisons;
mod lockfree_partitioned;
mod mkbench;
mod utils;

//use lockfree_comparisons::*;
use mkbench::ReplicaTrait;
use utils::benchmark::*;
use utils::topology::{MachineTopology, ThreadMapping};
use utils::Operation;

/// The initial amount of entries all Hashmaps are initialized with
#[cfg(feature = "smokebench")]
pub const INITIAL_CAPACITY: usize = 1 << 22; // ~ 4M
#[cfg(not(feature = "smokebench"))]
pub const INITIAL_CAPACITY: usize = 1 << 26; // ~ 67M

// Biggest key in the hash-map
#[cfg(feature = "smokebench")]
pub const KEY_SPACE: usize = 5_000_000;
#[cfg(not(feature = "smokebench"))]
pub const KEY_SPACE: usize = 50_000_000;

// Key distribution for all hash-maps [uniform|skewed]
pub const UNIFORM: &'static str = "uniform";

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum QueueWr {
    Push(u64),
    Pop,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum QueueRd {
    Len,
}

impl LogMapper for QueueRd {
    fn hash(&self, _nlogs: usize, logs: &mut Vec<usize>) {
        logs.clear();
        logs.push(0);
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum QueueConcurrent {
    Push(u64),
    Len,
    Pop,
}

impl LogMapper for QueueConcurrent {
    fn hash(&self, _nlogs: usize, logs: &mut Vec<usize>) {
        logs.clear();
        logs.push(0);
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum SkipListConcurrent {
    Get(u64),
}

impl LogMapper for SkipListConcurrent {
    fn hash(&self, nlogs: usize, logs: &mut Vec<usize>) {
        logs.clear();
        match self {
            SkipListConcurrent::Get(k) => logs.push(*k as usize % nlogs),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum OpWr {
    Push(u64, u64),
}

impl LogMapper for OpWr {
    fn hash(&self, nlogs: usize, logs: &mut Vec<usize>) {
        logs.clear();
        match self {
            OpWr::Push(k, _v) => logs.push(*k as usize % nlogs),
        }
    }
}

pub fn generate_qops_concurrent(
    nop: usize,
    write_ratio: usize,
    span: usize,
) -> Vec<Operation<QueueConcurrent, ()>> {
    let mut ops = Vec::with_capacity(nop);

    let mut t_rng = rand::thread_rng();

    for idx in 0..nop {
        let _id = t_rng.gen_range(0, span as u64);

        if idx % 100 < write_ratio {
            if idx % 2 == 0 {
                ops.push(Operation::ReadOperation(QueueConcurrent::Push(
                    t_rng.next_u64(),
                )));
            } else {
                ops.push(Operation::ReadOperation(QueueConcurrent::Pop));
            }
        } else {
            ops.push(Operation::ReadOperation(QueueConcurrent::Len));
        }
    }

    ops.shuffle(&mut t_rng);
    ops
}

pub fn generate_sops_concurrent(
    nop: usize,
    write_ratio: usize,
    span: usize,
) -> Vec<Operation<SkipListConcurrent, OpWr>> {
    let mut ops = Vec::with_capacity(nop);

    let mut t_rng = rand::thread_rng();

    for idx in 0..nop {
        let _id = t_rng.gen_range(0, span as u64);

        if idx % 100 < write_ratio {
            ops.push(Operation::WriteOperation(OpWr::Push(
                t_rng.next_u64(),
                t_rng.next_u64(),
            )));
        } else {
            ops.push(Operation::ReadOperation(SkipListConcurrent::Get(
                t_rng.next_u64(),
            )));
        }
    }

    ops.shuffle(&mut t_rng);
    ops
}

pub fn generate_sops_partitioned_concurrent(
    nop: usize,
    write_ratio: usize,
    span: usize,
) -> Vec<Operation<SkipListConcurrent, OpWr>> {
    let mut ops = Vec::with_capacity(nop);

    let mut t_rng = rand::thread_rng();

    for idx in 0..nop {
        let _id = t_rng.gen_range(0, span as u64);

        if idx % 100 < write_ratio {
            ops.push(Operation::WriteOperation(OpWr::Push(
                t_rng.next_u64() % 25_000_000,
                t_rng.next_u64(),
            )));
        } else {
            ops.push(Operation::ReadOperation(SkipListConcurrent::Get(
                t_rng.next_u64() % 25_000_000,
            )));
        }
    }

    ops.shuffle(&mut t_rng);
    ops
}

// Number of operation for test-harness.
#[cfg(feature = "smokebench")]
pub const NOP: usize = 2_500_000;
#[cfg(not(feature = "smokebench"))]
pub const NOP: usize = 25_000_000;

fn concurrent_ds_scale_out<T: Sized>(
    c: &mut TestHarness,
    name: &str,
    write_ratio: usize,
    mkops: Box<
        dyn Fn() -> Vec<Operation<<T as Dispatch>::ReadOperation, <T as Dispatch>::WriteOperation>>,
    >,
) where
    T: Dispatch<ReadOperation = SkipListConcurrent> + Sized,
    T: Dispatch<WriteOperation = OpWr> + Sized,
    T: 'static,
    T: Dispatch + Sync + Default + Send,
    <T as Dispatch>::ReadOperation: Send + Sync + Debug + Copy,
    <T as Dispatch>::WriteOperation: Send + Sync + Debug + Copy,
    <T as Dispatch>::Response: Send + Sync + Debug,
{
    let bench_name = format!("{}-scaleout-wr{}", name, write_ratio);

    mkbench::ScaleBenchBuilder::<lockfree_partitioned::ConcurrentDs<T>>::new(mkops())
        .thread_defaults()
        .replica_strategy(mkbench::ReplicaStrategy::One) // Can only be One
        .update_batch(128)
        .thread_mapping(ThreadMapping::Interleave)
        .log_strategy(mkbench::LogStrategy::One)
        .configure(
            c,
            &bench_name,
            |_cid, rid, _log, replica, op, _batch_size| match op {
                Operation::ReadOperation(op) => {
                    replica.exec_ro(*op, rid);
                }
                Operation::WriteOperation(op) => {
                    replica.exec(*op, rid);
                }
            },
        );
}

fn concurrent_ds_nr_scale_out<R>(c: &mut TestHarness, name: &str, write_ratio: usize)
where
    R: ReplicaTrait + Send + Sync + 'static,
    R::D: Send,
    R::D: Dispatch<ReadOperation = SkipListConcurrent>,
    R::D: Dispatch<WriteOperation = OpWr>,
    <R::D as Dispatch>::WriteOperation: Send + Sync,
    <R::D as Dispatch>::ReadOperation: Send + Sync,
    <R::D as Dispatch>::Response: Sync + Send + Debug,
{
    let ops = generate_sops_concurrent(NOP, write_ratio, KEY_SPACE);
    let topology = MachineTopology::new();
    let sockets = topology.sockets();
    let cores_on_socket = topology.cpus_on_socket(sockets[0]).len();

    let increment = if topology.cores() > 120 { 8 } else { 4 };

    let mut nlog = 0;
    while nlog <= cores_on_socket {
        let logs = if nlog == 0 { 1 } else { nlog };
        let bench_name = format!("{}{}-scaleout-wr{}", name, logs, write_ratio);

        mkbench::ScaleBenchBuilder::<R>::new(ops.clone())
            .thread_defaults()
            .replica_strategy(mkbench::ReplicaStrategy::Socket)
            .update_batch(128)
            .thread_mapping(ThreadMapping::Interleave)
            .log_strategy(mkbench::LogStrategy::Custom(logs))
            .configure(
                c,
                &bench_name,
                |_cid, rid, _log, replica, op, _batch_size| match op {
                    Operation::ReadOperation(op) => {
                        replica.exec_ro(*op, rid);
                    }
                    Operation::WriteOperation(op) => {
                        /*let op = match op {
                            OpWr::Push(k, v) => {
                                let key = *k + (rid.0 * 25_000_000) as u64;
                                OpWr::Push(key, *v)
                            }
                        };*/
                        replica.exec(*op, rid);
                    }
                },
            );
        nlog += increment;
    }
}

fn main() {
    let _r = env_logger::try_init();
    if cfg!(feature = "smokebench") {
        log::warn!("Running with feature 'smokebench' may not get the desired results");
    }

    utils::disable_dvfs();

    let mut harness = Default::default();
    let write_ratios = vec![0, 10, 80, 100];

    for write_ratio in write_ratios.into_iter() {
        /*concurrent_ds_scale_out::<SegQueueWrapper>(
            &mut harness,
            "segqueue",
            write_ratio,
            Box::new(move || generate_qops_concurrent(NOP, write_ratio, KEY_SPACE)),
        );*/

        /*concurrent_ds_scale_out::<SkipListWrapper>(
            &mut harness,
            "skiplist",
            write_ratio,
            Box::new(move || generate_sops_concurrent(NOP, write_ratio, KEY_SPACE)),
        );*/

        concurrent_ds_scale_out::<lockfree_partitioned::SkipListWrapper>(
            &mut harness,
            "skiplist-partinput",
            write_ratio,
            Box::new(move || generate_sops_concurrent(NOP, write_ratio, KEY_SPACE)),
        );

        concurrent_ds_nr_scale_out::<Replica<lockfree_partitioned::SkipListWrapper>>(
            &mut harness,
            "skiplist-mlnr",
            write_ratio,
        );
    }
}
