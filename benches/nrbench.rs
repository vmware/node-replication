// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A CLI tool to execute various benchmarks configurable with command-line parameters.
#![allow(unused)]

extern crate clap;
extern crate criterion;
extern crate env_logger;

mod hashmap;
mod mkbench;
mod stack;
mod synthetic;
mod utils;

use clap::{load_yaml, App};
use mkbench::{ReplicaStrategy, ThreadMapping};
use utils::Operation;

enum Builder {
    Stack(mkbench::ScaleBenchBuilder<stack::Stack>),
    Synthetic(mkbench::ScaleBenchBuilder<synthetic::AbstractDataStructure>),
    HashMap(mkbench::ScaleBenchBuilder<hashmap::NrHashMap>),
}

impl Builder {
    fn replica_strategy(&mut self, rs: ReplicaStrategy) {
        match self {
            Builder::Stack(b) => {
                b.replica_strategy(rs);
            }
            Builder::Synthetic(b) => {
                b.replica_strategy(rs);
            }
            Builder::HashMap(b) => {
                b.replica_strategy(rs);
            }
        }
    }

    fn thread_mapping(&mut self, tm: ThreadMapping) {
        match self {
            Builder::Stack(b) => {
                b.thread_mapping(tm);
            }
            Builder::Synthetic(b) => {
                b.thread_mapping(tm);
            }
            Builder::HashMap(b) => {
                b.thread_mapping(tm);
            }
        }
    }

    fn log_size(&mut self, ls: usize) {
        match self {
            Builder::Stack(b) => {
                b.log_size(ls);
            }
            Builder::Synthetic(b) => {
                b.log_size(ls);
            }
            Builder::HashMap(b) => {
                b.log_size(ls);
            }
        }
    }

    fn threads(&mut self, t: usize) {
        match self {
            Builder::Stack(b) => {
                b.threads(t);
            }
            Builder::Synthetic(b) => {
                b.threads(t);
            }
            Builder::HashMap(b) => {
                b.threads(t);
            }
        }
    }
}

fn main() {
    env_logger::init();
    let yml = load_yaml!("nrbench.yml");

    // Cargo passes --bench to us, ignore it:
    let args = std::env::args().filter(|e| e != "--bench");
    let matches = App::from_yaml(yml).get_matches_from(args);

    // Create a builder
    let nops = usize::from_str_radix(matches.value_of("nop").unwrap(), 10).unwrap();
    let ds = matches.value_of("datastructure").unwrap_or("stack");
    let mut builder = match ds {
        "stack" => Builder::Stack(mkbench::ScaleBenchBuilder::<stack::Stack>::new(
            stack::generate_operations(nops),
        )),
        "synthetic" => Builder::Synthetic(mkbench::ScaleBenchBuilder::<
            synthetic::AbstractDataStructure,
        >::new(synthetic::generate_operations(
            nops, 0, false, false, true,
        ))),
        "hashmap" => Builder::HashMap(mkbench::ScaleBenchBuilder::<hashmap::NrHashMap>::new(
            hashmap::generate_operations(nops, 0, 10_000, "uniform"),
        )),
        _ => unreachable!("Invalid CLI argument, may be clap bug if possible_values doesn't work?"),
    };

    // Set the replica strategy
    match matches.value_of("replicas").unwrap_or("one") {
        "one" => builder.replica_strategy(ReplicaStrategy::One),
        "socket" => builder.replica_strategy(ReplicaStrategy::Socket),
        "l3" => builder.replica_strategy(ReplicaStrategy::L3),
        "l2" => builder.replica_strategy(ReplicaStrategy::L2),
        "l1" => builder.replica_strategy(ReplicaStrategy::L1),
        _ => unreachable!("Invalid CLI argument, may be clap bug if possible_values doesn't work?"),
    };

    // Set thread mapping
    match matches.value_of("mapping").unwrap_or("sequential") {
        "sequential" => builder.thread_mapping(ThreadMapping::Sequential),
        "interleave" => builder.thread_mapping(ThreadMapping::Interleave),
        _ => unreachable!("Invalid CLI argument, may be clap bug if possible_values doesn't work?"),
    };

    // Set log size
    let l = usize::from_str_radix(matches.value_of("logsz").unwrap(), 10).unwrap();
    builder.log_size(l);

    let ts: Vec<&str> = matches.values_of("threads").unwrap().collect();

    for t in ts {
        let thread_num = usize::from_str_radix(t, 10).unwrap_or(1);
        builder.threads(thread_num);
    }

    let mut c = criterion::Criterion::default()
        .sample_size(10)
        .output_directory(std::path::Path::new("out"));

    if let Builder::Stack(b) = builder {
        b.configure(
            &mut c,
            "nrbench-stack",
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
                    while replica.get_responses(rid, &mut o) == 0 {}
                    o.clear();
                }
            },
        );
    } else if let Builder::Synthetic(b) = builder {
        b.configure(
            &mut c,
            "nrbench-synthetic",
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
                    while replica.get_responses(rid, &mut o) == 0 {}
                    o.clear();
                }
            },
        );
    } else if let Builder::HashMap(b) = builder {
        b.configure(
            &mut c,
            "nrbench-hashmap",
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
                    while replica.get_responses(rid, &mut o) == 0 {}
                    o.clear();
                }
            },
        );
    } else {
        unreachable!("Unhandled builder type.");
    }
    c.final_summary();
}
