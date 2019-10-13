// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

extern crate chrono;
extern crate clap;
extern crate core_affinity;
extern crate csv;
extern crate nom;
extern crate rand;
extern crate rustc_serialize;
extern crate std;

mod pinning;
mod plot;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::sync::{Arc, Barrier};
use std::thread;
use std::usize;

use clap::{load_yaml, App};

use chrono::Duration;
use serde::{Deserialize, Serialize};

use log::*;

use node_replication::log::Log;
use node_replication::replica::Replica;
use node_replication::Dispatch;

use rand::{thread_rng, Rng};

use pinning::{Cpu, MachineTopology, Socket, ThreadMapping, L1, L2, L3};
use plot::{plot_per_thread_throughput, plot_throughput_scale_out};

const DEFAULT_STACK_SIZE: u32 = 1000u32 * 1000u32;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicaStrategy {
    /// One replica per system.
    One,
    /// One replica per L1 cache.
    L1,
    /// One replica per L2 cache.
    L2,
    /// One replica per L3 cache.
    L3,
    /// One replica per socket.
    Socket,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
enum Op {
    Push(u32),

    Pop,

    Invalid,
}

impl Default for Op {
    fn default() -> Op {
        Op::Invalid
    }
}

struct Stack {
    storage: RefCell<Vec<u32>>,
}

impl Stack {
    pub fn push(&self, data: u32) {
        self.storage.borrow_mut().push(data);
    }

    pub fn pop(&self) -> Option<u32> {
        self.storage.borrow_mut().pop()
    }
}

impl Default for Stack {
    fn default() -> Stack {
        let s = Stack {
            storage: Default::default(),
        };

        for e in 0..DEFAULT_STACK_SIZE {
            s.push(e);
        }

        s
    }
}

impl Dispatch for Stack {
    type Operation = Op;
    type Response = Option<u32>;

    fn dispatch(&self, op: Self::Operation) -> Self::Response {
        match op {
            Op::Push(v) => {
                self.push(v);
                return None;
            }

            Op::Pop => return self.pop(),

            Op::Invalid => unreachable!("Op::Invalid?"),
        }
    }
}

fn bench_st(nop: usize, core: pinning::Core) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core as usize });
    let r: Stack = Default::default();

    let mut orng = thread_rng();
    let mut arng = thread_rng();

    let mut ops = Vec::with_capacity(nop);
    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 2usize {
            0usize => ops.push(Op::Pop),
            1usize => ops.push(Op::Push(arng.gen())),
            _ => ops.push(Op::Invalid),
        }
    }

    let time = Duration::span(|| {
        for i in 0..nop {
            r.dispatch(ops[i]);
        }
    });

    let duration = time.num_microseconds().unwrap();
    let throughput: usize = (nop * 1000 * 1000) / (duration) as usize;
    println!(
        "Baseline Single Thread Stack: Throughput: {} op/s",
        throughput
    );
}

fn bench(
    r: Arc<Replica<Stack>>,
    nop: usize,
    barrier: Arc<Barrier>,
    core: pinning::Core,
) -> (u64, u64, i64) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core as usize });

    let idx = r.register().expect("Failed to register with Replica.");

    let mut orng = thread_rng();
    let mut arng = thread_rng();

    let mut ops = Vec::with_capacity(nop);
    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 2usize {
            0usize => ops.push(Op::Pop),
            1usize => ops.push(Op::Push(arng.gen())),
            _ => ops.push(Op::Invalid),
        }
    }
    barrier.wait();

    let mut o = vec![];
    let mut _resps = 0;
    let time = Duration::span(|| {
        for i in 0..nop {
            r.execute(ops[i], idx);
            while r.get_responses(idx, &mut o) == 0 {};
            o.clear();
        }
    });

    let duration = time.num_microseconds().unwrap();
    let throughput: usize = (nop * 1000 * 1000) / (duration) as usize;
    info!("Thread {} Throughput: {} op/s", idx, throughput);

    (idx as u64, throughput as u64, duration)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Benchmark {
    /// Replica granularity.
    rs: ReplicaStrategy,
    /// Thread assignment.
    tm: ThreadMapping,
    /// # Replicas
    r: usize,
    /// # Threads.
    t: usize,
    /// Log size.
    l: usize,
    /// Operations on the log.
    n: usize,
    /// Replica <-> Thread/Cpu mapping as used by the benchmark.
    rm: HashMap<usize, Vec<Cpu>>,
    /// Results of the benchmark, (replica idx, ops/s, runtime in microseconds) per thread.
    results: Vec<(u64, u64, i64)>,
}

impl Benchmark {
    fn result_filename(&self) -> String {
        format!(
            "nrbench_stack_threads_{}_replicas_{}_tm_{:?}_rm_{:?}_l_{}_n_{}",
            self.t, self.r, self.tm, self.rs, self.l, self.n
        )
    }

    fn aggregate_tput(&self) -> u64 {
        assert_eq!(self.t, self.results.len(), "Have all results");
        self.results.iter().map(|(_rid, tput, _dur)| tput).sum()
    }

    fn threads(&self) -> u64 {
        assert_eq!(self.t, self.results.len(), "Have all results");
        self.results.len() as u64
    }
}

/// Calculates how to divide threads among replicas and CPU.
///
/// This is a function based on how many threads we have, how we map
/// them onto the CPUs, the granularity of replicas, and the topology of the
/// underlying hardware.
fn replica_core_allocation(
    topology: &MachineTopology,
    rs: ReplicaStrategy,
    tm: ThreadMapping,
    ts: usize,
) -> HashMap<usize, Vec<Cpu>> {
    let cpus = topology.allocate(tm, ts, false);
    trace!("Pin to {:?}", cpus);
    let mut rm: HashMap<usize, Vec<Cpu>> = HashMap::new();

    match rs {
        ReplicaStrategy::One => {
            rm.insert(0, cpus.iter().map(|c| c.cpu).collect());
        }
        ReplicaStrategy::Socket => {
            let mut sockets: Vec<Socket> = cpus.iter().map(|t| t.socket).collect();
            sockets.sort();
            sockets.dedup();

            for s in sockets {
                rm.insert(
                    s as usize,
                    cpus.iter()
                        .filter(|c| c.socket == s)
                        .map(|c| c.cpu)
                        .collect(),
                );
            }
        }
        ReplicaStrategy::L1 => {
            let mut l1: Vec<L1> = cpus.iter().map(|t| t.l1).collect();
            l1.sort();
            l1.dedup();

            for s in l1 {
                rm.insert(
                    s as usize,
                    cpus.iter().filter(|c| c.l1 == s).map(|c| c.cpu).collect(),
                );
            }
        }
        ReplicaStrategy::L2 => {
            let mut l2: Vec<L2> = cpus.iter().map(|t| t.l2).collect();
            l2.sort();
            l2.dedup();

            for s in l2 {
                rm.insert(
                    s as usize,
                    cpus.iter().filter(|c| c.l2 == s).map(|c| c.cpu).collect(),
                );
            }
        }
        ReplicaStrategy::L3 => {
            let mut l3: Vec<L3> = cpus.iter().map(|t| t.l3).collect();
            l3.sort();
            l3.dedup();

            for s in l3 {
                rm.insert(
                    s as usize,
                    cpus.iter().filter(|c| c.l3 == s).map(|c| c.cpu).collect(),
                );
            }
        }
    };

    rm
}

fn main() {
    env_logger::init();
    let yml = load_yaml!("args.yml");
    let matches = App::from_yaml(yml).get_matches();
    let topology = MachineTopology::new();

    let replica_strategy = match matches.value_of("replicas").unwrap_or("one") {
        "one" => ReplicaStrategy::One,
        "socket" => ReplicaStrategy::Socket,
        "l3" => ReplicaStrategy::L3,
        "l2" => ReplicaStrategy::L2,
        "l1" => ReplicaStrategy::L1,
        _ => unreachable!("Invalid CLI argument, may be clap bug if possible_values doesn't work?"),
    };

    let ts: Vec<&str> = matches.values_of("threads").unwrap().collect();
    let ts: Vec<usize> = ts
        .iter()
        .map(|t| usize::from_str_radix(t, 10).unwrap_or(1))
        .collect();

    let l = usize::from_str_radix(matches.value_of("logsz").unwrap(), 10).unwrap();
    let n = usize::from_str_radix(matches.value_of("nop").unwrap(), 10).unwrap();

    bench_st(n * 100, 2);

    let thread_mapping = match matches.value_of("mapping").unwrap_or("sequential") {
        "none" => ThreadMapping::None,
        "sequential" => ThreadMapping::Sequential,
        "interleave" => ThreadMapping::Interleave,
        _ => unreachable!("Invalid CLI argument, may be clap bug if possible_values doesn't work?"),
    };

    let mut benchmarks: Vec<Benchmark> = Vec::with_capacity(ts.len());
    for t in ts {
        let rm = replica_core_allocation(&topology, replica_strategy, thread_mapping, t);
        let r = rm.len();
        let mut c = Benchmark {
            rs: replica_strategy,
            tm: thread_mapping,
            r,
            t,
            l,
            n,
            rm,
            results: Default::default(),
        };
        debug!("Setup is: {:#?}", c);

        let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(
            l * 1024 * 1024 * 1024,
        ));

        let mut replicas = Vec::with_capacity(r);
        for i in 0..r {
            debug!("Creating replica {}", i);
            replicas.push(Arc::new(Replica::<Stack>::new(&log)));
        }

        let mut threads = Vec::new();
        let barrier = Arc::new(Barrier::new(t));

        for (rid, cores) in c.rm.clone().into_iter() {
            for cid in cores {
                let r = replicas[rid].clone();
                let o = n.clone();
                let b = barrier.clone();
                println!(
                    "Spawn thread#{} on core {} with replica {}",
                    threads.len(),
                    cid,
                    rid
                );
                let child = thread::spawn(move || bench(r, o, b, cid));
                threads.push(child);
            }
        }

        for _i in 0..threads.len() {
            let retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Thread didn't finish successfully.");
            c.results.push(retval);
        }
        plot_per_thread_throughput(&c, &c.results).unwrap();
        benchmarks.push(c);
    }

    let mut tputs = Vec::with_capacity(benchmarks.len());
    for b in benchmarks.iter() {
        tputs.push((b.threads(), b.aggregate_tput()));
    }
    plot_throughput_scale_out(&tputs).expect("Can't plot throughput scale-out graph");

    let f = File::create("results.json").expect("Unable to create file");
    let f = BufWriter::new(f);
    serde_json::to_writer_pretty(f, &benchmarks).unwrap();
}
