// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Helper functions to instantiate and configure benchmarks.
//!
//! The file exports two items:
//!  - baseline_comparison: A generic function to compare a data-structure
//!    with and without a log.
//! - `ScaleBenchBuilder`: A struct that helps to configure criterion
//!    to evaluate the scalability of a data-structure with node-replication.
#![allow(unused)]

use std::cell::RefMut;
use std::collections::HashMap;
use std::fmt;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc, Barrier};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use criterion::{black_box, BenchmarkId, Criterion, Throughput};
use log::*;
use node_replication::{log::Log, replica::Replica, Dispatch};

use crate::utils;
use crate::utils::topology::*;

pub use crate::utils::topology::ThreadMapping;

use arr_macro::arr;

/// Threshold after how many iterations we log a warning for busy spinning loops.
///
/// This helps with debugging to figure out where things may end up blocking.
/// Should be a power of two to avoid divisions.
pub const WARN_THRESHOLD: usize = 1 << 28;

type BenchFn<T> = fn(
    crate::utils::ThreadId,
    usize,
    &Arc<Log<'static, <T as Dispatch>::Operation>>,
    &Arc<Replica<T>>,
    &Vec<<T as Dispatch>::Operation>,
    usize,
);

/// Creates a benchmark to evalute the overhead the log adds for a given data-structure.
///
/// Takes a generic data-structure that implements dispatch and a vector of operations
/// to execute against said data-structure.
///
/// Then configures the supplied criterion runner to do two benchmarks:
/// - Running the DS operations on a single-thread directly against the DS.
/// - Running the DS operation on a single-thread but go through a replica/log.
pub fn baseline_comparison<T: Dispatch + Default>(
    c: &mut Criterion,
    name: &str,
    ops: Vec<<T as Dispatch>::Operation>,
    log_size_bytes: usize,
) {
    utils::disable_dvfs();
    let mut s: T = Default::default();

    // First benchmark is just a stack on a single thread:
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(ops.len() as u64));
    group.bench_function("baseline", |b| {
        b.iter(|| {
            for i in 0..ops.len() {
                s.dispatch(ops[i].clone());
            }
        })
    });

    // 2nd benchmark we compare the stack but now we put a log in front:
    let log = Arc::new(Log::<<T as Dispatch>::Operation>::new(log_size_bytes));
    let r = Replica::<T>::new(&log);
    let ridx = r.register().expect("Failed to register with Replica.");

    group.bench_function("log", |b| {
        b.iter(|| {
            let mut o = vec![];
            for i in 0..ops.len() {
                r.execute(ops[i].clone(), ridx);
                while r.get_responses(ridx, &mut o) == 0 {}
                o.clear();
            }
        })
    });

    group.finish();
}

/// How replicas are mapped to cores/threads.
#[derive(Copy, Clone, Eq, PartialEq)]
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

impl fmt::Debug for ReplicaStrategy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ReplicaStrategy::One => write!(f, "RS=System"),
            ReplicaStrategy::L1 => write!(f, "RS=L1"),
            ReplicaStrategy::L2 => write!(f, "RS=L2"),
            ReplicaStrategy::L3 => write!(f, "RS=L3"),
            ReplicaStrategy::Socket => write!(f, "RS=Socket"),
        }
    }
}

pub struct ScaleBenchmark<T: Dispatch + Default + Send>
where
    <T as node_replication::Dispatch>::Operation: std::marker::Send,
    <T as node_replication::Dispatch>::Response: std::marker::Send,
    <T as node_replication::Dispatch>::Operation: 'static,
{
    /// Replica <-> Thread/Cpu mapping as used by the benchmark.
    rm: HashMap<usize, Vec<Cpu>>,
    /// An Arc reference to operations executed on the log.
    operations: Vec<<T as Dispatch>::Operation>,
    /// An Arc reference to the log.
    log: Arc<Log<'static, <T as Dispatch>::Operation>>,
    /// Results of the benchmark, (replica idx, ops/s, runtime in microseconds) per thread.
    results: Vec<(u64, u64, i64)>,
    /// Batch-size (passed as a parameter to benchmark funtion `f`)
    batch_size: usize,
    /// If we should wait at the end and periodically process the log
    /// (to avoid lifeness issues where all threads of a replica A have exited
    /// and now replica B can no longer make progress due to GC)
    sync: bool,
    /// Benchmark function to execute
    f: BenchFn<T>,
    /// A series of channels to communicate iteration count to every worker.
    cmd_channels: Vec<Sender<u64>>,
    /// A result channel
    result_channel: (Sender<Duration>, Receiver<Duration>),
    /// Thread handles
    handles: Vec<JoinHandle<()>>,
}

impl<T: Dispatch + Default + Send> ScaleBenchmark<T>
where
    <T as node_replication::Dispatch>::Operation: std::marker::Send,
    <T as node_replication::Dispatch>::Operation: std::marker::Sync,
    <T as node_replication::Dispatch>::Response: std::marker::Send,
    <T as node_replication::Dispatch>::ResponseError: std::marker::Send,
    T: 'static,
{
    /// Create a new ScaleBenchmark.
    fn new(
        topology: &MachineTopology,
        rs: ReplicaStrategy,
        tm: ThreadMapping,
        ts: usize,
        operations: Vec<<T as Dispatch>::Operation>,
        batch_size: usize,
        sync: bool,
        log: Arc<Log<'static, <T as Dispatch>::Operation>>,
        f: BenchFn<T>,
    ) -> ScaleBenchmark<T> {
        ScaleBenchmark {
            rm: ScaleBenchmark::<T>::replica_core_allocation(topology, rs, tm, ts),
            operations: operations,
            log,
            results: Default::default(),
            batch_size,
            sync,
            f: f,
            cmd_channels: Default::default(),
            result_channel: channel(),
            handles: Default::default(),
        }
    }

    /// Return the amount of threads created by this benchmark.
    fn threads(&self) -> usize {
        // aggregate cores per replica, then sum them all
        self.rm.values().map(|v| v.len()).sum()
    }

    /// Return the amount of replias created by this benchmark.
    fn replicas(&self) -> usize {
        self.rm.len()
    }

    /// Terminate the worker threads by sending 0 to the iter channel:
    fn terminate(&self) {
        for tx in self.cmd_channels.iter() {
            tx.send(0).expect("Can't send termination.");
        }
    }

    /// Execute sends the iteration count to all worker threads
    /// then waits to receive the respective duration from the workers
    /// finally it returns the minimal Duration over all threads
    /// after ensuring the run was fair.
    fn execute(&self, iters: u64, reset_log: bool) -> Duration {
        if reset_log {
            unsafe {
                self.log.reset();
            }
        }

        for tx in self.cmd_channels.iter() {
            tx.send(iters).expect("Can't send iter.");
        }

        // Wait for all threads to finish and gather runtimes
        let mut durations: Vec<Duration> = Vec::with_capacity(self.threads());
        for i in 0..self.threads() {
            let duration: Duration = self
                .result_channel
                .1
                .recv()
                .expect("Can't receive a Duration?");
            durations.push(duration);
        }

        // Sort floats to get min/max, can't just do sort() because floats are weird:
        durations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let min_thread_duration: f64 = durations
            .first()
            .unwrap_or(&Default::default())
            .as_secs_f64();
        let max_thread_duration: f64 = durations
            .last()
            .unwrap_or(&Default::default())
            .as_secs_f64();

        // Panic in case we are starving threads:
        debug_assert!(
            min_thread_duration < max_thread_duration,
            "Calculating fairness only works if min < max."
        );
        debug_assert!(min_thread_duration > 0.0, "Threads must have some runtime");
        let fairness = max_thread_duration / min_thread_duration;
        if fairness < 0.9 {
            panic!("Fairness threshold below 0.9: {}, figure out why some threads were starved (max = {}, min = {})?", fairness, max_thread_duration, min_thread_duration);
        }

        durations[0]
    }

    fn alloc_replicas(&self, replicas: &mut Vec<Arc<Replica<T>>>) {
        for (rid, cores) in self.rm.clone().into_iter() {
            // Pinning the thread to the replica' cores forces the memory
            // allocation to be local to the where a replica will be used later
            utils::pin_thread(cores[0]);

            let log = self.log.clone();
            replicas.push(Arc::new(Replica::<T>::new(&log)));
        }
    }

    fn startup(&mut self, name: &str) {
        let thread_num = self.threads();
        // Need a barrier to synchronize starting of threads
        let barrier = Arc::new(Barrier::new(thread_num));

        let complete = Arc::new(arr![AtomicUsize::default(); 128]);
        let mut replicas: Vec<Arc<Replica<T>>> = Vec::with_capacity(self.replicas());
        self.alloc_replicas(&mut replicas);
        let do_sync = self.sync;

        debug!(
            "Execute benchmark with the following replica: [core_id] mapping: {:#?}",
            self.rm
        );
        let mut tid = 0;
        for (rid, cores) in self.rm.clone().into_iter() {
            let num = cores.len();
            for core_id in cores {
                let b = barrier.clone();

                let log: Arc<_> = self.log.clone();
                let replica = replicas[rid].clone();
                let operations = self.operations.clone();
                let f = self.f.clone();
                let batch_size = self.batch_size;
                let replica_token = replica
                    .register()
                    .expect("Can't register replica, out of slots?");

                let (iter_tx, iter_rx) = channel();
                self.cmd_channels.push(iter_tx);
                let result_channel = self.result_channel.0.clone();

                let com = complete.clone();
                let nre = replicas.len();
                let rmc = self.rm.clone();

                self.handles.push(thread::spawn(move || {
                    utils::pin_thread(core_id);
                    loop {
                        let iters = iter_rx.recv().expect("Can't get iter from channel?");
                        if iters == 0 {
                            debug!(
                                "Finished with this ScaleBench, worker thread {} is done.",
                                tid
                            );
                            return;
                        }

                        debug!(
                            "Running {:?} on core {} replica#{} rtoken#{} iters={}",
                            thread::current().id(),
                            core_id,
                            rid,
                            replica_token,
                            iters
                        );

                        b.wait();
                        let start = Instant::now();
                        for _i in 0..iters {
                            black_box((f)(
                                core_id,
                                replica_token,
                                &log,
                                &replica,
                                &operations,
                                batch_size,
                            ));
                        }
                        let elapsed = start.elapsed();

                        debug!(
                            "Completed {:?} on core {} replica#{} rtoken#{} in {:?}",
                            thread::current().id(),
                            core_id,
                            rid,
                            replica_token,
                            elapsed
                        );

                        result_channel.send(elapsed);
                        if !do_sync {
                            b.wait();
                            continue;
                        } else if com[rid].fetch_add(1, Ordering::Relaxed) == num - 1 {
                            // Periodically sync/advance all, and return once all
                            // replicas have completed.
                            loop {
                                let mut done = 0; // How many replicas are done with the operations
                                for (r, c) in rmc.clone().into_iter() {
                                    if com[r].load(Ordering::Relaxed) == c.len() {
                                        done += 1;
                                    }
                                }
                                if done == nre {
                                    break;
                                }

                                // Consume the log but we don't apply operations anymore
                                replica.sync(|_o: <T as Dispatch>::Operation, _r: usize| {});
                            }
                        }

                        b.wait();
                        com[rid].store(0, Ordering::Relaxed);
                    }
                }));

                tid += 1;
            }
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
        let cpus = topology.allocate(tm, ts, true);
        debug_assert_eq!(ts, cpus.len());

        trace!(
            "Allocated cores for benchmark with {:?} {:?} {:?}",
            rs,
            tm,
            cpus
        );
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
}

/// A generic benchmark configurator for node-replication scalability benchmarks.
#[derive(Debug)]
pub struct ScaleBenchBuilder<T: Dispatch + Default>
where
    <T as node_replication::Dispatch>::Operation: std::marker::Send,
    <T as node_replication::Dispatch>::Response: std::marker::Send,
    T: 'static,
{
    /// Replica granularity.
    replica_strategies: Vec<ReplicaStrategy>,
    /// Thread assignments.
    thread_mappings: Vec<ThreadMapping>,
    /// # Threads.
    threads: Vec<usize>,
    /// Batch sizes to use (default 1)
    batches: Vec<usize>,
    /// Sync replicas periodically (true)
    sync: bool,
    /// Log size (bytes).
    log_size: usize,
    /// Operations executed on the log.
    operations: Vec<<T as Dispatch>::Operation>,
    /// Reset the log between different `execute`
    /// If we have many ops and do tests where there is no GC, we may starve
    reset_log: bool,
}

impl<T: Dispatch + Default> ScaleBenchBuilder<T>
where
    <T as node_replication::Dispatch>::Operation: std::marker::Send,
    <T as node_replication::Dispatch>::Operation: std::marker::Sync,
    <T as node_replication::Dispatch>::Response: std::marker::Send,
    <T as node_replication::Dispatch>::ResponseError: std::marker::Send,
    T: 'static,
    T: std::marker::Send,
{
    /// Initialize an "empty" ScaleBenchBuilder with a  MiB log.
    ///
    /// By default this won't execute any runs,
    /// you have to at least call `threads`, `thread_mapping`
    /// `replica_strategy` once and set `operations`.
    pub fn new(ops: Vec<<T as Dispatch>::Operation>) -> ScaleBenchBuilder<T> {
        ScaleBenchBuilder {
            replica_strategies: Vec::new(),
            thread_mappings: Vec::new(),
            threads: Vec::new(),
            batches: vec![1usize],
            sync: true,
            log_size: 1024 * 1024 * 2,
            operations: ops,
            reset_log: false,
        }
    }

    /// Configures the builder automatically based on the underlying machine properties.
    pub fn machine_defaults(&mut self) -> &mut Self {
        let topology = MachineTopology::new();

        self.thread_mapping(ThreadMapping::Sequential);
        self.replica_strategy(ReplicaStrategy::One);
        self.replica_strategy(ReplicaStrategy::Socket);
        self.replica_strategy(ReplicaStrategy::L1);

        // On larger machines thread increments are bigger than on
        // smaller machines:
        let thread_incremements = if topology.cores() > 24 {
            8
        } else if topology.cores() > 16 {
            4
        } else {
            2
        };

        for t in (0..(topology.cores() + 1)).step_by(thread_incremements) {
            if t == 0 {
                // Can't run on 0 threads
                self.threads(t + 1);
            } else {
                self.threads(t);
            }
        }

        self
    }

    /// Run benchmark with batching of size `b`.
    pub fn add_batch(&mut self, b: usize) -> &mut Self {
        self.batches.push(b);
        self
    }

    /// Run benchmark with `t` threads.
    pub fn threads(&mut self, t: usize) -> &mut Self {
        self.threads.push(t);
        self
    }

    /// Run benchmark with given thread <-> machine mapping.
    pub fn thread_mapping(&mut self, tm: ThreadMapping) -> &mut Self {
        self.thread_mappings.push(tm);
        self
    }

    /// Run benchmark with given replication strategy.
    pub fn replica_strategy(&mut self, rs: ReplicaStrategy) -> &mut Self {
        self.replica_strategies.push(rs);
        self
    }

    /// Run benchmark with the `ls` bytes of log-size.
    pub fn log_size(&mut self, ls: usize) -> &mut Self {
        self.log_size = ls;
        self
    }

    /// Reset the log for different executions.
    pub fn reset_log(&mut self) -> &mut Self {
        self.reset_log = true;
        self
    }

    /// Disable syncing the log for a replica after all threads have completed.
    pub fn disable_sync(&mut self) -> &mut Self {
        self.sync = false;
        self
    }

    /// Creates a benchmark to evalute the scalability properties of the
    /// log for a given data-structure.
    ///
    /// This configures the supplied criterion runner to execute
    /// as many benchmarks as result from the configuration options set in the
    /// ScaleBenchBuilder arguments.
    ///
    /// Criterion will be configured to create a run for every
    /// possible triplet: (replica strategy, thread mapping, #threads).
    pub fn configure(&self, c: &mut Criterion, name: &str, f: BenchFn<T>) {
        let topology = MachineTopology::new();
        utils::disable_dvfs();

        let mut group = c.benchmark_group(name);
        for rs in self.replica_strategies.iter() {
            for tm in self.thread_mappings.iter() {
                for ts in self.threads.iter() {
                    for b in self.batches.iter() {
                        let log = Arc::new(Log::<<T as Dispatch>::Operation>::new(self.log_size));
                        let mut runner = ScaleBenchmark::<T>::new(
                            &topology,
                            *rs,
                            *tm,
                            *ts,
                            self.operations.to_vec(),
                            *b,
                            self.sync,
                            log.clone(),
                            f,
                        );
                        runner.startup(name);

                        let name = format!("{:?} {:?} BS={}", *rs, *tm, *b);
                        group.throughput(Throughput::Elements((self.operations.len() * ts) as u64));
                        group.bench_with_input(
                            BenchmarkId::new(name, *ts),
                            &runner,
                            |cb, runner| {
                                cb.iter_custom(|iters| runner.execute(iters, self.reset_log))
                            },
                        );

                        runner.terminate();
                    }
                }
            }
        }
    }
}
