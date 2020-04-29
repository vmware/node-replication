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

use std::cell::{Cell, RefMut};
use std::collections::HashMap;
use std::fmt;
use std::fs::OpenOptions;
use std::hint::black_box;
use std::io::Write;
use std::marker::{PhantomData, Send, Sync};
use std::path::Path;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc, Barrier, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use csv::WriterBuilder;
use log::*;
use node_replication::{log::Log, replica::Replica, Dispatch};
use rand::seq::SliceRandom;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use serde::Serialize;

use crate::utils;
use crate::utils::benchmark::*;
use crate::utils::topology::*;
use crate::utils::Operation;

pub use crate::utils::topology::ThreadMapping;

use arr_macro::arr;

/// Threshold after how many iterations we log a warning for busy spinning loops.
///
/// This helps with debugging to figure out where things may end up blocking.
/// Should be a power of two to avoid divisions.
pub const WARN_THRESHOLD: usize = 1 << 28;

type BenchFn<R> = fn(
    crate::utils::ThreadId,
    usize,
    &Arc<Log<'static, <<R as ReplicaTrait>::D as Dispatch>::WriteOperation>>,
    &Arc<R>,
    &Operation<
        <<R as ReplicaTrait>::D as Dispatch>::ReadOperation,
        <<R as ReplicaTrait>::D as Dispatch>::WriteOperation,
    >,
    usize,
);

pub trait ReplicaTrait {
    type D: Dispatch + Default + Sync;

    fn new_arc(log: &Arc<Log<'static, <Self::D as Dispatch>::WriteOperation>>) -> Arc<Self>;

    fn register_me(&self) -> Option<usize>;

    fn sync_me<F: FnMut(<Self::D as Dispatch>::WriteOperation, usize)>(&self, d: F);

    fn exec(
        &self,
        op: <Self::D as Dispatch>::WriteOperation,
        idx: usize,
    ) -> Result<<Self::D as Dispatch>::Response, <Self::D as Dispatch>::ResponseError>;

    fn exec_ro(
        &self,
        op: <Self::D as Dispatch>::ReadOperation,
        idx: usize,
    ) -> Result<<Self::D as Dispatch>::Response, <Self::D as Dispatch>::ResponseError>;
}

impl<'a, T: Dispatch + Sync + Default> ReplicaTrait for node_replication::replica::Replica<'a, T> {
    type D = T;

    fn new_arc(log: &Arc<Log<'static, <Self::D as Dispatch>::WriteOperation>>) -> Arc<Self> {
        Self::new(log)
    }

    fn sync_me<F: FnMut(<Self::D as Dispatch>::WriteOperation, usize)>(&self, mut d: F) {
        self.sync(d);
    }

    fn register_me(&self) -> Option<usize> {
        self.register()
    }

    fn exec(
        &self,
        op: <Self::D as Dispatch>::WriteOperation,
        idx: usize,
    ) -> Result<<Self::D as Dispatch>::Response, <Self::D as Dispatch>::ResponseError> {
        self.execute(op, idx)
    }

    fn exec_ro(
        &self,
        op: <Self::D as Dispatch>::ReadOperation,
        idx: usize,
    ) -> Result<<Self::D as Dispatch>::Response, <Self::D as Dispatch>::ResponseError> {
        self.execute_ro(op, idx)
    }
}

/// Log the baseline comparision results to a CSV file
///
/// # TODO
/// Ideally this can go into the runner that was previously
/// not possible since we used criterion for the runner.
fn write_results(name: String, duration: Duration, results: Vec<usize>) -> std::io::Result<()> {
    let file_name = "baseline_comparison.csv";
    let write_headers = !Path::new(file_name).exists(); // write headers only to new file
    let mut csv_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(file_name)?;

    let mut wtr = WriterBuilder::new()
        .has_headers(write_headers)
        .from_writer(csv_file);

    #[derive(Serialize)]
    struct Record {
        name: String,
        batch_size: usize,
        duration: f64,
        exp_time_in_sec: usize,
        iterations: usize,
    };

    for (idx, ops) in results.iter().enumerate() {
        let record = Record {
            name: name.clone(),
            batch_size: 1,
            duration: duration.as_secs_f64(),
            exp_time_in_sec: idx + 1, // start at 1 (for first second)
            iterations: *ops,
        };
        wtr.serialize(record);
    }

    wtr.flush()
}

/// Creates a benchmark to evalute the overhead the log adds for a given data-structure.
///
/// Takes a generic data-structure that implements dispatch and a vector of operations
/// to execute against said data-structure.
///
/// Then configures the supplied criterion runner to do two benchmarks:
/// - Running the DS operations on a single-thread directly against the DS.
/// - Running the DS operation on a single-thread but go through a replica/log.
pub(crate) fn baseline_comparison<R: ReplicaTrait>(
    c: &mut TestHarness,
    name: &str,
    ops: Vec<Operation<<R::D as Dispatch>::ReadOperation, <R::D as Dispatch>::WriteOperation>>,
    log_size: usize,
) where
    R::D: Dispatch + Sync + Default,
    <R::D as Dispatch>::WriteOperation: Send,
    <R::D as Dispatch>::WriteOperation: Sync,
    <R::D as Dispatch>::ReadOperation: Sync,
    <R::D as Dispatch>::ReadOperation: Send,
    <R::D as Dispatch>::Response: Send,
    <R::D as Dispatch>::ResponseError: Send,
{
    utils::disable_dvfs();
    let mut s: R::D = Default::default();

    let log_period = Duration::from_secs(1);

    // 1st benchmark: just T on a single thread
    let mut group = c.benchmark_group(name);
    let duration = group.duration;

    let mut operations_per_second: Vec<usize> = Vec::with_capacity(32);
    group.bench_function("baseline", |b| {
        b.iter(|| {
            let mut operations_completed: usize = 0;
            let nop: usize = ops.len();
            let mut iter: usize = 0;

            let start = Instant::now();
            let end_experiment = start + duration;
            let mut next_log = start + log_period;

            while Instant::now() < end_experiment {
                match &ops[iter % nop] {
                    Operation::ReadOperation(o) => {
                        s.dispatch(o.clone());
                    }
                    Operation::WriteOperation(o) => {
                        s.dispatch_mut(o.clone());
                    }
                }
                operations_completed += 1;
                iter += 1;

                if Instant::now() >= next_log {
                    trace!("Operations completed {} / s", operations_completed);
                    operations_per_second.push(operations_completed);
                    // reset operations completed
                    operations_completed = 0;
                    next_log += log_period;
                }
            }

            // Some threads may not end up adding the last second of measuring due to bad timing,
            // so make sure we remove it everywhere:
            if operations_per_second.len() == duration.as_secs() as usize {
                operations_per_second.pop(); // Get rid of last second of measurements
            }

            operations_completed
        })
    });
    write_results(
        format!("{}-{}", group.group_name.clone(), "baseline"),
        duration,
        operations_per_second,
    )
    .expect("Can't write resutls");

    // 2nd benchmark: we compare T with a log in front:
    let log = Arc::new(Log::<<R::D as Dispatch>::WriteOperation>::new(log_size));
    let r = Replica::<R::D>::new(&log);
    let ridx = r.register_me().expect("Failed to register with Replica.");

    let mut operations_per_second: Vec<usize> = Vec::with_capacity(32);
    group.bench_function("log", |b| {
        b.iter(|| {
            let mut operations_completed: usize = 0;
            let nop: usize = ops.len();
            let mut iter: usize = 0;

            let start = Instant::now();
            let end_experiment = start + duration;
            let mut next_log = start + log_period;

            while Instant::now() < end_experiment {
                match &ops[iter % nop] {
                    Operation::ReadOperation(op) => {
                        r.exec_ro(op.clone(), ridx);
                    }
                    Operation::WriteOperation(op) => {
                        r.exec(op.clone(), ridx);
                    }
                }
                operations_completed += 1;
                iter += 1;

                if Instant::now() >= next_log {
                    trace!("Operations completed {} / s", operations_completed);
                    operations_per_second.push(operations_completed);
                    // reset operations completed
                    operations_completed = 0;
                    next_log += log_period;
                }
            }

            // Some threads may not end up adding the last second of measuring due to bad timing,
            // so make sure we remove it everywhere:
            if operations_per_second.len() == duration.as_secs() as usize {
                operations_per_second.pop(); // Get rid of last second of measurements
            }

            operations_completed
        })
    });
    write_results(
        format!("{}-{}", group.group_name.clone(), "log"),
        duration,
        operations_per_second,
    )
    .expect("Can't write resutls");

    group.finish();
}

/// How replicas are mapped to cores/threads.
#[derive(Serialize, Copy, Clone, Eq, PartialEq)]
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
    /// One for every hardware thread.
    PerThread,
}

impl fmt::Display for ReplicaStrategy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ReplicaStrategy::One => write!(f, "System"),
            ReplicaStrategy::L1 => write!(f, "L1"),
            ReplicaStrategy::L2 => write!(f, "L2"),
            ReplicaStrategy::L3 => write!(f, "L3"),
            ReplicaStrategy::Socket => write!(f, "Socket"),
            ReplicaStrategy::PerThread => write!(f, "PerThread"),
        }
    }
}

impl fmt::Debug for ReplicaStrategy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ReplicaStrategy::One => write!(f, "RS=System"),
            ReplicaStrategy::L1 => write!(f, "RS=L1"),
            ReplicaStrategy::L2 => write!(f, "RS=L2"),
            ReplicaStrategy::L3 => write!(f, "RS=L3"),
            ReplicaStrategy::Socket => write!(f, "RS=Socket"),
            ReplicaStrategy::PerThread => write!(f, "RS=PerThread"),
        }
    }
}

pub struct ScaleBenchmark<R: ReplicaTrait>
where
    <R::D as Dispatch>::WriteOperation: Send,
    <R::D as Dispatch>::WriteOperation: Sync,
    <R::D as Dispatch>::ReadOperation: Sync,
    <R::D as Dispatch>::ReadOperation: Send,
    <R::D as Dispatch>::Response: Send,
    <R::D as Dispatch>::WriteOperation: 'static,
    R::D: Sync + Dispatch + Default + Send,
{
    /// Name of the benchmark
    name: String,
    /// ReplicaStrategy used by the benchmark
    rs: ReplicaStrategy,
    /// ThreadMapping used by the benchmark
    tm: ThreadMapping,
    /// Total amount of threads used by the benchmark
    ts: usize,
    /// Replica <-> Thread/Cpu mapping as used by the benchmark.
    rm: HashMap<usize, Vec<Cpu>>,
    /// An Arc reference to operations executed on the log.
    operations:
        Vec<Operation<<R::D as Dispatch>::ReadOperation, <R::D as Dispatch>::WriteOperation>>,
    /// An Arc reference to the log.
    log: Arc<Log<'static, <R::D as Dispatch>::WriteOperation>>,
    /// Results of the benchmark we map the #iteration to a list of per-thread results
    /// (each per-thread stores completed ops in per-sec intervals).
    /// It's a hash-map so it acts like a cache i.e., we ensure to only save the latest
    /// experiement for a specific duration (avoids storing the warm-up results).
    /// It has to be a Mutex because we can only access a RO version of ScaleBenchmark at execution time.
    results: Mutex<HashMap<Duration, Vec<(Core, Vec<usize>)>>>,
    /// Batch-size (passed as a parameter to benchmark funtion `f`)
    batch_size: usize,
    /// If we should wait at the end and periodically process the log
    /// (to avoid lifeness issues where all threads of a replica A have exited
    /// and now replica B can no longer make progress due to GC)
    sync: bool,
    /// Benchmark function to execute
    f: BenchFn<R>,
    /// A series of channels to communicate iteration count to every worker.
    cmd_channels: Vec<Sender<Duration>>,
    /// A result channel
    result_channel: (Sender<(Core, Vec<usize>)>, Receiver<(Core, Vec<usize>)>),
    /// Thread handles
    handles: Vec<JoinHandle<()>>,
}

impl<R: 'static> ScaleBenchmark<R>
where
    <R::D as Dispatch>::WriteOperation: Send,
    <R::D as Dispatch>::WriteOperation: Sync,
    <R::D as Dispatch>::ReadOperation: Sync,
    <R::D as Dispatch>::ReadOperation: Send,
    <R::D as Dispatch>::Response: Send,
    <R::D as Dispatch>::ResponseError: Send,
    R::D: 'static + Sync + Dispatch + Default + Send,
    R: ReplicaTrait + Sync + Send,
{
    /// Create a new ScaleBenchmark.
    fn new(
        name: String,
        topology: &MachineTopology,
        rs: ReplicaStrategy,
        tm: ThreadMapping,
        ts: usize,
        operations: Vec<
            Operation<<R::D as Dispatch>::ReadOperation, <R::D as Dispatch>::WriteOperation>,
        >,
        batch_size: usize,
        sync: bool,
        log: Arc<Log<'static, <R::D as Dispatch>::WriteOperation>>,
        f: BenchFn<R>,
    ) -> ScaleBenchmark<R>
    where
        R: Sync,
    {
        ScaleBenchmark {
            name,
            rs,
            tm,
            ts,
            rm: ScaleBenchmark::<R>::replica_core_allocation(topology, rs, tm, ts),
            log,
            results: Default::default(),
            operations,
            batch_size,
            sync,
            f,
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
    fn terminate(&self) -> std::io::Result<()> {
        for tx in self.cmd_channels.iter() {
            tx.send(Duration::from_secs(0))
                .expect("Can't send termination.");
        }

        // Log the per-thread runtimes to the CSV file
        // TODO: Ideally this can go into the runner that was previously
        // not possible since we used criterion for the runner.
        let file_name = "scaleout_benchmarks.csv";
        let write_headers = !Path::new(file_name).exists(); // write headers only to new file
        let mut csv_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(file_name)?;
        let results = self.results.lock().expect("Can't lock results");

        let mut wtr = WriterBuilder::new()
            .has_headers(write_headers)
            .from_writer(csv_file);

        for (duration, thread_results) in results.iter() {
            for (tid, (cid, ops_per_sec)) in thread_results.iter().enumerate() {
                #[derive(Serialize)]
                struct Record {
                    name: String,
                    rs: ReplicaStrategy,
                    tm: ThreadMapping,
                    batch_size: usize,
                    threads: usize,
                    duration: f64,
                    thread_id: usize,
                    core_id: u64,
                    exp_time_in_sec: usize,
                    iterations: usize,
                };

                for (idx, ops) in ops_per_sec.iter().enumerate() {
                    let record = Record {
                        name: self.name.clone(),
                        rs: self.rs,
                        tm: self.tm,
                        batch_size: self.batch_size,
                        threads: self.ts,
                        duration: duration.as_secs_f64(),
                        thread_id: tid,
                        core_id: *cid,
                        exp_time_in_sec: idx + 1, // start at 1 (for first second)
                        iterations: *ops,
                    };
                    wtr.serialize(record);
                }
            }
        }
        wtr.flush()?;

        Ok(())
    }

    /// Execute sends the iteration count to all worker threads
    /// then waits to receive the respective duration from the workers
    /// finally it returns the minimal Duration over all threads
    /// after ensuring the run was fair.
    fn execute(&self, duration: Duration, reset_log: bool) -> usize {
        if reset_log {
            unsafe {
                self.log.reset();
            }
        }

        for tx in self.cmd_channels.iter() {
            tx.send(duration).expect("Can't send iter.");
        }

        // Wait for all threads to finish and gather runtimes
        let mut core_iteratios: Vec<(Core, Vec<usize>)> = Vec::with_capacity(self.threads());
        let mut intervals = 0;
        for i in 0..self.threads() {
            let core_ops: (Core, Vec<usize>) = self
                .result_channel
                .1
                .recv()
                .expect("Can't receive a per-thread result?");
            if intervals > 0 && intervals != core_ops.1.len() {
                error!("Receveived different no. of measurements from individual threads");
            }
            intervals = core_ops.1.len();
            core_iteratios.push(core_ops);
        }

        let core_aggregate_tput: Vec<usize> = core_iteratios
            .iter()
            .map(|(cid, time_vec)| time_vec.iter().sum())
            .collect();
        let min_tput = *core_aggregate_tput.iter().min().unwrap_or(&1) / intervals;
        let max_tput = *core_aggregate_tput.iter().max().unwrap_or(&1) / intervals;
        let total_tput = core_aggregate_tput.iter().sum::<usize>() / intervals;
        println!(
            ">> {:.2} Mops (min {:.2} Mops, max {:.2}) Mops",
            total_tput as f64 / 1_000_000 as f64,
            min_tput as f64 / 1_000_000 as f64,
            max_tput as f64 / 1_000_000 as f64,
        );

        let mut results = self.results.lock().unwrap();
        results.insert(duration, core_iteratios.clone());
        0usize
    }

    fn alloc_replicas(&mut self, replicas: &mut Vec<Arc<R>>) {
        let mut handles = Vec::with_capacity(self.rm.len());
        for (rid, cores) in self.rm.clone().into_iter() {
            let log = self.log.clone();
            // Parallelize the creation of the replicas as this can take
            // quite some time if you run e.g, PerThread or L1 strategies
            // on big machine
            handles.push(
                thread::spawn(move || {
                    let core0 = cores[0];
                    // Pinning the thread to the replica' cores forces the memory
                    // allocation to be local to the where a replica will be used later
                    utils::pin_thread(core0);

                    (rid, ReplicaTrait::new_arc(&log))
                })
                .join()
                .unwrap(),
            );
        }
        handles.sort_by(|a, b| b.0.cmp(&a.0));

        for handle in handles {
            replicas.push(handle.1);
        }
    }

    fn startup(&mut self) {
        let thread_num = self.threads();
        // Need a barrier to synchronize starting of threads
        let barrier = Arc::new(Barrier::new(thread_num));

        let complete = Arc::new(arr![AtomicUsize::default(); 128]);
        let mut replicas: Vec<Arc<R>> = Vec::with_capacity(self.replicas());
        self.alloc_replicas(&mut replicas);
        let do_sync = self.sync;

        debug!(
            "Execute benchmark {} with the following replica: [core_id] mapping: {:#?}",
            self.name, self.rm
        );
        let mut tid = 0;
        for (rid, cores) in self.rm.clone().into_iter() {
            let num = cores.len();
            for core_id in cores {
                // Pin thread to force the allocations below (`operations` etc.)
                // with the correct NUMA affinity
                utils::pin_thread(core_id);

                let b = barrier.clone();
                let log: Arc<_> = self.log.clone();
                let replica = replicas[rid].clone();
                let mut operations = self.operations.clone();
                operations.shuffle(&mut rand::thread_rng());
                let f = self.f.clone();
                let batch_size = self.batch_size;
                let replica_token = replica
                    .register_me()
                    .expect("Can't register replica, out of slots?");

                let (duration_tx, duration_rx) = channel();
                self.cmd_channels.push(duration_tx);
                let com = complete.clone();
                let result_channel = self.result_channel.0.clone();

                let com = complete.clone();
                let nre = replicas.len();
                let rmc = self.rm.clone();
                let log_period = Duration::from_secs(1);

                let name = self.name.clone();

                self.handles.push(thread::spawn(move || {
                    utils::pin_thread(core_id);
                    if name.starts_with("urcu") {
                        unsafe {
                            urcu_sys::rcu_register_thread();
                        }
                    }
                    loop {
                        let duration = duration_rx.recv().expect("Can't get iter from channel?");
                        if duration.as_nanos() == 0 {
                            debug!(
                                "Finished with this ScaleBench, worker thread {} is done.",
                                tid
                            );
                            return;
                        }

                        debug!(
                            "Running {:?} on core {} replica#{} rtoken#{} for {:?}",
                            thread::current().id(),
                            core_id,
                            rid,
                            replica_token,
                            duration
                        );

                        let mut operations_per_second: Vec<usize> = Vec::with_capacity(32);
                        let mut operations_completed: usize = 0;
                        let mut iter: usize = 0;
                        let nop: usize = operations.len();

                        b.wait();
                        let start = Instant::now();
                        let end_experiment = start + duration;
                        let mut next_log = start + log_period;
                        while Instant::now() < end_experiment {
                            for _i in 0..batch_size {
                                black_box((f)(
                                    core_id,
                                    replica_token,
                                    &log,
                                    &replica,
                                    &operations[iter],
                                    batch_size,
                                ));
                                iter = (iter + 1) % nop;
                            }
                            operations_completed += 1 * batch_size;

                            if Instant::now() >= next_log {
                                trace!("Operations completed {} / s", operations_completed);
                                operations_per_second.push(operations_completed);
                                // reset operations completed
                                operations_completed = 0;
                                next_log += log_period;
                            }
                        }

                        // Some threads may not end up adding the last second of measuring due to bad timing,
                        // so make sure we remove it everywhere:
                        if operations_per_second.len() == duration.as_secs() as usize {
                            operations_per_second.pop(); // Get rid of last second of measurements
                        }

                        debug!(
                            "Completed {:?} on core {} replica#{} rtoken#{} did {} ops in {:?}",
                            thread::current().id(),
                            core_id,
                            rid,
                            replica_token,
                            operations_completed,
                            duration
                        );

                        result_channel.send((core_id, operations_per_second));

                        if name.starts_with("urcu") {
                            unsafe {
                                urcu_sys::rcu_unregister_thread();
                            }
                        }

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
                                replica.sync_me(
                                    |_o: <R::D as Dispatch>::WriteOperation, _r: usize| {},
                                );
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
            ReplicaStrategy::L1 => match tm {
                ThreadMapping::None => {}
                ThreadMapping::Sequential => {
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
                // Giving replica number based on L1 number won't work in this case, as the
                // L1 numbers are allocated to Node-0 first and then to Node-1, and so on.
                ThreadMapping::Interleave => {
                    let mut l1: Vec<L1> = cpus.iter().map(|t| t.l1).collect();
                    l1.sort();
                    l1.dedup();

                    let mut rid = 0;
                    let mut mapping: HashMap<L1, usize> = HashMap::with_capacity(cpus.len());
                    for cpu in cpus.iter() {
                        let cache_num = cpu.l1;
                        if mapping.get(&cache_num).is_none() {
                            mapping.insert(cache_num, rid);
                            rid += 1;
                        }
                    }

                    for s in l1 {
                        rm.insert(
                            *mapping.get(&s).unwrap(),
                            cpus.iter().filter(|c| c.l1 == s).map(|c| c.cpu).collect(),
                        );
                    }
                }
            },
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
            ReplicaStrategy::PerThread => {
                for (idx, core) in cpus.iter().map(|c| c.cpu).enumerate() {
                    rm.insert(idx, vec![core]);
                }
            }
        };

        rm
    }
}

/// A generic benchmark configurator for node-replication scalability benchmarks.
#[derive(Debug)]
pub struct ScaleBenchBuilder<R: ReplicaTrait>
where
    <R::D as Dispatch>::WriteOperation: Send,
    <R::D as Dispatch>::Response: Send,
    <R::D as Dispatch>::ReadOperation: Send,
    R::D: 'static + Sync + Dispatch + Default,
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
    /// Reset the log between different `execute`
    /// If we have many ops and do tests where there is no GC, we may starve
    reset_log: bool,
    /// Operations executed on the log.
    operations:
        Vec<Operation<<R::D as Dispatch>::ReadOperation, <R::D as Dispatch>::WriteOperation>>,
    /// Marker for R
    _marker: PhantomData<R>,
}

impl<R: 'static + ReplicaTrait> ScaleBenchBuilder<R>
where
    R::D: Dispatch + Default + Send + Sync,
    <R::D as Dispatch>::WriteOperation: Send,
    <R::D as Dispatch>::WriteOperation: Sync,
    <R::D as Dispatch>::ReadOperation: Sync,
    <R::D as Dispatch>::ReadOperation: Send,
    <R::D as Dispatch>::Response: Send,
    <R::D as Dispatch>::ResponseError: Send,
{
    /// Initialize an "empty" ScaleBenchBuilder with a  MiB log.
    ///
    /// By default this won't execute any runs,
    /// you have to at least call `threads`, `thread_mapping`
    /// `replica_strategy` once and set `operations`.
    pub fn new(
        ops: Vec<Operation<<R::D as Dispatch>::ReadOperation, <R::D as Dispatch>::WriteOperation>>,
    ) -> ScaleBenchBuilder<R> {
        ScaleBenchBuilder {
            replica_strategies: Vec::new(),
            thread_mappings: Vec::new(),
            threads: Vec::new(),
            batches: vec![1usize],
            sync: true,
            log_size: 1024 * 1024 * 2,
            reset_log: false,
            operations: ops,
            _marker: PhantomData,
        }
    }

    /// Configures the builder automatically based on the underlying machine properties.
    pub fn machine_defaults(&mut self) -> &mut Self {
        self.thread_mapping(ThreadMapping::Sequential);
        self.replica_strategy(ReplicaStrategy::One);
        self.replica_strategy(ReplicaStrategy::Socket);
        self.replica_strategy(ReplicaStrategy::L1);
        self.thread_defaults()
    }

    pub fn thread_defaults(&mut self) -> &mut Self {
        let topology = MachineTopology::new();
        let max_cores = topology.cores();

        // On larger machines thread increments are bigger than on
        // smaller machines:
        let thread_incremements = if max_cores > 120 {
            16
        } else if max_cores > 24 {
            8
        } else if max_cores > 16 {
            4
        } else {
            2
        };

        for t in (0..(max_cores + 1)).step_by(thread_incremements) {
            if t == 0 {
                // Can't run on 0 threads
                self.threads(t + 1);
            } else {
                self.threads(t);
            }
        }

        // Go in increments of one around "interesting" socket boundaries
        let sockets = topology.sockets();
        let cores_on_s0 = topology.cpus_on_socket(sockets[0]);
        let cores_per_socket = cores_on_s0.len();
        for i in 0..sockets.len() {
            let multiplier = i + 1;
            fn try_add(to_add: usize, max_cores: usize, cur_threads: &mut Vec<usize>) {
                if !cur_threads.contains(&to_add) && to_add <= max_cores {
                    cur_threads.push(to_add);
                } else {
                    trace!("Didn't add {} threads", to_add);
                }
            }

            let core_socket_boundary = multiplier * cores_per_socket;
            try_add(core_socket_boundary - 1, max_cores, &mut self.threads);
            try_add(core_socket_boundary, max_cores, &mut self.threads);
            try_add(core_socket_boundary + 1, max_cores, &mut self.threads);
        }

        self.threads.sort();
        self
    }

    /// Run benchmark with batching of size `b`.
    pub fn add_batch(&mut self, b: usize) -> &mut Self {
        self.batches.push(b);
        self
    }

    pub fn update_batch(&mut self, b: usize) -> &mut Self {
        self.batches.clear();
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
    /// TestHarness will be configured to create a run for every
    /// possible triplet: (replica strategy, thread mapping, #threads).
    pub(crate) fn configure(&self, c: &mut TestHarness, name: &str, f: BenchFn<R>)
    where
        R: ReplicaTrait + Sync + Send,
        <R::D as Dispatch>::WriteOperation: Send + Sync,
        <R::D as Dispatch>::ReadOperation: Send + Sync,
        <R::D as Dispatch>::Response: Send + Sync,
        <R::D as Dispatch>::ResponseError: Send + Sync,
        R::D: 'static + Send + Sync,
    {
        let topology = MachineTopology::new();
        utils::disable_dvfs();
        println!("{}", name);

        let mut group = c.benchmark_group(name);
        for rs in self.replica_strategies.iter() {
            for tm in self.thread_mappings.iter() {
                for ts in self.threads.iter() {
                    for b in self.batches.iter() {
                        let log = Arc::new(Log::<<R::D as Dispatch>::WriteOperation>::new(
                            self.log_size,
                        ));
                        let mut runner = ScaleBenchmark::<R>::new(
                            String::from(name),
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
                        runner.startup();

                        let name = format!("{:?} {:?} BS={}", *rs, *tm, *b);
                        group.bench_with_input(
                            BenchmarkId::new(name, *ts),
                            &runner,
                            |cb, runner| {
                                cb.iter_custom(|duration| runner.execute(duration, self.reset_log))
                            },
                        );

                        runner
                            .terminate()
                            .expect("Couldn't terminate the experiment");
                    }
                }
            }
        }
    }
}
