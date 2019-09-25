// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Utility functions to do multi-threaded benchmarking of the log infrastructure.

use std::cell::RefCell;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use criterion::black_box;
use hwloc::Topology;
use log::warn;

use node_replication::log::Log;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ThreadMapping {
    /// Don't do any pinning.
    None,
    /// Keep threads of a replica on the same socket (as much as possible).
    Sequential,
    /// Spread threads of a replica out across sockets.
    Interleave,
}

/// Generic benchmark configuration parameters for node-replication.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct BenchConfig {
    /// Number of threads
    threads: usize,
    /// How to map threads to cores
    thread_strategy: ThreadMapping,
    /// How to allocate replicas
    replica_strategy: ReplicaStrategy,
    /// Size of the log in bytes
    log_size: usize,
    /// Amount of iterations/operations per benchmark
    ops: usize,
}

pub type CoreId = usize;
pub type ThreadId = usize;

thread_local! {
    pub static CORE_ID: RefCell<CoreId> = RefCell::new(0);
}

pub fn set_core_id(cid: CoreId) {
    CORE_ID.with(|core_id| *core_id.borrow_mut() = cid);
}

#[cfg(not(target_os = "linux"))]
#[allow(unused)]
pub fn get_core_id() -> CoreId {
    CORE_ID.with(|cid| cid.borrow().clone())
}

#[cfg(target_os = "linux")]
pub fn get_core_id() -> CoreId {
    let ids = core_affinity::get_core_ids().unwrap();
    assert_eq!(ids.len(), 1);
    ids[0].id
}

// Pin a thread to a core
#[cfg(target_os = "linux")]
pub fn pin_thread(core_id: CoreId) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

#[cfg(not(target_os = "linux"))]
pub fn pin_thread(_core_id: CoreId) {
    warn!("Can't pin threads on MacOS core_affinity crate has a bug!");
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MappingStrategy {
    /// None
    Identity,
}

pub fn thread_mapping(strategy: MappingStrategy, thread_number: ThreadId) -> CoreId {
    match strategy {
        MappingStrategy::Identity => thread_number,
    }
}

#[cfg(target_os = "linux")]
pub fn disable_dvfs() {
    let o = process::Command::new("sh")
        .args(&[
            "-s",
            "echo performance | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor",
        ])
        .output()
        .expect("failed to change scaling governor");
    assert!(o.status.success());
}

#[cfg(not(target_os = "linux"))]
pub fn disable_dvfs() {
    warn!("Can't disable DVFS, expect non-optimal test results!");
}

#[allow(unused)]
pub fn debug_topology() {
    let topo = Topology::new();
    for i in 0..topo.depth() {
        println!("*** Objects at level {}", i);

        for (idx, object) in topo.objects_at_depth(i).iter().enumerate() {
            println!("{}: {:?}", idx, object.os_index());
        }
    }
}

#[allow(unused)]
pub fn generic_log_bench(
    iters: u64,
    thread_num: usize,
    batch: usize,
    log: &Arc<Log<'static, usize>>,
    operations: &Arc<Vec<usize>>,
    f: fn(&Arc<Log<usize>>, &Arc<Vec<usize>>, usize) -> (),
) -> Duration {
    // Need a barrier to synchronize starting of threads
    let barrier = Arc::new(Barrier::new(thread_num));
    // Thread handles to `join` them at the end
    let mut handles = Vec::with_capacity(thread_num);
    // Our strategy on how we pin the threads to cores
    let mapping = MappingStrategy::Identity;

    // TODO: Remove once we have GC
    unsafe {
        log.reset();
    }

    // Spawn n-1 threads that perform the same benchmark function:
    for thread_id in 1..thread_num {
        let c = barrier.clone();
        let ops = operations.clone();
        let log = log.clone();
        handles.push(thread::spawn(move || {
            let core_id = thread_mapping(mapping, thread_id);
            pin_thread(core_id);
            set_core_id(core_id);
            for _i in 0..iters {
                c.wait();
                black_box(f(&log, &ops, batch));
                c.wait();
            }
        }));
    }

    // Spawn the master thread that performs the measurements:
    let c = barrier.clone();
    let thread_id: usize = 0;
    pin_thread(thread_mapping(mapping, thread_id));
    let log = log.clone();
    let mut elapsed = Duration::new(0, 0);
    for _i in 0..iters {
        // Wait for other threads to start
        c.wait();
        let start = Instant::now();
        black_box(f(&log, &operations, batch));
        c.wait();
        elapsed = elapsed + start.elapsed();
    }

    // Wait for other threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    elapsed
}

/// Takes a generic data-structure that implements dispatch and a vector of operations
/// to execute against said data-structure.
///
/// It configures the supplied criterion runner to do two benchmarks:
/// - Running the DS operations on a single-thread directly against the DS.
/// - Running the DS operation on a single-thread but go through a replica/log.
///
/// Use this function to evalute the overhead the log adds for a given data-structure.
pub fn baseline_comparison_benchmark<T: Dispatch + Default>(
    c: &mut Criterion,
    name: &str,
    ops: Vec<<T as Dispatch>::Operation>,
) {
    let s: T = Default::default();

    // First benchmark is just a stack on a single thread:
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(ops.len() as u64));
    group.bench_function("baseline", |b| {
        b.iter(|| {
            for i in 0..ops.len() {
                s.dispatch(ops[i]);
            }
        })
    });

    // 2nd benchmark we compare the stack but now we put a log in front:
    let log = Arc::new(Log::<<T as Dispatch>::Operation>::new(LOG_SIZE_BYTES));
    let r = Replica::<T>::new(&log);
    let ridx = r.register().expect("Failed to register with Replica.");

    group.bench_function("log", |b| {
        b.iter(|| {
            for i in 0..ops.len() {
                r.execute(ops[i], ridx);
            }
        })
    });

    group.finish();
}
