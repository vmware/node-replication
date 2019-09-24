// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Utility functions to do multi-threaded benchmarking of the log infrastructure.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use std::cell::RefCell;

use hwloc::Topology;
use log::{warn};
use criterion::black_box;

use node_replication::log::Log;


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
