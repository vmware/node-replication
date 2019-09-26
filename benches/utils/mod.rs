// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Utility functions to do multi-threaded benchmarking of the log infrastructure.
use log::warn;
use std::cell::RefCell;
use std::process;

pub mod topology;

/// Type to identify an OS thread.
/// Ideally in our benchmark we should have one OS thread per core.
/// On MacOS this is not guaranteed.
pub type ThreadId = u64;

thread_local! {
    pub static CORE_ID: RefCell<topology::Cpu> = RefCell::new(0);
}

pub fn set_core_id(cid: topology::Cpu) {
    CORE_ID.with(|core_id| *core_id.borrow_mut() = cid);
}

#[cfg(not(target_os = "linux"))]
#[allow(unused)]
pub fn get_core_id() -> topology::Cpu {
    CORE_ID.with(|cid| cid.borrow().clone())
}

#[cfg(target_os = "linux")]
pub fn get_core_id() -> topology::Cpu {
    let ids = core_affinity::get_core_ids().unwrap();
    assert_eq!(ids.len(), 1);
    ids[0].id
}

// Pin a thread to a core
#[cfg(target_os = "linux")]
pub fn pin_thread(core_id: topology::Cpu) {
    core_affinity::set_for_current(core_affinity::topology::Cpu { id: core_id });
}

#[cfg(not(target_os = "linux"))]
pub fn pin_thread(_core_id: topology::Cpu) {
    warn!("Can't pin threads explicitly for benchmarking.");
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

// XXX remove
/*
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

    // Our strategy on how allocate cores
    let mapping = ThreadMapping::Sequential;

    unsafe {
        // TODO: Remove once we have GC
        log.reset();
    }

    // Spawn n-1 threads that perform the same benchmark function:
    for thread_id in 1..thread_num {
        let c = barrier.clone();
        let ops = operations.clone();
        let log = log.clone();
        handles.push(thread::spawn(move || {
            let core_id = thread_mapping(mapping, thread_id);
            utils::pin_thread(core_id);
            utils::set_core_id(core_id);

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
    utils::pin_thread(thread_mapping(mapping, thread_id));
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
}*/
