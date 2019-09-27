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
    ids[0].id as topology::Cpu
}

// Pin a thread to a core
#[cfg(target_os = "linux")]
pub fn pin_thread(core_id: topology::Cpu) {
    core_affinity::set_for_current(core_affinity::CoreId {
        id: core_id as usize,
    });
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
