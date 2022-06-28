// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Utility functions to do multi-threaded benchmarking of the log infrastructure.
#![allow(dead_code)]

use std::fmt::Debug;

pub mod benchmark;
pub mod topology;

/// A wrapper type to distinguish between arbitrary generated read or write operations
/// in the test harness.
#[derive(Clone)]
pub enum Operation<R: Sized, W: Sized + Clone + PartialEq> {
    ReadOperation(R),
    WriteOperation(W),
}

impl Debug for Operation<(), ()> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::ReadOperation(_) => write!(f, "ReadOperation"),
            Operation::WriteOperation(_) => write!(f, "WriteOperation"),
        }
    }
}

/// Type to identify an OS thread.
/// Ideally in our benchmark we should have one OS thread per core.
/// On MacOS this is not guaranteed.
pub type ThreadId = u64;

// Pin a thread to a core
#[cfg(target_os = "linux")]
pub fn pin_thread(core_id: topology::Cpu) {
    core_affinity::set_for_current(core_affinity::CoreId {
        id: core_id as usize,
    });
}

#[cfg(not(target_os = "linux"))]
pub fn pin_thread(_core_id: topology::Cpu) {
    log::warn!("Can't pin threads explicitly for benchmarking.");
}

#[cfg(target_os = "linux")]
pub fn disable_dvfs() {
    use std::process;

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
    log::warn!("Can't disable DVFS, expect non-optimal test results!");
}
