use std::process;

use hwloc::Topology;
use log::{error, warn};

use std::cell::RefCell;

pub type CoreId = usize;
pub type ThreadId = usize;

thread_local! {
    pub static CORE_ID: RefCell<CoreId> = RefCell::new(0);
}

pub fn set_core_id(cid: CoreId) {
    CORE_ID.with(|core_id| *core_id.borrow_mut() = cid);
}

#[cfg(not(target_os = "linux"))]
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
    warn!("Can't disable DVFS, expect flaky test results!");
}

#[allow(unused)]
pub fn debug_topology() {
    let topo = Topology::new();
    for i in 0..topo.depth() {
        println!("*** Objects at level {}", i);

        for (idx, object) in topo.objects_at_depth(i).iter().enumerate() {
            //println!("{}", object.name());

            println!("{}: {:?}", idx, object.os_index());
        }
    }
}
