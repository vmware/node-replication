// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Replays a system-call trace against a vspace implementation to
//! evaluate performance of a replicated x86-64 address space.
#![allow(unused)]
#![feature(alloc_layout_extra, thread_local)]

#[macro_use]
extern crate log;
extern crate alloc;

use std::fs::File;
use std::io;
use std::io::prelude::*;

use node_replication::Dispatch;

mod mkbench;
mod os_workload;
mod utils;

use criterion::{criterion_group, criterion_main, Criterion};
use mkbench::{ReplicaStrategy, ThreadMapping};
use os_workload::kpi::{ProcessOperation, SystemCall, VSpaceOperation};

/// Operations that go on the log
///
/// We conveniently use the same format for the parsing of our trace files.
#[derive(Copy, Clone, Debug, PartialEq)]
enum Opcode {
    /// An operation on the process.
    Process(ProcessOperation, u64, u64, u64, u64),
    /// An operation on the vspace (of the process).
    VSpace(VSpaceOperation, u64, u64, u64, u64),
    /// A no-op, we should never encounter this in dispatch!
    Empty,
}

impl Default for Opcode {
    fn default() -> Opcode {
        Opcode::Empty
    }
}

/// A BespinDispatcher that does syscalls on the os_workload ported code-base.
#[derive(Default)]
struct BespinDispatcher;

impl Dispatch for BespinDispatcher {
    type Operation = Opcode;
    type Response = (u64, u64);
    type ResponseError = os_workload::KError;

    fn dispatch(&mut self, op: Self::Operation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            Opcode::Process(op, a1, a2, a3, a4) => {
                return os_workload::syscall_handle(
                    SystemCall::Process as u64,
                    op as u64,
                    a1,
                    a2,
                    a3,
                    a4,
                )
            }
            Opcode::VSpace(op, a1, a2, a3, a4) => {
                return os_workload::syscall_handle(
                    SystemCall::VSpace as u64,
                    op as u64,
                    a1,
                    a2,
                    a3,
                    a4,
                )
            }
            Opcode::Empty => unreachable!(),
        };
    }
}

/// A PosixDispatcher that does syscalls on the local host.
#[derive(Default)]
struct PosixDispatcher;

/// The implementation of the PosixDispatcher.
impl Dispatch for PosixDispatcher {
    type Operation = Opcode;
    type Response = ();
    type ResponseError = ();

    fn dispatch(&mut self, op: Self::Operation) -> Result<(), ()> {
        use nix::sys::mman::{MapFlags, ProtFlags};

        match op {
            Opcode::Process(pop, _a, _b, _c, _d) => match pop {
                ProcessOperation::AllocateVector => Err(()),
                ProcessOperation::Exit => Err(()),
                ProcessOperation::InstallVCpuArea => Err(()),
                ProcessOperation::Log => Err(()),
                ProcessOperation::SubscribeEvent => Err(()),
                ProcessOperation::Unknown => {
                    unreachable!("Got a ProcessOperation::Unknown in dispatch")
                }
            },
            Opcode::VSpace(vop, a, b, _c, _d) => {
                match vop {
                    VSpaceOperation::Identify => Err(()),
                    VSpaceOperation::Map => {
                        let base = a;
                        let size: nix::libc::size_t = b as nix::libc::size_t;
                        debug!("VSpaceOperation::Map base {:#x} {:#x}", base, size);
                        let res = unsafe {
                            nix::sys::mman::mmap(
                                base as *mut std::ffi::c_void, //0 as *mut std::ffi::c_void,
                                size,
                                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                                MapFlags::MAP_ANON | MapFlags::MAP_PRIVATE, //| MapFlags::MAP_FIXED,
                                -1 as std::os::unix::io::RawFd,
                                0 as nix::libc::off_t,
                            )
                        };

                        match res {
                            Ok(m) => {
                                trace!("mmap worked {:p}", m);
                                Ok(())
                            }
                            Err(e) => {
                                error!("mmap failed to map: {}", e);
                                Err(())
                            }
                        };
                        Err(())
                    }
                    VSpaceOperation::MapDevice => Err(()),
                    VSpaceOperation::Unmap => {
                        let base = a;
                        let size: nix::libc::size_t = b as nix::libc::size_t;
                        debug!("VSpaceOperation::Unmap base {:#x} {:#x}", base, size);
                        let res =
                            unsafe { nix::sys::mman::munmap(base as *mut std::ffi::c_void, size) };

                        match res {
                            Ok(()) => {
                                trace!("munmap worked");
                                Ok(())
                            }
                            Err(e) => {
                                error!("mmap failed to unmap: {}", e);
                                Err(())
                            }
                        };
                        Err(())
                    }
                    VSpaceOperation::Unknown => {
                        unreachable!("Got a VSpaceOperation::Unknown in dispatch")
                    }
                }
            }
            Opcode::Empty => unreachable!("Got an Opcode::Empty in dispatch"),
        }
    }
}

/// Parses syscall trace files and returns them as a vector
/// of `Opcode`.
///
/// `file` is a path to the trace, relative to the base-dir
/// of the repository.
fn parse_syscall_trace(file: &str) -> io::Result<Vec<Opcode>> {
    let file = File::open(file)?;
    let reader = io::BufReader::new(file);
    let mut ops: Vec<Opcode> = Vec::with_capacity(3000);

    // Parses lines that look like this:
    // ```no-run
    // syscall: Process Log 32272690 2 0 2
    // syscall: VSpace Identify 296775680 0 287430656 1
    // ```
    for line in reader.lines() {
        let line = line.unwrap();
        let words: Vec<&str> = line.split_whitespace().collect();
        assert!(words.len() == 7);

        assert_eq!(words[0], "syscall:");
        let sc = words[1]; // Process or VSpace
        let op = words[2]; // Log, Map, Unmap, Identify etc.
        let a1 = words[3].parse::<u64>().unwrap_or(0);
        let a2 = words[4].parse::<u64>().unwrap_or(0);
        let a3 = words[5].parse::<u64>().unwrap_or(0);
        let a4 = words[6].parse::<u64>().unwrap_or(0);

        let opcode = match SystemCall::from(sc) {
            SystemCall::Process => Opcode::Process(ProcessOperation::from(op), a1, a2, a3, a4),
            SystemCall::VSpace => Opcode::VSpace(VSpaceOperation::from(op), a1, a2, a3, a4),
            _ => unreachable!(),
        };

        ops.push(opcode);
    }

    Ok(ops)
}

/// A simple benchmark that takes a bunch of syscall operations
/// and then replays them using on our `os_workload` implementation
/// of a syscall handler code.
fn bespin_vspace_single_threaded(c: &mut Criterion) {
    env_logger::try_init();

    const LOG_SIZE_BYTES: usize = 16 * 1024 * 1024;

    let ops = parse_syscall_trace("benches/os_workload/bsd_init.log").unwrap();
    mkbench::baseline_comparison::<BespinDispatcher>(c, "vspace", ops, LOG_SIZE_BYTES);
}

/// A simple benchmark that takes a bunch of syscall operations
/// and replays them on our OS as a apples-to-oranges comparison
fn posix_vspace_single_threaded(c: &mut Criterion) {
    env_logger::try_init();

    const LOG_SIZE_BYTES: usize = 16 * 1024 * 1024;

    let ops = parse_syscall_trace("benches/os_workload/bsd_init.log").unwrap();
    mkbench::baseline_comparison::<PosixDispatcher>(c, "posix-vspace", ops, LOG_SIZE_BYTES);
}

fn vspace_scale_out(c: &mut Criterion) {
    env_logger::try_init();

    let ops = parse_syscall_trace("benches/os_workload/bsd_init.log").unwrap();

    mkbench::ScaleBenchBuilder::<BespinDispatcher>::new(ops)
        .machine_defaults()
        .configure(
            c,
            "vspace-scaleout",
            |_cid, rid, _log, replica, ops, _batch_size| {
                let mut o = vec![];
                for op in ops {
                    replica.execute(*op, rid);
                    let mut i = 1;
                    while replica.get_responses(rid, &mut o) == 0 {
                        if i % mkbench::WARN_THRESHOLD == 0 {
                            log::warn!(
                                "{:?} Waiting too long for get_responses",
                                std::thread::current().id()
                            );
                        }
                        i += 1;
                    }
                    o.clear();
                }
            },
        );
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bespin_vspace_single_threaded, posix_vspace_single_threaded, vspace_scale_out
);

criterion_main!(benches);
