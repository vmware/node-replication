#![feature(alloc_layout_extra)]

#[macro_use]
extern crate criterion;
#[macro_use]
extern crate log;
extern crate alloc;
extern crate core_affinity;
extern crate hwloc;

use criterion::black_box;
use criterion::{Benchmark, Criterion, ParameterizedBenchmark, Throughput};

use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

mod utils;

use node_replication::{log::Log, replica::Replica, Dispatch};

mod os_workload;
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

    fn dispatch(&self, op: Self::Operation) -> Self::Response {
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
                .expect("Process syscall failed");
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
                .expect("VSpace syscall failed")
            }
            Opcode::Empty => unreachable!(),
        };
    }
}

fn fix_op(oper: &Opcode) -> Opcode {
    match oper {
        Opcode::VSpace(op, a1, a2, a3, a4) => match op {
            VSpaceOperation::Map => {
                let paddr = os_workload::get_paddr(*a2);
                return Opcode::VSpace(*op, *a1, *a2, paddr, *a2);
            }
            _ => return *oper,
        },
        _ => return *oper,
    }
}

/// A PosixDispatcher that does syscalls on the local host.
#[derive(Default)]
struct PosixDispatcher;

/// The implementation of the PosixDispatcher.
impl Dispatch for PosixDispatcher {
    type Operation = Opcode;
    type Response = ();

    fn dispatch(&self, op: Self::Operation) {
        use nix::sys::mman::{MapFlags, ProtFlags};

        match op {
            Opcode::Process(pop, _a, _b, _c, _d) => match pop {
                ProcessOperation::AllocateVector => {}
                ProcessOperation::Exit => {}
                ProcessOperation::InstallVCpuArea => {}
                ProcessOperation::Log => {}
                ProcessOperation::SubscribeEvent => {}
                ProcessOperation::Unknown => {
                    unreachable!("Got a ProcessOperation::Unknown in dispatch")
                }
            },
            Opcode::VSpace(vop, a, b, _c, _d) => {
                match vop {
                    VSpaceOperation::Identify => {}
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
                                ()
                            }
                            Err(e) => error!("mmap failed to map: {}", e),
                        };
                    }
                    VSpaceOperation::MapDevice => {}
                    VSpaceOperation::Unmap => {
                        let base = a;
                        let size: nix::libc::size_t = b as nix::libc::size_t;
                        debug!("VSpaceOperation::Unmap base {:#x} {:#x}", base, size);
                        let res =
                            unsafe { nix::sys::mman::munmap(base as *mut std::ffi::c_void, size) };

                        match res {
                            Ok(()) => {
                                trace!("munmap worked");
                                ()
                            }
                            Err(e) => error!("mmap failed to unmap: {}", e),
                        };
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

/// A NopDispatcher that ignores all our operations.
#[derive(Default)]
struct NopDispatcher;

/// The implementation of our NopDispatcher.
///
/// It doesn't do anything which is what it is supposed to do.
impl Dispatch for NopDispatcher {
    type Operation = Opcode;
    type Response = ();

    fn dispatch(&self, op: Self::Operation) -> Self::Response {
        match op {
            Opcode::Process(_op, _a, _b, _c, _d) => {}
            Opcode::VSpace(_op, _a, _b, _c, _d) => {}
            Empty => unreachable!(),
        }
    }
}

/// Parses syscall trace files and returns them as a vector
/// of `Opcode`.
///
/// `file` is a path to the trace, relative to the base-dir
/// of the repository.
fn parse_ops(file: &str) -> io::Result<Vec<Opcode>> {
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

/// Measures overhead of putting stuff on the log and no-op processing it at the same time.
fn log_exec(iters: u64, thread_num: usize, operations: &Arc<Vec<Opcode>>) -> Duration {
    // Need a barrier to synchronize starting of threads
    let barrier = Arc::new(Barrier::new(thread_num));
    // Thread handles to `join` them at the end
    let mut handles = Vec::with_capacity(thread_num);
    // Our strategy on how we pin the threads to cores
    let mapping = utils::MappingStrategy::Identity;

    let log = Arc::new(Log::<Opcode>::new(1024 * 1024 * 1024 * 4));
    let replica = Arc::new(Replica::<BespinDispatcher>::new(&log));

    // Spawn some load generator threads
    for thread_id in 1..thread_num {
        let c = barrier.clone();
        let ops = operations.clone();
        let replica = replica.clone();
        handles.push(thread::spawn(move || {
            let core_id = utils::thread_mapping(mapping, thread_id);
            utils::pin_thread(core_id);
            let idx = replica
                .register()
                .expect("Failed to register with Replica.");
            for _i in 0..iters {
                os_workload::kcb::set_kcb();
                c.wait();
                for op in ops.iter() {
                    assert_ne!(*op, Default::default());
                    replica.execute(fix_op(op), idx);
                }
                c.wait();
                os_workload::kcb::drop_kcb();
            }
        }));
    }

    let c = barrier.clone();
    let thread_id: usize = 0;
    utils::pin_thread(utils::thread_mapping(mapping, thread_id));

    //let replica = Arc::new(Replica::<NopDispatcher>::new(&log));
    let idx = replica
        .register()
        .expect("Failed to register with Replica.");

    // Measure on our base thread
    let mut elapsed = Duration::new(0, 0);
    for _i in 0..iters {
        // Wait for other threads to start
        os_workload::kcb::set_kcb();
        c.wait();
        let start = Instant::now();
        for op in operations.iter() {
            assert_ne!(*op, Default::default());
            replica.execute(fix_op(op), idx);
        }
        c.wait();
        elapsed = elapsed + start.elapsed();
        os_workload::kcb::drop_kcb();
    }

    // Wait for other threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    elapsed
}

/// Applies the syscalls as a series to the dummy kernel implementation in `os_workload`.
fn apply_syscalls(operations: &Arc<Vec<Opcode>>) {
    for op in operations.iter() {
        match fix_op(op) {
            Opcode::Process(op, a1, a2, a3, a4) => {
                os_workload::syscall_handle(SystemCall::Process as u64, op as u64, a1, a2, a3, a4)
                    .expect("Process syscall failed")
            }
            Opcode::VSpace(op, a1, a2, a3, a4) => {
                os_workload::syscall_handle(SystemCall::VSpace as u64, op as u64, a1, a2, a3, a4)
                    .expect("VSpace syscall failed")
            }
            Opcode::Empty => unreachable!(),
        };
    }
}

fn syscalls_without_log(iters: u64, thread_num: usize, operations: &Arc<Vec<Opcode>>) -> Duration {
    // Need a barrier to synchronize starting of threads
    let barrier = Arc::new(Barrier::new(thread_num));
    // Thread handles to `join` them at the end
    let mut handles = Vec::with_capacity(thread_num);
    // Our strategy on how we pin the threads to cores
    let mapping = utils::MappingStrategy::Identity;

    // Spawn some load generator threads
    for thread_id in 1..thread_num {
        let c = barrier.clone();
        let ops = operations.clone();
        handles.push(thread::spawn(move || {
            let core_id = utils::thread_mapping(mapping, thread_id);
            utils::pin_thread(core_id);
            utils::set_core_id(core_id);
            for _i in 0..iters {
                os_workload::kcb::set_kcb();
                c.wait();
                black_box(apply_syscalls(black_box(&ops)));
                c.wait();
                os_workload::kcb::drop_kcb();
            }
        }));
    }

    let c = barrier.clone();
    let thread_id: usize = 0;
    utils::pin_thread(utils::thread_mapping(mapping, thread_id));

    // Measure on our base thread
    let mut elapsed = Duration::new(0, 0);
    for _i in 0..iters {
        os_workload::kcb::set_kcb();
        // Wait for other threads to start
        c.wait();
        let start = Instant::now();
        black_box(apply_syscalls(black_box(&operations)));
        c.wait();
        elapsed = elapsed + start.elapsed();
        os_workload::kcb::drop_kcb();
    }

    // Wait for other threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    elapsed
}

fn log_overhead(c: &mut Criterion) {
    //env_logger::init();
    utils::disable_dvfs();

    let operations = Arc::new(parse_ops("benches/bsd_init.log").unwrap());
    let cpus = num_cpus::get();
    let elements = operations.len();

    let thread_counts: Vec<usize> = vec![1, 2];

    c.bench(
        "log",
        ParameterizedBenchmark::new(
            "overhead",
            move |b, thread_counts| {
                b.iter_custom(|iters| log_exec(iters, *thread_counts, &operations))
            },
            thread_counts,
        )
        .throughput(move |t| Throughput::Elements((t * elements) as u32)),
    );
}

/// A simple benchmark that takes a bunch of syscall operations
/// and then replays them using on our `os_workload` implementation
/// of a syscall handler code.
fn node_replication_benchmark(c: &mut Criterion) {
    env_logger::init();

    utils::disable_dvfs();
    let operations = Arc::new(parse_ops("benches/bsd_init.log").unwrap());
    let cpus = num_cpus::get();
    let elements = operations.len();

    let threads: Vec<usize> = vec![1, 2, 4];

    c.bench(
        "syscall",
        ParameterizedBenchmark::new(
            "no-log",
            move |b, threads| {
                b.iter_custom(|iters| syscalls_without_log(iters, *threads, &operations))
            },
            threads,
        )
        .throughput(move |t| Throughput::Elements((t * elements) as u32)),
    );
}

fn generic_log_bench(
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
    let mapping = utils::MappingStrategy::Identity;

    unsafe {
        log.reset();
    }

    // Spawn some load generator threads
    for thread_id in 1..thread_num {
        let c = barrier.clone();
        let ops = operations.clone();
        let log = log.clone();
        handles.push(thread::spawn(move || {
            let core_id = utils::thread_mapping(mapping, thread_id);
            utils::pin_thread(core_id);
            utils::set_core_id(core_id);
            for _i in 0..iters {
                os_workload::kcb::set_kcb();
                c.wait();
                black_box(f(&log, &ops, batch));
                c.wait();
                os_workload::kcb::drop_kcb();
            }
        }));
    }

    let c = barrier.clone();
    let thread_id: usize = 0;
    utils::pin_thread(utils::thread_mapping(mapping, thread_id));
    let log = log.clone();
    // Measure on our base thread
    let mut elapsed = Duration::new(0, 0);
    for _i in 0..iters {
        os_workload::kcb::set_kcb();
        // Wait for other threads to start
        c.wait();
        let start = Instant::now();
        black_box(f(&log, &operations, batch));
        c.wait();
        elapsed = elapsed + start.elapsed();
        os_workload::kcb::drop_kcb();
    }

    // Wait for other threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    elapsed
}

fn scale_log(log: &Arc<Log<usize>>, ops: &Arc<Vec<usize>>, batch: usize) {
    for batch_op in ops.rchunks(batch) {
        let r = log.append(batch_op);
        assert!(r.is_some());
    }
}

fn log_scale_bench(c: &mut Criterion) {
    //env_logger::init();

    utils::disable_dvfs();
    let mut operations = Vec::new();
    for e in 0..50000 {
        operations.push(e);
    }

    let operations = Arc::new(operations);
    let elements = operations.len();

    let threads_batchsize: Vec<(usize, usize)> =
        vec![(1, 1), (1, 8), (2, 1), (2, 8), (4, 1), (4, 8)];
    let log = Arc::new(Log::<usize>::new(1024 * 1024 * 1024 * 2));

    c.bench(
        "log",
        ParameterizedBenchmark::new(
            "append",
            move |b, (threads, batch)| {
                b.iter_custom(|iters| {
                    generic_log_bench(iters, *threads, *batch, &log, &operations, scale_log)
                })
            },
            threads_batchsize,
        )
        .throughput(move |(t, b)| Throughput::Elements((t * elements) as u32)),
    );
}

criterion_group!(osbench, node_replication_benchmark);
criterion_group!(logscale, log_scale_bench);
criterion_group!(
    name = logbench;
    config = Criterion::default().sample_size(8);
    targets = log_overhead
);
criterion_main!(osbench, logbench, logscale);
