#![feature(test)]
#![feature(get_mut_unchecked)]
#![feature(bench_black_box)]

mod mkbench;
mod utils;

use crate::mkbench::ReplicaTrait;
use cnr::{Dispatch, LogMapper, Replica};
use nrfs::*;
use std::cell::UnsafeCell;
use utils::benchmark::*;
use utils::topology::*;
use utils::Operation;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum OpRd {
    FileRead(u64),
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum OpWr {
    FileWrite(u64),
}

impl LogMapper for OpRd {
    fn hash(&self, nlogs: usize, logs: &mut Vec<usize>) {
        match self {
            OpRd::FileRead(fd) => logs.push((*fd - 1) as usize % nlogs),
        }
    }
}

impl LogMapper for OpWr {
    fn hash(&self, nlogs: usize, logs: &mut Vec<usize>) {
        match self {
            OpWr::FileWrite(fd) => logs.push((*fd - 1) as usize % nlogs),
        }
    }
}

struct NrFilesystem {
    memfs: MemFS,
    write_buffer: Vec<Vec<u8>>,
    read_buffer: Vec<UnsafeCell<Vec<u8>>>,
}

impl Default for NrFilesystem {
    fn default() -> NrFilesystem {
        let topology = MachineTopology::new();
        let sockets = topology.sockets();
        let num_cpus = topology.cpus_on_socket(sockets[0]).len();

        let memfs = MemFS::default();
        //Create a private file for each core.
        let buffer = vec![0xb; 4096];
        for i in 0..num_cpus {
            let filename = format!("file-{}", i);
            match memfs.create(&filename, u64::from(FileModes::S_IRWXU)) {
                Ok(mnode_num) => {
                    memfs
                        .write(mnode_num, &buffer, 0)
                        .expect("Issue in writing to the file");
                }
                Err(e) => unreachable!("{}", e),
            }
        }

        let write_buffer = vec![vec![0xb; 4096]; 96];
        let mut read_buffer = Vec::with_capacity(96);
        for _i in 0..96 {
            read_buffer.push(UnsafeCell::new(vec![0; 4096]));
        }
        NrFilesystem {
            memfs,
            write_buffer,
            read_buffer,
        }
    }
}

unsafe impl Sync for NrFilesystem {}

impl Dispatch for NrFilesystem {
    type ReadOperation = OpRd;
    type WriteOperation = OpWr;
    type Response = Result<usize, ()>;

    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            OpRd::FileRead(memnode) => {
                match self.memfs.read(
                    memnode,
                    unsafe { &mut *self.read_buffer[memnode as usize - 1].get() },
                    0,
                ) {
                    Ok(len) => Ok(len),
                    Err(e) => unreachable!("Memnode {} Err {:?}", memnode, e),
                }
            }
        }
    }

    fn dispatch_mut(&self, op: Self::WriteOperation) -> Self::Response {
        match op {
            OpWr::FileWrite(memnode) => {
                match self
                    .memfs
                    .write(memnode, &self.write_buffer[memnode as usize - 1], 0)
                {
                    Ok(len) => Ok(len),
                    Err(e) => unreachable!("Memnode {} Err {:?}", memnode, e),
                }
            }
        }
    }
}

fn generate_nrfs_ops(write_ratio: usize) -> Vec<Operation<OpRd, OpWr>> {
    let nop = 10000;
    let mut ops = Vec::with_capacity(nop);

    for idx in 0..nop {
        if idx % 100 < write_ratio {
            ops.push(Operation::WriteOperation(OpWr::FileWrite(0)));
        } else {
            ops.push(Operation::ReadOperation(OpRd::FileRead(0)));
        }
    }
    ops
}

fn nrfs_scale_out(c: &mut TestHarness, num_cpus: usize, write_ratio: usize) {
    let ops = generate_nrfs_ops(write_ratio);
    let logs = num_cpus;
    let bench_name = format!("nrfs-mlnr{}-scaleout-wr{}", logs, write_ratio);

    mkbench::ScaleBenchBuilder::<Replica<NrFilesystem>>::new(ops)
        .thread_defaults()
        .replica_strategy(mkbench::ReplicaStrategy::Socket)
        .update_batch(128)
        .thread_mapping(ThreadMapping::Sequential)
        .log_strategy(mkbench::LogStrategy::Custom(logs))
        .configure(
            c,
            &bench_name,
            |_cid, rid, _log, replica, op, _batch_size| match op {
                Operation::ReadOperation(op) => {
                    let op = match op {
                        OpRd::FileRead(_mnode) => OpRd::FileRead(rid.id() as u64 + 1),
                    };
                    let _ignore = replica.exec_ro(op, rid);
                }
                Operation::WriteOperation(op) => {
                    let op = match op {
                        OpWr::FileWrite(_mnode) => OpWr::FileWrite(rid.id() as u64 + 1),
                    };
                    let _ignore = replica.exec(op, rid);
                }
            },
        );
}

fn main() {
    let _r = env_logger::try_init();
    utils::disable_dvfs();

    let mut harness = Default::default();
    // This translate to drbl and dwol in fxmark.
    let write_ratios = vec![0, 100];

    let topology = MachineTopology::new();
    let sockets = topology.sockets();
    let num_cpus = topology.cpus_on_socket(sockets[0]).len();

    for write_ratio in write_ratios.into_iter() {
        nrfs_scale_out(&mut harness, num_cpus, write_ratio);
    }
}
