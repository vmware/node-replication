extern crate env_logger;

use chashmap::CHashMap;

use cnr::Dispatch;
use cnr::Log;
use cnr::LogMapper;
use cnr::Replica;

use std::sync::{Arc, Barrier};
use std::thread;

struct CNRHashmap {
    hashmap: CHashMap<usize, usize>,
}

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
enum OpRd {
    Get(usize),
}

impl LogMapper for OpRd {
    fn hash(&self, nlogs: usize, logs: &mut Vec<usize>) {
        match self {
            OpRd::Get(k) => logs.push(*k % nlogs),
        }
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
enum OpWr {
    Put(usize, usize),
    PutScan(usize, usize),
}

impl LogMapper for OpWr {
    fn hash(&self, nlogs: usize, logs: &mut Vec<usize>) {
        match self {
            OpWr::Put(k, _v) => logs.push(*k % nlogs),
            OpWr::PutScan(_k, _v) => {
                for i in 0..nlogs {
                    logs.push(i);
                }
            }
        }
    }
}

impl Default for CNRHashmap {
    fn default() -> Self {
        let capacity = 1_00_000;
        let hashmap = CHashMap::with_capacity(capacity);
        for i in 0..capacity {
            hashmap.insert(i, i + 1);
        }
        CNRHashmap { hashmap }
    }
}

impl Dispatch for CNRHashmap {
    type ReadOperation = OpRd;
    type WriteOperation = OpWr;
    type Response = Option<usize>;

    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            OpRd::Get(key) => Some(*self.hashmap.get(&key).unwrap()),
        }
    }

    fn dispatch_mut(&self, op: Self::WriteOperation) -> Self::Response {
        match op {
            OpWr::Put(key, val) => self.hashmap.insert(key, val),
            OpWr::PutScan(key, val) => self.hashmap.insert(key, val),
        }
    }
}

fn setup(nlogs: usize, nreplicas: usize, nops: usize, nthreads: usize) {
    let _r = env_logger::try_init();
    let barrier = Arc::new(Barrier::new(nthreads));

    let mut logs = Vec::with_capacity(nlogs);
    let mut replicas = Vec::with_capacity(nreplicas);

    for i in 0..nlogs {
        let log = Arc::new(Log::<<CNRHashmap as Dispatch>::WriteOperation>::new(
            4 * 1024 * 1024,
            i + 1,
        ));
        logs.push(log.clone());
    }

    for _i in 0..nreplicas {
        replicas.push(Replica::<CNRHashmap>::new(logs.clone()))
    }

    // Spawn the threads
    let mut threads = Vec::new();
    for i in 0..nthreads {
        let replica = replicas[i % nreplicas].clone();
        let b = barrier.clone();

        let t = thread::spawn(move || {
            let idx = replica.register().unwrap();
            b.wait();

            for i in 0..nops {
                match i % 3 {
                    0 => replica.execute_mut_scan(OpWr::PutScan(i, idx.id()), idx),
                    1 => replica.execute_mut(OpWr::Put(i, idx.id()), idx),
                    2 => replica.execute(OpRd::Get(i), idx),
                    _ => unreachable!(),
                };
            }

            b.wait();
        });
        threads.push(t);
    }

    // Join threads
    for thread in threads.into_iter() {
        thread.join().expect("Thread didn't finish successfully.");
    }
}

#[test]
fn single_replica_single_log_mix1() {
    let nlogs = 1;
    let nreplicas = 1;
    let nops = 1000;
    let nthreads = 1;
    setup(nlogs, nreplicas, nops, nthreads);
}

#[test]
fn single_replica_single_log_mix2() {
    let nlogs = 1;
    let nreplicas = 1;
    let nops = 1000;
    let nthreads = 8;
    setup(nlogs, nreplicas, nops, nthreads);
}

#[test]
fn single_replica_multi_log_mix() {
    let nlogs = 4;
    let nreplicas = 1;
    let nops = 1000;
    let nthreads = 8;
    setup(nlogs, nreplicas, nops, nthreads);
}

#[test]
fn multi_replica_single_log_mix1() {
    let nlogs = 1;
    let nreplicas = 2;
    let nops = 1000;
    let nthreads = 2;
    setup(nlogs, nreplicas, nops, nthreads);
}

#[test]
fn multi_replica_single_log_mix2() {
    let nlogs = 1;
    let nreplicas = 2;
    let nops = 1000;
    let nthreads = 8;
    setup(nlogs, nreplicas, nops, nthreads);
}

#[test]
fn multi_replica_multi_log_mix() {
    let nlogs = 4;
    let nreplicas = 2;
    let nops = 1000;
    let nthreads = 8;
    setup(nlogs, nreplicas, nops, nthreads);
}
