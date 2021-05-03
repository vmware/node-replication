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
    fn hash(&self) -> usize {
        0
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
enum OpWr {
    Put(usize, usize),
}

impl LogMapper for OpWr {
    fn hash(&self) -> usize {
        0
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
        }
    }
}

#[test]
fn multiple_replica_scans() {
    let _r = env_logger::try_init();

    let nlogs = 4;
    let nreplicas = 2;
    let nops = 100;
    let nthreads = 8;
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
                replica.execute_mut_scan(OpWr::Put(i, idx.id()), idx);
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
