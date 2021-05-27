// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![feature(is_sorted)]

extern crate env_logger;

use chashmap::CHashMap;

use cnr::Dispatch;
use cnr::Log;
use cnr::LogMapper;
use cnr::Replica;

use std::cell::RefCell;
use std::sync::Arc;

/// Maximum size of the hashmap.
const CAPACITY: usize = 10_000_000;

/// Number of logs used in the tests.
const NLOGS: usize = 4;

struct CNRHashmap {
    hashmap: CHashMap<usize, usize>,
    lookup: Vec<RefCell<Vec<usize>>>,
    inserted: Vec<RefCell<Vec<usize>>>,
    inserted_scan: Vec<RefCell<Vec<usize>>>,
}

impl CNRHashmap {
    pub fn get(&self, key: usize) -> Option<usize> {
        self.lookup[key % NLOGS].borrow_mut().push(key);
        Some(*self.hashmap.get(&key).unwrap())
    }

    pub fn insert(&self, key: usize, val: usize) -> Option<usize> {
        self.inserted[key % NLOGS].borrow_mut().push(key);
        self.hashmap.insert(key, val)
    }

    pub fn insert_scan(&self, key: usize, val: usize) -> Option<usize> {
        self.inserted_scan[key % NLOGS].borrow_mut().push(key);
        self.hashmap.insert(key, val)
    }
}

unsafe impl Sync for CNRHashmap {}
unsafe impl Send for CNRHashmap {}

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
enum OpRd {
    Get(usize),
}

impl LogMapper for OpRd {
    fn hash(&self, nlogs: usize, logs: &mut Vec<usize>) {
        logs.clear();
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
        logs.clear();
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
        let mut lookup = Vec::with_capacity(2);
        let mut inserted = Vec::with_capacity(2);
        let mut inserted_scan = Vec::with_capacity(2);

        for _i in 0..NLOGS {
            lookup.push(RefCell::new(Vec::new()));
            inserted.push(RefCell::new(Vec::new()));
            inserted_scan.push(RefCell::new(Vec::new()));
        }
        CNRHashmap {
            hashmap: CHashMap::with_capacity(CAPACITY),
            lookup,
            inserted,
            inserted_scan,
        }
    }
}

impl Dispatch for CNRHashmap {
    type ReadOperation = OpRd;
    type WriteOperation = OpWr;
    type Response = Option<usize>;

    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            OpRd::Get(key) => self.get(key),
        }
    }

    fn dispatch_mut(&self, op: Self::WriteOperation) -> Self::Response {
        match op {
            OpWr::Put(key, val) => self.insert(key, val),
            OpWr::PutScan(key, val) => self.insert_scan(key, val),
        }
    }
}

#[test]
fn sequential_test_mut_order() {
    let nlogs = NLOGS;
    let mut logs = Vec::with_capacity(nlogs);

    // Allocate the logs.
    for i in 0..nlogs {
        let log = Arc::new(Log::<<CNRHashmap as Dispatch>::WriteOperation>::new(
            4 * 1024 * 1024,
            i + 1,
        ));
        logs.push(log.clone());
    }

    // Allocate the replica.
    let replica = Replica::<CNRHashmap>::new(logs.clone());

    let idx = replica.register().unwrap();
    for i in 0..CAPACITY {
        replica.execute_mut(OpWr::Put(i, i), idx);
    }

    let v = |data: &CNRHashmap| {
        for i in 0..nlogs {
            assert!(data.inserted[i].borrow().is_sorted());
        }
    };

    replica.verify(v);
}

#[test]
fn parallel_test_mut_order() {}

#[test]
fn sequential_test_scan_order() {
    let nlogs = NLOGS;
    let mut logs = Vec::with_capacity(nlogs);

    // Allocate the logs.
    for i in 0..nlogs {
        let log = Arc::new(Log::<<CNRHashmap as Dispatch>::WriteOperation>::new(
            4 * 1024 * 1024,
            i + 1,
        ));
        logs.push(log.clone());
    }

    // Allocate the replicas.
    let replica = Replica::<CNRHashmap>::new(logs.clone());

    let idx = replica.register().unwrap();
    for i in 0..CAPACITY {
        replica.execute_mut_scan(OpWr::PutScan(i, i), idx);
    }

    let v = |data: &CNRHashmap| {
        for i in 0..nlogs {
            assert!(data.inserted_scan[i].borrow().is_sorted());
        }
    };

    replica.verify(v);
}

#[test]
fn parallel_test_scan_order() {}

#[test]
fn sequential_test_mut2scan_order() {}

#[test]
fn parallel_test_mut2scan_order() {}
