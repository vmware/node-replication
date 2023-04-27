// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Various tests for node-replication with the help of a stack.
#![feature(generic_associated_types)]

extern crate rand;
extern crate std;

use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;
use std::usize;

use node_replication::nr::{Dispatch, Log, Replica};

use rand::{thread_rng, Rng};

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
enum OpWr {
    Push(u32),
    Pop,
}

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
enum OpRd {
    Peek,
}

#[derive(Clone)]
struct Stack {
    storage: Vec<u32>,
}

fn compare_vectors<T: PartialEq>(a: &Vec<T>, b: &Vec<T>) -> bool {
    let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
    matching == a.len() && matching == b.len()
}

impl Stack {
    pub fn push(&mut self, data: u32) {
        self.storage.push(data);
    }

    pub fn pop(&mut self) -> Option<u32> {
        self.storage.pop()
    }

    pub fn peek(&self) -> Option<u32> {
        let len = self.storage.len();
        if len > 0 {
            return Some(self.storage[len - 1]);
        }
        return None;
    }
}

impl Default for Stack {
    fn default() -> Stack {
        let s = Stack {
            storage: Default::default(),
        };

        s
    }
}

impl Dispatch for Stack {
    type ReadOperation<'rop> = OpRd;
    type WriteOperation = OpWr;
    type Response = Option<u32>;

    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
        match op {
            OpRd::Peek => self.peek(),
        }
    }

    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
        match op {
            OpWr::Push(v) => {
                self.push(v);
                Some(v)
            }
            OpWr::Pop => self.pop(),
        }
    }
}

/// Sequential data structure test (one thread).
///
/// Execute operations at random, comparing the result
/// against a known correct implementation.
#[test]
fn sequential_test() {
    let log = Log::<<Stack as Dispatch>::WriteOperation>::new_with_bytes(4 * 1024 * 1024, ());

    let mut orng = thread_rng();
    let nop = 50;

    let ltkn = log.register().expect("Can't register with log");
    let r = Replica::<Stack>::new(ltkn);
    let idx = r.register().expect("Failed to register with Replica.");
    let mut correct_stack: Vec<u32> = Vec::new();

    // Populate with some initial data
    for _i in 0..50 {
        let element = orng.gen();
        r.execute_mut(&log, OpWr::Push(element), idx).unwrap();
        correct_stack.push(element);
    }

    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 3usize {
            0usize => {
                let o = r.execute_mut(&log, OpWr::Pop, idx).unwrap();
                let popped = correct_stack.pop().unwrap();
                assert_eq!(popped, o.unwrap());
            }
            1usize => {
                let element = orng.gen();
                let pushed = r.execute_mut(&log, OpWr::Push(element), idx).unwrap();
                correct_stack.push(element);
                assert_eq!(pushed, Some(element));
            }
            2usize => {
                let o = r.execute(&log, OpRd::Peek, idx).unwrap();
                let mut ele = None;
                let len = correct_stack.len();
                if len > 0 {
                    ele = Some(correct_stack[len - 1]);
                }
                assert_eq!(ele, o);
            }
            _ => unreachable!(),
        }
    }

    let v = |data: &Stack| {
        assert!(
            compare_vectors(&correct_stack, &data.storage),
            "Push operation error detected"
        );
    };
    r.verify(&log, v);
}

/// A stack to verify that the log works correctly with multiple threads.
#[derive(Eq, PartialEq, Clone)]
struct VerifyStack {
    storage: Vec<u32>,
    per_replica_counter: HashMap<u16, u16>,
}

impl VerifyStack {
    pub fn push(&mut self, data: u32) {
        self.storage.push(data);
    }

    pub fn pop(&mut self) -> u32 {
        self.storage.pop().unwrap()
    }

    pub fn peek(&self) -> u32 {
        self.storage.last().unwrap().clone()
    }
}

impl Default for VerifyStack {
    fn default() -> VerifyStack {
        let s = VerifyStack {
            storage: Default::default(),
            per_replica_counter: Default::default(),
        };

        s
    }
}

impl Dispatch for VerifyStack {
    type ReadOperation<'rop> = OpRd;
    type WriteOperation = OpWr;
    type Response = Option<u32>;

    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
        match op {
            OpRd::Peek => {
                let ele: u32 = self.peek();
                let tid = (ele & 0xffff) as u16;
                let val = ((ele >> 16) & 0xffff) as u16;
                //println!("Peek tid {} val {}", tid, val);

                let last_popped = self
                    .per_replica_counter
                    .get(&tid)
                    .unwrap_or(&u16::max_value());

                // Reading already popped element.
                if *last_popped <= val {
                    println!(
                        "assert violation last_popped={} val={} tid={} {:?}",
                        *last_popped, val, tid, self.per_replica_counter
                    );
                }
                assert!(
                    *last_popped > val,
                    "Elements that came from a given thread are monotonically decreasing"
                );
                Some(ele)
            }
        }
    }

    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
        match op {
            OpWr::Push(v) => {
                let _tid = (v & 0xffff) as u16;
                let _val = ((v >> 16) & 0xffff) as u16;
                //println!("Push tid {} val {}", tid, val);
                self.push(v);
                Some(v)
            }
            OpWr::Pop => {
                let ele: u32 = self.pop();
                let tid = (ele & 0xffff) as u16;
                let val = ((ele >> 16) & 0xffff) as u16;
                //println!("POP tid {} val {}", tid, val);

                let cnt = self
                    .per_replica_counter
                    .get(&tid)
                    .unwrap_or(&u16::max_value());
                if *cnt <= val {
                    println!(
                        "assert violation cnt={} val={} tid={} {:?}",
                        *cnt, val, tid, self.per_replica_counter
                    );
                }
                assert!(
                    *cnt > val,
                    "Elements that came from a given thread are monotonically decreasing"
                );
                self.per_replica_counter.insert(tid, val);

                if val == 0 {
                    // Would be nice to assert this but if it runs in release
                    // mode without enough #total ops this will fail:
                    //assert_eq!(self.per_replica_counter.len(), 8, "Popped a final element from a thread before seeing elements from every thread.");
                    //println!("per_replica_counter ={:?}", per_replica_counter);
                }
                Some(ele)
            }
        }
    }
}

/// Many threads run in parallel, each pushing a unique increasing element into the stack.
// Then, a single thread pops all elements and checks that they are popped in the right order.
#[test]
fn parallel_push_sequential_pop_test() {
    let t = 4usize;
    let r = 2usize;
    let l = 32usize;
    let nop: u16 = 50000;

    let log = Arc::new(Log::<<Stack as Dispatch>::WriteOperation>::new_with_bytes(
        l * 1024 * 1024,
        (),
    ));

    let mut replicas = Vec::with_capacity(r);
    for _i in 0..r {
        let ltkn = log.register().expect("Register should work");
        replicas.push(Arc::new(Replica::<VerifyStack>::new(ltkn)));
    }

    let mut threads = Vec::new();
    let barrier = Arc::new(Barrier::new(t * r));

    for i in 0..r {
        for j in 0..t {
            let replica = replicas[i].clone();
            let log = log.clone();
            let b = barrier.clone();
            let child = thread::spawn(move || {
                let tid: u32 = (i * t + j) as u32;
                //println!("tid = {} i={} j={}", tid, i, j);
                let idx = replica
                    .register()
                    .expect("Failed to register with replica.");

                // 1. Insert phase
                b.wait();
                for i in 0..nop {
                    replica
                        .execute_mut(&log, OpWr::Push((i as u32) << 16 | tid), idx)
                        .unwrap();
                }
            });
            threads.push(child);
        }
    }

    for _i in 0..threads.len() {
        let _retval = threads
            .pop()
            .unwrap()
            .join()
            .expect("Thread didn't finish successfully.");
    }

    // Verify by popping everything off all replicas:
    for i in 0..r {
        let replica = replicas[i].clone();
        let token = replica.register().unwrap();
        for _j in 0..t {
            for _z in 0..nop {
                replica.execute(&log, OpRd::Peek, token).unwrap();
                replica.execute_mut(&log, OpWr::Pop, token).unwrap();
            }
        }
    }
}

/// Many threads run in parallel, each pushing a unique increasing element into the stack.
/// Then, many threads run in parallel, each popping an element and checking that the
/// elements that came from a given thread are monotonically decreasing.
#[test]
fn parallel_push_and_pop_test() {
    let t = 4usize;
    let r = 2usize;
    let l = 128usize;
    let nop: u16 = u16::MAX;

    let log = Arc::new(Log::<<Stack as Dispatch>::WriteOperation>::new_with_bytes(
        l * 1024 * 1024,
        (),
    ));

    let mut replicas = Vec::with_capacity(r);
    for _i in 0..r {
        let ltkn = log.register().expect("Register should work");
        replicas.push(Arc::new(Replica::<VerifyStack>::new(ltkn)));
    }

    let mut threads = Vec::new();
    let barrier = Arc::new(Barrier::new(t * r));

    for i in 0..r {
        for j in 0..t {
            let replica = replicas[i].clone();
            let b = barrier.clone();
            let log = log.clone();
            let child = thread::spawn(move || {
                let tid: u32 = (i * t + j) as u32;
                //println!("tid = {} i={} j={}", tid, i, j);
                let idx = replica
                    .register()
                    .expect("Failed to register with replica.");

                // 1. Insert phase
                b.wait();
                for i in 0..nop {
                    replica
                        .execute_mut(&log, OpWr::Push((i as u32) << 16 | tid), idx)
                        .unwrap();
                }

                // 2. Dequeue phase, verification
                b.wait();
                for _i in 0..nop {
                    replica.execute(&log, OpRd::Peek, idx).unwrap();
                    replica.execute_mut(&log, OpWr::Pop, idx).unwrap();
                }
            });
            threads.push(child);
        }
    }

    for _i in 0..threads.len() {
        let _retval = threads
            .pop()
            .unwrap()
            .join()
            .expect("Thread didn't finish successfully.");
    }
}

fn bench(r: Arc<Replica<Stack>>, log: &Log<OpWr>, nop: usize, barrier: Arc<Barrier>) -> (u64, u64) {
    let idx = r.register().expect("Failed to register with Replica.");

    let mut orng = thread_rng();
    let mut arng = thread_rng();

    let mut ops = Vec::with_capacity(nop);
    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 2usize {
            0usize => ops.push(OpWr::Pop),
            1usize => ops.push(OpWr::Push(arng.gen())),
            _ => unreachable!(),
        }
    }
    barrier.wait();

    for i in 0..nop {
        if nop % 1000 == 0 {
            std::thread::yield_now();
        }
        r.execute_mut(&log, ops[i], idx).expect("should work");
    }

    barrier.wait();

    (0, 0)
}

/// Verify that 2 replicas are equal after a set of random
/// operations have been executed against the log.
#[test]
fn replicas_are_equal() {
    let t = 4usize;
    let r = 2usize;
    let l = 1usize;
    let n = 50usize;

    let log = Arc::new(Log::<<Stack as Dispatch>::WriteOperation>::new_with_bytes(
        l * 1024 * 1024,
        (),
    ));

    let mut replicas = Vec::with_capacity(r);
    for _i in 0..r {
        let ltkn = log.register().expect("Register should work");
        replicas.push(Arc::new(Replica::<Stack>::new(ltkn)));
    }

    let mut threads = Vec::new();
    let barrier = Arc::new(Barrier::new(t * r));

    for i in 0..r {
        for _j in 0..t {
            let r = replicas[i].clone();
            let log = log.clone();
            let o = n.clone();
            let b = barrier.clone();
            let child = thread::spawn(move || bench(r, &log, o, b));
            threads.push(child);
        }
    }

    for _i in 0..threads.len() {
        let _retval = threads
            .pop()
            .unwrap()
            .join()
            .expect("Thread didn't finish successfully.");
    }

    let mut d0 = vec![];
    let mut p0 = vec![];
    let v = |data: &Stack| {
        d0.extend_from_slice(&data.storage);
        p0.extend_from_slice(&data.storage);
    };
    replicas[0].verify(&log, v);

    let mut d1 = vec![];
    let mut p1 = vec![];
    let v = |data: &Stack| {
        d1.extend_from_slice(&data.storage);
        p1.extend_from_slice(&data.storage);
    };
    replicas[1].verify(&log, v);

    assert_eq!(d0, d1, "Data-structures don't match.");
    assert_eq!(p0, p1, "Removed elements in each replica dont match.");
}
