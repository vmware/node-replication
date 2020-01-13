// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

extern crate rand;
extern crate std;

use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;
use std::usize;

use node_replication::log::Log;
use node_replication::replica::Replica;
use node_replication::Dispatch;

use rand::{thread_rng, Rng};

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
enum Op {
    Push(u32),
    Pop,
}

#[derive(Eq, PartialEq)]
struct Stack {
    storage: Vec<u32>,
    popped: Vec<Option<u32>>,
}

impl Stack {
    pub fn push(&mut self, data: u32) {
        self.storage.push(data);
    }

    pub fn pop(&mut self) {
        let r = self.storage.pop();
        self.popped.push(r);
    }
}

impl Default for Stack {
    fn default() -> Stack {
        let s = Stack {
            storage: Default::default(),
            popped: Default::default(),
        };

        s
    }
}

impl Dispatch for Stack {
    type Operation = Op;
    type Response = Option<u32>;
    type ResponseError = ();

    fn dispatch(&mut self, op: Self::Operation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            Op::Push(v) => self.push(v),
            Op::Pop => self.pop(),
        }

        Ok(None)
    }
}

/// Sequential data structure test (one thread).
///
/// Execute operations at random, comparing the result
/// against a known correct implementation.
#[test]
fn sequential_test() {
    let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(4 * 1024 * 1024));

    let mut orng = thread_rng();
    let nop = 50;

    let r = Replica::<Stack>::new(&log);
    let idx = r.register().expect("Failed to register with Replica.");
    let mut o = vec![];
    let mut correct_stack: Vec<u32> = Vec::new();
    let mut correct_popped: Vec<Option<u32>> = Vec::new();

    // Populate with some initial data
    for _i in 0..50 {
        let element = orng.gen();
        r.execute(Op::Push(element), idx);
        r.get_responses(idx, &mut o);
        o.clear();
        correct_stack.push(element);
    }

    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 2usize {
            0usize => {
                r.execute(Op::Pop, idx);
                correct_popped.push(correct_stack.pop());
            }
            1usize => {
                let element = orng.gen();
                r.execute(Op::Push(element), idx);
                correct_stack.push(element);
            }
            _ => unreachable!(),
        }
        r.get_responses(idx, &mut o);
        o.clear();
    }

    let v = |data: RefMut<Stack>| {
        assert_eq!(correct_popped, data.popped, "Pop operation error detected");
        assert_eq!(correct_stack, data.storage, "Push operation error detected");
    };
    r.verify(v);
}

/// A stack to verify that the log works correctly with multiple threads.
#[derive(Eq, PartialEq)]
struct VerifyStack {
    storage: RefCell<Vec<u32>>,
    per_replica_counter: RefCell<HashMap<u16, u16>>,
}

impl VerifyStack {
    pub fn push(&self, data: u32) {
        self.storage.borrow_mut().push(data);
    }

    pub fn pop(&self) -> u32 {
        self.storage.borrow_mut().pop().unwrap()
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
    type Operation = Op;
    type Response = Option<u32>;
    type ResponseError = Option<()>;

    fn dispatch(&mut self, op: Self::Operation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            Op::Push(v) => {
                let _tid = (v & 0xffff) as u16;
                let _val = ((v >> 16) & 0xffff) as u16;
                //println!("Push tid {} val {}", tid, val);
                self.push(v);
            }
            Op::Pop => {
                let ele: u32 = self.pop();
                let tid = (ele & 0xffff) as u16;
                let val = ((ele >> 16) & 0xffff) as u16;
                //println!("POP tid {} val {}", tid, val);
                let mut per_replica_counter = self.per_replica_counter.borrow_mut();

                let cnt = per_replica_counter.get(&tid).unwrap_or(&u16::max_value());
                if *cnt <= val {
                    println!(
                        "assert violation cnt={} val={} tid={} {:?}",
                        *cnt, val, tid, per_replica_counter
                    );
                }
                assert!(
                    *cnt > val,
                    "Elements that came from a given thread are monotonically decreasing"
                );
                per_replica_counter.insert(tid, val);

                if val == 0 {
                    // This is one of our last elements, so we sanity check that we've
                    // seen values from all threads by now (if not we may have been really unlucky
                    // with thread scheduling or something is wrong with fairness in our implementation)
                    // println!("per_replica_counter ={:?}", per_replica_counter);
                    assert_eq!(per_replica_counter.len(), 8, "Popped a final element from a thread before seeing elements from every thread.");
                }
            }
        }

        return Ok(None);
    }
}

/// Many threads run in parallel, each pushing a unique increasing element into the stack.
// Then, a single thread pops all elements and checks that they are popped in the right order.
#[test]
fn parallel_push_sequential_pop_test() {
    let t = 4usize;
    let r = 2usize;
    let l = 1usize;
    let nop: u16 = 50000;

    let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(
        l * 1024 * 1024 * 1024,
    ));

    let mut replicas = Vec::with_capacity(r);
    for _i in 0..r {
        replicas.push(Arc::new(Replica::<VerifyStack>::new(&log)));
    }

    let mut threads = Vec::new();
    let barrier = Arc::new(Barrier::new(t * r));

    for i in 0..r {
        for j in 0..t {
            let replica = replicas[i].clone();
            let b = barrier.clone();
            let child = thread::spawn(move || {
                let tid: u32 = (i * t + j) as u32;
                //println!("tid = {} i={} j={}", tid, i, j);
                let idx = replica
                    .register()
                    .expect("Failed to register with replica.");
                let mut o = vec![];

                // 1. Insert phase
                b.wait();
                for i in 0..nop {
                    replica.execute(Op::Push((i as u32) << 16 | tid), idx);
                    while replica.get_responses(idx, &mut o) == 0 {}
                    o.clear();
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
        let mut o = vec![];
        for _j in 0..t {
            for _z in 0..nop {
                replica.execute(Op::Pop, 1);
                replica.get_responses(1, &mut o);
                o.clear();
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
    let l = 1usize;
    let nop: u16 = 50000;

    let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(
        l * 1024 * 1024 * 1024,
    ));

    let mut replicas = Vec::with_capacity(r);
    for _i in 0..r {
        replicas.push(Arc::new(Replica::<VerifyStack>::new(&log)));
    }

    let mut threads = Vec::new();
    let barrier = Arc::new(Barrier::new(t * r));

    for i in 0..r {
        for j in 0..t {
            let replica = replicas[i].clone();
            let b = barrier.clone();
            let child = thread::spawn(move || {
                let tid: u32 = (i * t + j) as u32;
                //println!("tid = {} i={} j={}", tid, i, j);
                let idx = replica
                    .register()
                    .expect("Failed to register with replica.");
                let mut o = vec![];

                // 1. Insert phase
                b.wait();
                for i in 0..nop {
                    replica.execute(Op::Push((i as u32) << 16 | tid), idx);
                    while replica.get_responses(idx, &mut o) == 0 {}
                    o.clear();
                }

                // 2. Dequeue phase, verification
                b.wait();
                for _i in 0..nop {
                    replica.execute(Op::Pop, idx);
                    while replica.get_responses(idx, &mut o) == 0 {}
                    o.clear();
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

fn bench(r: Arc<Replica<Stack>>, nop: usize, barrier: Arc<Barrier>) -> (u64, u64) {
    let idx = r.register().expect("Failed to register with Replica.");

    let mut o = vec![];
    let mut orng = thread_rng();
    let mut arng = thread_rng();

    let mut ops = Vec::with_capacity(nop);
    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 2usize {
            0usize => ops.push(Op::Pop),
            1usize => ops.push(Op::Push(arng.gen())),
            _ => unreachable!(),
        }
    }
    barrier.wait();

    for i in 0..nop {
        r.execute(ops[i], idx);
        while r.get_responses(idx, &mut o) == 0 {}
        o.clear();
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

    let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(
        l * 1024 * 1024 * 1024,
    ));

    let mut replicas = Vec::with_capacity(r);
    for _i in 0..r {
        replicas.push(Arc::new(Replica::<Stack>::new(&log)));
    }

    let mut threads = Vec::new();
    let barrier = Arc::new(Barrier::new(t * r));

    for i in 0..r {
        for _j in 0..t {
            let r = replicas[i].clone();
            let o = n.clone();
            let b = barrier.clone();
            let child = thread::spawn(move || bench(r, o, b));
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
    let v = |data: RefMut<Stack>| {
        d0.extend_from_slice(&data.storage);
        p0.extend_from_slice(&data.popped);
    };
    replicas[0].verify(v);

    let mut d1 = vec![];
    let mut p1 = vec![];
    let v = |data: RefMut<Stack>| {
        d1.extend_from_slice(&data.storage);
        p1.extend_from_slice(&data.popped);
    };
    replicas[1].verify(v);

    assert_eq!(d0, d1, "Data-structures don't match.");
    assert_eq!(p0, p1, "Removed elements in each replica dont match.");
}
