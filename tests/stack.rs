// Copyright © VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

extern crate rand;
extern crate std;

use std::collections::HashMap;
use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::usize;

use node_replication::Dispatch;
use node_replication::Log;
use node_replication::Replica;

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

struct Stack {
    storage: Vec<u32>,
    popped: Vec<Option<u32>>,
    peeked: RwLock<Vec<Option<u32>>>,
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
        let r = self.storage.pop();
        self.popped.push(r);
        return r;
    }

    pub fn peek(&self) -> Option<u32> {
        let mut r = None;
        let len = self.storage.len();
        if len > 0 {
            r = Some(self.storage[len - 1]);
        }
        self.peeked.write().unwrap().push(r);
        return r;
    }
}

impl Default for Stack {
    fn default() -> Stack {
        let s = Stack {
            storage: Default::default(),
            popped: Default::default(),
            peeked: Default::default(),
        };

        s
    }
}

impl Dispatch for Stack {
    type ReadOperation = OpRd;
    type WriteOperation = OpWr;
    type Response = Option<u32>;
    type ResponseError = ();

    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpRd::Peek => return Ok(self.peek()),
        };
    }

    fn dispatch_mut(
        &mut self,
        op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpWr::Push(v) => {
                self.push(v);
                return Ok(Some(v));
            }
            OpWr::Pop => return Ok(self.pop()),
        }
    }
}

/// Sequential data structure test (one thread).
///
/// Execute operations at random, comparing the result
/// against a known correct implementation.
#[test]
fn sequential_test() {
    let log = Arc::new(Log::<<Stack as Dispatch>::WriteOperation>::new(
        4 * 1024 * 1024,
    ));

    let mut orng = thread_rng();
    let nop = 50;

    let r = Replica::<Stack>::new(&log);
    let idx = r.register().expect("Failed to register with Replica.");
    let mut correct_stack: Vec<u32> = Vec::new();
    let mut correct_popped: Vec<Option<u32>> = Vec::new();
    let mut correct_peeked: Vec<Option<u32>> = Vec::new();

    // Populate with some initial data
    for _i in 0..50 {
        let element = orng.gen();
        r.execute_mut(OpWr::Push(element), idx).unwrap();
        correct_stack.push(element);
    }

    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 3usize {
            0usize => {
                let o = r.execute_mut(OpWr::Pop, idx);
                let popped = correct_stack.pop();

                match o {
                    Ok(element) => assert_eq!(popped, element),
                    Err(_) => {}
                }
                correct_popped.push(popped);
            }
            1usize => {
                let element = orng.gen();
                match r.execute_mut(OpWr::Push(element), idx) {
                    Ok(ele) => assert_eq!(Some(element), ele),
                    Err(_) => {}
                }
                correct_stack.push(element);
            }
            2usize => {
                let o = r.execute(OpRd::Peek, idx);
                let mut ele = None;
                let len = correct_stack.len();
                if len > 0 {
                    ele = Some(correct_stack[len - 1]);
                }
                match o {
                    Ok(element) => assert_eq!(ele, element),
                    Err(_) => {}
                }
                correct_peeked.push(ele);
            }
            _ => unreachable!(),
        }
    }

    let v = |data: &Stack| {
        assert!(
            compare_vectors(&correct_popped, &data.popped),
            "Pop operation error detected"
        );
        assert!(
            compare_vectors(&correct_stack, &data.storage),
            "Push operation error detected"
        );
        assert!(
            compare_vectors(&correct_peeked, &data.peeked.read().unwrap()),
            "Peek operation error detected"
        );
    };
    r.verify(v);
}

/// A stack to verify that the log works correctly with multiple threads.
#[derive(Eq, PartialEq)]
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
    type ReadOperation = OpRd;
    type WriteOperation = OpWr;
    type Response = Option<u32>;
    type ResponseError = Option<()>;

    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
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
                return Ok(Some(ele));
            }
        }
    }

    fn dispatch_mut(
        &mut self,
        op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpWr::Push(v) => {
                let _tid = (v & 0xffff) as u16;
                let _val = ((v >> 16) & 0xffff) as u16;
                //println!("Push tid {} val {}", tid, val);
                self.push(v);
                return Ok(Some(v));
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
                    // This is one of our last elements, so we sanity check that we've
                    // seen values from all threads by now (if not we may have been really unlucky
                    // with thread scheduling or something is wrong with fairness in our implementation)
                    // println!("per_replica_counter ={:?}", per_replica_counter);
                    assert_eq!(self.per_replica_counter.len(), 8, "Popped a final element from a thread before seeing elements from every thread.");
                }
                return Ok(Some(ele));
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
    let l = 1usize;
    let nop: u16 = 50000;

    let log = Arc::new(Log::<<Stack as Dispatch>::WriteOperation>::new(
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

                // 1. Insert phase
                b.wait();
                for i in 0..nop {
                    replica
                        .execute_mut(OpWr::Push((i as u32) << 16 | tid), idx)
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
        for _j in 0..t {
            for _z in 0..nop {
                replica.execute(OpRd::Peek, i + 1).unwrap();
                replica.execute_mut(OpWr::Pop, i + 1).unwrap();
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

    let log = Arc::new(Log::<<Stack as Dispatch>::WriteOperation>::new(
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

                // 1. Insert phase
                b.wait();
                for i in 0..nop {
                    replica
                        .execute_mut(OpWr::Push((i as u32) << 16 | tid), idx)
                        .unwrap();
                }

                // 2. Dequeue phase, verification
                b.wait();
                for _i in 0..nop {
                    replica.execute(OpRd::Peek, idx).unwrap();
                    replica.execute_mut(OpWr::Pop, idx).unwrap();
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
        r.execute_mut(ops[i], idx).unwrap();
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

    let log = Arc::new(Log::<<Stack as Dispatch>::WriteOperation>::new(
        l * 1024 * 1024 * 1024,
    ));

    let mut replicas = Vec::with_capacity(r);
    for _i in 0..r {
        replicas.push(Replica::<Stack>::new(&log));
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
    let v = |data: &Stack| {
        d0.extend_from_slice(&data.storage);
        p0.extend_from_slice(&data.popped);
    };
    replicas[0].verify(v);

    let mut d1 = vec![];
    let mut p1 = vec![];
    let v = |data: &Stack| {
        d1.extend_from_slice(&data.storage);
        p1.extend_from_slice(&data.popped);
    };
    replicas[1].verify(v);

    assert_eq!(d0, d1, "Data-structures don't match.");
    assert_eq!(p0, p1, "Removed elements in each replica dont match.");
}
