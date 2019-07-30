extern crate rand;
extern crate std;

use std::cell::RefCell;
use std::sync::{Arc, Barrier};
use std::thread;
use std::usize;

use node_replication::log::Log;
use node_replication::replica::Replica;
use node_replication::Dispatch;

use rand::{thread_rng, Rng};

const DEFAULT_STACK_SIZE: u32 = 100;

#[derive(Clone, Copy)]
enum Op {
    Push(u32),
    Pop,
    Invalid,
}

impl Default for Op {
    fn default() -> Op {
        Op::Invalid
    }
}

#[derive(Eq, PartialEq)]
struct Stack {
    storage: RefCell<Vec<u32>>,
    popped: RefCell<Vec<u32>>,
}

impl Stack {
    pub fn push(&self, data: u32) {
        self.storage.borrow_mut().push(data);
    }

    pub fn pop(&self) {
        let r = self.storage.borrow_mut().pop();

        if r.is_none() {
            panic!("Pop empty")
        } else {
            self.popped.borrow_mut().push(r.unwrap());
        }
    }
}

impl Default for Stack {
    fn default() -> Stack {
        let s = Stack {
            storage: Default::default(),
            popped: Default::default(),
        };

        for e in 0..DEFAULT_STACK_SIZE {
            s.push(e);
        }

        s
    }
}

impl Dispatch for Stack {
    type Operation = Op;

    fn dispatch(&self, op: Self::Operation) {
        match op {
            Op::Push(v) => self.push(v),
            Op::Pop => self.pop(),
            Op::Invalid => panic!("Got invalid OP"),
        }
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
            0usize => ops.push(Op::Pop),
            1usize => ops.push(Op::Push(arng.gen())),
            _ => unreachable!(),
        }
    }
    barrier.wait();

    for i in 0..nop {
        r.execute(ops[i], idx);
    }

    barrier.wait();

    (0, 0)
}

#[test]
fn stack_integration() {
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

    unsafe {
        let s0 = Arc::try_unwrap(replicas.pop().unwrap()).unwrap().data();
        let s1 = Arc::try_unwrap(replicas.pop().unwrap()).unwrap().data();
        assert_eq!(s0.storage, s1.storage, "Data-structures don't match.");
        assert_eq!(
            s0.popped, s1.popped,
            "Removed elements in each replica dont match."
        );
    }
}
