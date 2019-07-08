extern crate std;

use std::cell::RefCell;
use std::env;
use std::sync::Arc;
use std::thread;

use node_replication::log::Log;
use node_replication::replica::Replica;
use node_replication::Dispatch;

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

#[derive(Default)]
struct Stack {
    storage: RefCell<Vec<u32>>,
}

impl Stack {
    pub fn push(&self, data: u32) {
        self.storage.borrow_mut().push(data);
    }

    pub fn pop(&self) -> Option<u32> {
        self.storage.borrow_mut().pop()
    }
}

impl Dispatch for Stack {
    type Operation = Op;

    fn dispatch(&self, op: Self::Operation) {
        match op {
            Op::Push(v) => self.push(v),

            Op::Pop => {
                self.pop();
            }

            Op::Invalid => {}
        }
    }
}

fn main() {
    let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::default());
    let replica = Arc::new(Replica::<Stack>::new(&log));

    let mut threads = Vec::new();
    for _t in 0..4 {
        let r = replica.clone();
        let child = thread::spawn(move || {
            let idx = r.register().expect("Failed to register with Replica.");
            r.execute(Op::Push(100), idx);
            r.execute(Op::Pop, idx);
        });
        threads.push(child);
    }

    for _t in 0..threads.len() {
        let _r = threads.pop().unwrap().join();
    }
}
