// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

extern crate std;

use node_replication::log::Log;
use node_replication::replica::Replica;
use node_replication::Dispatch;

use rand::{thread_rng, Rng};

const DEFAULT_STACK_SIZE: u32 = 1_000u32;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
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

impl Default for Stack {
    fn default() -> Stack {
        let s = Stack {
            storage: Default::default(),
        };

        for e in 0..DEFAULT_STACK_SIZE {
            s.push(e);
        }

        s
    }
}

impl Dispatch for Stack {
    type Operation = Op;
    type Response = Option<u32>;

    fn dispatch(&self, op: Self::Operation) -> Self::Response {
        match op {
            Op::Push(v) => {
                self.push(v);
                return None;
            }
            Op::Pop => return self.pop(),
            Op::Invalid => unreachable!("Op::Invalid?"),
        }
    }
}

fn main() {
    use std::sync::Arc;
    let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(1 * 1024 * 1024));
    let replica = Replica::<Stack>::new(&log);
    let ridx = replica.register().expect("Couldn't register with replica");

    for i in 0..1024 {
        match i % 2 {
            0 => replica.execute(Op::Push(i as u32), ridx),
            1 => replica.execute(Op::Pop, ridx),
        };
    }
}
