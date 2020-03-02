// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A minimal example to implement a replicated stack (single-thread).
use std::sync::Arc;

use node_replication::log::Log;
use node_replication::replica::Replica;
use node_replication::Dispatch;

/// We support push and pop operations on the stack.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
enum OpWr {
    Push(u32),
    Pop,
}

/// The actual stack, it's represented by a vector underneath.
struct Stack {
    storage: Vec<u32>,
}

impl Stack {
    /// Push adds an element from the underlying storage.
    pub fn push(&mut self, data: u32) {
        self.storage.push(data);
    }

    /// Pop removes an element from the underlying storage.
    pub fn pop(&mut self) -> Option<u32> {
        self.storage.pop()
    }
}

/// The stack needs a Default implementation, here we add some initial elements.
impl Default for Stack {
    fn default() -> Stack {
        const DEFAULT_STACK_SIZE: u32 = 1_000u32;

        let mut s = Stack {
            storage: Default::default(),
        };

        for e in 0..DEFAULT_STACK_SIZE {
            s.push(e);
        }

        s
    }
}

impl Dispatch for Stack {
    type ReadOperation = ();
    type WriteOperation = OpWr;
    type Response = Option<u32>;
    type ResponseError = Option<()>;

    fn dispatch(&self, _op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        Err(None)
    }
    /// The dispatch traint defines how operations coming from the log
    /// are execute against our local stack within a replica.
    fn dispatch_mut(
        &mut self,
        op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpWr::Push(v) => {
                self.push(v);
                return Ok(None);
            }
            OpWr::Pop => return Ok(self.pop()),
        }
    }
}

/// We initialize a log, a replica for a stack, register with the reploca and
/// then execute operations on the replica.
fn main() {
    const ONE_MIB: usize = 1 * 1024 * 1024;
    let log = Arc::new(Log::<<Stack as Dispatch>::WriteOperation>::new(ONE_MIB));
    let replica = Replica::<Stack>::new(&log);
    let ridx = replica.register().expect("Couldn't register with replica");

    for i in 0..1024 {
        match i % 2 {
            0 => replica.execute(OpWr::Push(i as u32), ridx).unwrap(),
            1 => replica.execute(OpWr::Pop, ridx).unwrap(),
            _ => unreachable!(),
        };
    }
}
