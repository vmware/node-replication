// Copyright © VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A minimal example that implements a replicated stack
use std::sync::Arc;

use node_replication::log::Log;
use node_replication::replica::Replica;
use node_replication::Dispatch;

/// We support mutable push and pop operations on the stack.
#[derive(Debug, PartialEq, Clone)]
enum Modify {
    Push(u32),
    Pop,
}

/// We support an immutable read operation to peek the stack.
#[derive(Debug, Clone, PartialEq)]
enum Access {
    Peek,
}

/// The actual stack, it uses a single-threaded Vec.
struct Stack {
    storage: Vec<u32>,
}

impl Default for Stack {
    /// The stack Default implementation, as it is
    /// executed for every Replica.
    ///
    /// This should be deterministic as it is used to create multiple instances
    /// of a Stack for every replica.
    fn default() -> Stack {
        const DEFAULT_STACK_SIZE: u32 = 1_000u32;

        let mut s = Stack {
            storage: Default::default(),
        };

        for e in 0..DEFAULT_STACK_SIZE {
            s.storage.push(e);
        }

        s
    }
}

/// The Dispatch traits executes `ReadOperation` (our Access enum)
/// and `WriteOperation` (our `Modify` enum) against the replicated
/// data-structure.
impl Dispatch for Stack {
    type ReadOperation = Access;
    type WriteOperation = Modify;
    type Response = u32;
    type ResponseError = ();

    /// The `dispatch` function applies the immutable operations.
    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            Access::Peek => self.storage.last().cloned().ok_or(()),
        }
    }

    /// The `dispatch_mut` function applies the mutable operations.
    fn dispatch_mut(
        &mut self,
        op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        match op {
            Modify::Push(v) => {
                self.storage.push(v);
                return Ok(v);
            }
            Modify::Pop => return self.storage.pop().ok_or(()),
        }
    }
}

/// We initialize a log, and two replicas for a stack, register with the replica
/// and then execute operations.
fn main() {
    // The operation log for storing `WriteOperation`, it has a size of 2 MiB:
    let log = Arc::new(Log::<<Stack as Dispatch>::WriteOperation>::new(
        2 * 1024 * 1024,
    ));

    // Next, we create two replicas of the stack
    let replica1 = Replica::<Stack>::new(&log);
    let replica2 = Replica::<Stack>::new(&log);

    // The replica executes a Modify or Access operations by calling
    // `execute` and `execute_ro`. Eventually they end up in the `Dispatch` trait.
    let thread_loop = |replica: &Arc<Replica<Stack>>, ridx| {
        for i in 0..2048 {
            let _r = match i % 3 {
                0 => replica.execute(Modify::Push(i as u32), ridx),
                1 => replica.execute(Modify::Pop, ridx),
                2 => replica.execute_ro(Access::Peek, ridx),
                _ => unreachable!(),
            };
        }
    };

    // Finally, we spawn three threads that issue operations, thread 1 and 2
    // will use replica1 and thread 3 will use replica 2:
    let replica11 = replica1.clone();
    std::thread::spawn(move || {
        let ridx = replica11.register().expect("Unable to register with log");
        thread_loop(&replica11, ridx);
    });

    let replica12 = replica1.clone();
    std::thread::spawn(move || {
        let ridx = replica12.register().expect("Unable to register with log");
        thread_loop(&replica12, ridx);
    });

    std::thread::spawn(move || {
        let ridx = replica2.register().expect("Unable to register with log");
        thread_loop(&replica2, ridx);
    });
}
