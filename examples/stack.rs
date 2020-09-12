// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A minimal example that implements a replicated stack
use crossbeam::queue::{PopError, SegQueue};
use std::sync::Arc;

use node_replication::Dispatch;
use node_replication::Log;
use node_replication::LogMapper;
use node_replication::Replica;

/// We support mutable push and pop operations on the stack.
#[derive(Hash, Clone, Debug, PartialEq)]
enum Modify {
    Push(u32),
    Pop,
}

impl LogMapper for Modify {
    fn hash(&self) -> usize {
        0
    }
}

/// We support an immutable read operation to peek the stack.
#[derive(Hash, Clone, Debug, PartialEq)]
enum Access {
    Peek,
}

impl LogMapper for Access {
    fn hash(&self) -> usize {
        0
    }
}

/// The actual stack, it uses a single-threaded Vec.
struct Stack {
    storage: SegQueue<u32>,
}

impl Default for Stack {
    /// The stack Default implementation, as it is
    /// executed for every Replica.
    ///
    /// This should be deterministic as it is used to create multiple instances
    /// of a Stack for every replica.
    fn default() -> Stack {
        const DEFAULT_STACK_SIZE: u32 = 1_000u32;

        let s = Stack {
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
    type Response = Result<u32, PopError>;

    /// The `dispatch` function applies the immutable operations.
    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            Access::Peek => {
                if self.storage.is_empty() {
                    Err(PopError)
                } else {
                    Ok(0)
                }
            }
        }
    }

    /// The `dispatch_mut` function applies the mutable operations.
    fn dispatch_mut(&self, op: Self::WriteOperation) -> Self::Response {
        match op {
            Modify::Push(v) => {
                self.storage.push(v);
                return Ok(0);
            }
            Modify::Pop => return self.storage.pop(),
        }
    }
}

/// We initialize a log, and two replicas for a stack, register with the replica
/// and then execute operations.
fn main() {
    // The operation log for storing `WriteOperation`, it has a size of 2 MiB:
    let log = Arc::new(Log::<<Stack as Dispatch>::WriteOperation>::new(
        2 * 1024 * 1024,
        1,
    ));

    // Next, we create two replicas of the stack
    let replica1 = Replica::<Stack>::new(vec![log.clone()]);
    let replica2 = Replica::<Stack>::new(vec![log.clone()]);

    // The replica executes a Modify or Access operations by calling
    // `execute_mut` and `execute`. Eventually they end up in the `Dispatch` trait.
    let thread_loop = |replica: &Arc<Replica<Stack>>, ridx| {
        for i in 0..2048 {
            let _r = match i % 3 {
                0 => replica.execute_mut(Modify::Push(i as u32), ridx),
                1 => replica.execute_mut(Modify::Pop, ridx),
                2 => replica.execute(Access::Peek, ridx),
                _ => unreachable!(),
            };
        }
    };

    // Finally, we spawn three threads that issue operations, thread 1 and 2
    // will use replica1 and thread 3 will use replica 2:
    let replica11 = replica1.clone();

    let mut threads = Vec::with_capacity(3);
    threads.push(std::thread::spawn(move || {
        let ridx = replica11.register().expect("Unable to register with log");
        thread_loop(&replica11, ridx);
    }));

    let replica12 = replica1.clone();
    threads.push(std::thread::spawn(move || {
        let ridx = replica12.register().expect("Unable to register with log");
        thread_loop(&replica12, ridx);
    }));

    threads.push(std::thread::spawn(move || {
        let ridx = replica2.register().expect("Unable to register with log");
        thread_loop(&replica2, ridx);
    }));

    // Wait for all the threads to finish
    for thread in threads {
        thread.join().unwrap();
    }
}
