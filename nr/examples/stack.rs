// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A minimal example that implements a node-replicated stack

use std::num::NonZeroUsize;
use std::sync::Arc;

use node_replication::Dispatch;
use node_replication::NodeReplicated;

/// We support mutable push and pop operations on the stack.
#[derive(Clone, Debug, PartialEq)]
enum Modify {
    Push(u32),
    Pop,
}

/// We support an immutable read operation to peek the stack.
#[derive(Clone, Debug, PartialEq)]
enum Access {
    Peek,
}

/// The actual stack is implemented with a (single-threaded) Vec.
struct Stack {
    storage: Vec<u32>,
}

impl Default for Stack {
    /// The stack Default implementation.
    ///
    /// This should be deterministic as it is used to create multiple instances of a Stack
    /// for every replica during initialization.
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

/// The Dispatch traits executes `ReadOperation` (our Access enum) and `WriteOperation`
/// (our `Modify` enum) against the replicated data-structure.
impl Dispatch for Stack {
    type ReadOperation = Access;
    type WriteOperation = Modify;
    type Response = Option<u32>;

    /// The `dispatch` function applies the immutable operations.
    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            Access::Peek => self.storage.last().cloned(),
        }
    }

    /// The `dispatch_mut` function applies the mutable operations.
    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
        match op {
            Modify::Push(v) => {
                self.storage.push(v);
                return None;
            }
            Modify::Pop => return self.storage.pop(),
        }
    }
}

/// We initialize a replicated stack with two replicas, then spawn threads, assign them to
/// replicas and execute operations.
fn main() {
    const NUM_REPLICAS: usize = 2;
    const NUM_THREADS: usize = 3;
    const NUM_OPS_PER_THREAD: usize = 2048;

    let two = NonZeroUsize::new(NUM_REPLICAS).expect("2 is not 0");
    let stack =
        Arc::new(NodeReplicated::new(two, |_node| {}).expect("Can't create NodeReplicated Stack"));

    // The replica executes a Modify or Access operations by calling
    // `execute_mut` and `execute`. Eventually they end up in the `Dispatch` trait.
    let thread_loop = |stack: &Arc<NodeReplicated<Stack>>, ridx| {
        for i in 0..NUM_OPS_PER_THREAD {
            let _r = match i % 3 {
                0 => stack.execute_mut(Modify::Push(i as u32), ridx),
                1 => stack.execute_mut(Modify::Pop, ridx),
                2 => stack.execute(Access::Peek, ridx),
                _ => unreachable!(),
            };
        }
    };

    let mut threads = Vec::with_capacity(NUM_THREADS);
    for idx in 0..NUM_THREADS {
        let stack = stack.clone();
        threads.push(std::thread::spawn(move || {
            let ridx = stack
                .register(idx % NUM_REPLICAS)
                .expect("Unable to register with stack");
            thread_loop(&stack, ridx);
        }));
    }

    // Wait for all the threads to finish
    for thread in threads {
        thread.join().unwrap();
    }
}
