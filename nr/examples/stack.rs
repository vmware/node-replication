// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A minimal example that implements a node-replicated, generic stack

#![feature(const_option)]

use std::cmp::PartialEq;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::Arc;

use node_replication::Dispatch;
use node_replication::NodeReplicated;
use node_replication::NodeReplicatedError;
use node_replication::ThreadToken;

/// We support mutable push and pop operations on the stack.
#[derive(Clone, Debug, PartialEq)]
enum Modify<T> {
    Push(T),
    Pop,
}

/// We support an immutable read operation to peek the stack.
#[derive(Clone, Debug, PartialEq)]
enum Access {
    Peek,
}

/// The actual stack is implemented with a (single-threaded) Vec.
struct Stack<T>
where
    T: Default,
{
    storage: Vec<T>,
}

impl<T> Default for Stack<T>
where
    T: Default,
{
    /// The stack Default implementation.
    ///
    /// This should be deterministic as it is used to create multiple instances of a Stack
    /// for every replica during initialization.
    fn default() -> Self {
        const DEFAULT_STACK_SIZE: u32 = 1_000u32;

        let mut s = Stack {
            storage: Default::default(),
        };

        for _i in 0..DEFAULT_STACK_SIZE {
            s.storage.push(T::default());
        }

        s
    }
}

/// The Dispatch traits executes `ReadOperation` (our Access enum) and `WriteOperation`
/// (our `Modify` enum) against the replicated data-structure.
impl<T> Dispatch for Stack<T>
where
    T: Default + Clone + PartialEq + Send,
{
    type ReadOperation = Access;
    type WriteOperation = Modify<T>;
    type Response = Option<T>;

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

struct NrStack<T>(Arc<NodeReplicated<Stack<T>>>, ThreadToken)
where
    T: Sync + Default + Clone + PartialEq + Send;

impl<T> Deref for NrStack<T>
where
    T: Sync + Default + Clone + PartialEq + Send,
{
    type Target = NodeReplicated<Stack<T>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> NrStack<T>
where
    T: Sync + Default + Clone + PartialEq + Send,
{
    fn push(&self, v: T) {
        self.execute_mut(Modify::Push(v), self.1);
    }

    fn pop(&self) -> Option<T> {
        self.execute_mut(Modify::Pop, self.1)
    }

    fn peek(&self) -> Option<T> {
        self.execute(Access::Peek, self.1)
    }
}

/// We initialize a replicated stack with two replicas, then spawn threads, assign them to
/// replicas and execute operations.
fn main() -> Result<(), NodeReplicatedError> {
    const NUM_REPLICAS: NonZeroUsize = NonZeroUsize::new(2).unwrap();
    const NUM_THREADS: usize = 3;
    const NUM_OPS_PER_THREAD: usize = 2048;

    let stack = Arc::new(NodeReplicated::new(NUM_REPLICAS, |_node| 0)?);

    // The replica executes a Modify or Access operations by calling
    // `execute_mut` and `execute`. Eventually they end up in the `Dispatch` trait.
    let thread_loop = |stack: NrStack<usize>| {
        for i in 0..NUM_OPS_PER_THREAD {
            match i % 3 {
                0 => {
                    stack.push(i);
                    Some(i)
                }
                1 => stack.pop(),
                2 => stack.peek(),
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
            thread_loop(NrStack(stack, ridx));
        }));
    }

    // Wait for all the threads to finish
    for thread in threads {
        thread.join().unwrap();
    }

    Ok(())
}
