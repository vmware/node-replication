// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A minimal example that implements a replicated hashmap
use chashmap::CHashMap as HashMap;
use std::sync::Arc;

use node_replication::Dispatch;
use node_replication::Log;
use node_replication::LogMapper;
use node_replication::Replica;

/// The node-replicated hashmap uses a std hashmap internally.
#[derive(Default)]
struct NrHashMap {
    storage: HashMap<u64, u64>,
}

/// We support mutable put operation on the hashmap.
#[derive(Hash, Clone, Debug, PartialEq)]
enum Modify {
    Put(u64, u64),
}

impl LogMapper for Modify {
    fn hash(&self) -> usize {
        0
    }
}

/// We support an immutable read operation to lookup a key from the hashmap.
#[derive(Hash, Clone, Debug, PartialEq)]
enum Access {
    Get(u64),
}

impl LogMapper for Access {
    fn hash(&self) -> usize {
        0
    }
}

/// The Dispatch traits executes `ReadOperation` (our Access enum)
/// and `WriteOperation` (our `Modify` enum) against the replicated
/// data-structure.
impl Dispatch for NrHashMap {
    type ReadOperation = Access;
    type WriteOperation = Modify;
    type Response = Option<u64>;

    /// The `dispatch` function applies the immutable operations.
    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            Access::Get(key) => self.storage.get(&key).map(|v| *v),
        }
    }

    /// The `dispatch_mut` function applies the mutable operations.
    fn dispatch_mut(&self, op: Self::WriteOperation) -> Self::Response {
        match op {
            Modify::Put(key, value) => self.storage.insert(key, value),
        }
    }
}

/// We initialize a log, and two replicas for a hashmap, register with the replica
/// and then execute operations.
fn main() {
    // The operation log for storing `WriteOperation`, it has a size of 2 MiB:
    let log = Arc::new(Log::<<NrHashMap as Dispatch>::WriteOperation>::new(
        2 * 1024 * 1024,
        1,
        |_idx: usize, _rid: usize| {},
    ));

    // Next, we create two replicas of the hashmap
    let replica1 = Replica::<NrHashMap>::new(vec![log.clone()]);
    let replica2 = Replica::<NrHashMap>::new(vec![log.clone()]);

    // The replica executes a Modify or Access operations by calling
    // `execute_mut` and `execute`. Eventually they end up in the `Dispatch` trait.
    let thread_loop = |replica: &Arc<Replica<NrHashMap>>, ridx| {
        for i in 0..2048 {
            let _r = match i % 2 {
                0 => replica.execute_mut(Modify::Put(i, i + 1), ridx),
                1 => {
                    let response = replica.execute(Access::Get(i - 1), ridx);
                    assert_eq!(response, Some(i));
                    response
                }
                _ => unreachable!(),
            };
        }
    };

    // Finally, we spawn three threads that issue operations, thread 1 and 2
    // will use replica1 and thread 3 will use replica 2:
    let mut threads = Vec::with_capacity(3);
    let replica11 = replica1.clone();
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
