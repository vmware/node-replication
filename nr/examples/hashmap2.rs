// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A minimal example that implements a replicated hashmap
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use log::info;
use node_replication::Dispatch;
use node_replication::NodeReplicated;

/// The node-replicated hashmap uses a std hashmap internally.
#[derive(Default)]
struct NrHashMap {
    storage: HashMap<u64, u64>,
}

/// We support mutable put operation on the hashmap.
#[derive(Clone, Debug, PartialEq)]
enum Modify {
    Put(u64, u64),
}

/// We support an immutable read operation to lookup a key from the hashmap.
#[derive(Clone, Debug, PartialEq)]
enum Access {
    Get(u64),
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
    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
        match op {
            Modify::Put(key, value) => self.storage.insert(key, value),
        }
    }
}

/// We initialize a log, and two replicas for a hashmap, register with the replica
/// and then execute operations.
fn main() {
    let _r = env_logger::try_init();
    // Next, we create two replicas of the hashmap

    for i in 4..=4 {
        let num_replica = NonZeroUsize::new(4).unwrap();
        let nrht = Arc::new(NodeReplicated::<NrHashMap>::new(num_replica, |_rid| {}).unwrap());

        // The replica executes a Modify or Access operations by calling
        // `execute_mut` and `execute`. Eventually they end up in the `Dispatch` trait.
        let thread_loop = |replica: Arc<NodeReplicated<NrHashMap>>, ttkn| {
            for i in 0..2_948_048 {
                let _r = match i % 2 {
                    0 => replica.execute_mut(Modify::Put(i, i + 1), ttkn),
                    1 => {
                        let response = replica.execute(Access::Get(i - 1), ttkn);
                        assert_eq!(response, Some(i));
                        response
                    }
                    _ => unreachable!(),
                };
            }
        };

        // Finally, we spawn three threads that issue operations, thread 1, 2, 3
        // will use replicas 1, 2, 3 -- 4th replica will implicitly be served by others
        // because we can in this model...
        let mut threads = Vec::with_capacity(4);
        let nrht_cln = nrht.clone();
        threads.push(std::thread::spawn(move || {
            let ttkn = nrht_cln.register(0).expect("Unable to register thread");
            thread_loop(nrht_cln, ttkn);
        }));
        let nrht_cln = nrht.clone();
        threads.push(std::thread::spawn(move || {
            let ttkn = nrht_cln.register(0).expect("Unable to register thread");
            thread_loop(nrht_cln, ttkn);
        }));

        /*let nrht_cln = nrht.clone();
        threads.push(std::thread::spawn(move || {
            let ttkn = nrht_cln.register(1).expect("Unable to register thread");
            thread_loop(nrht_cln, ttkn);
        }));

        let nrht_cln = nrht.clone();
        threads.push(std::thread::spawn(move || {
            let ttkn = nrht_cln.register(2).expect("Unable to register thread");
            thread_loop(nrht_cln, ttkn);
        }));*/

        // Wait for all the threads to finish
        for thread in threads {
            thread.join().unwrap();
        }
    }
}
