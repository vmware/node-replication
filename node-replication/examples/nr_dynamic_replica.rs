// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! An example that dynamically varies the amount of replicas over time.
#![feature(generic_associated_types)]

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;

use node_replication::nr::Dispatch;
use node_replication::nr::NodeReplicated;

/// The node-replicated hashmap uses a std hashmap internally.
#[derive(Default, Clone)]
struct NrHashMap {
    storage: HashMap<u64, u64>,
}

/// We support a mutable put operation to insert a value for a given key.
#[derive(Clone, Debug, PartialEq)]
enum Modify {
    /// Insert (key, value)
    Put(u64, u64),
}

/// We support an immutable read operation to lookup a key from the hashmap.
#[derive(Clone, Debug, PartialEq)]
enum Access {
    // Retrieve key.
    Get(u64),
}

/// The Dispatch trait executes `ReadOperation` (our Access enum) and `WriteOperation`
/// (our `Modify` enum) against the replicated data-structure.
impl Dispatch for NrHashMap {
    type ReadOperation<'rop> = Access;
    type WriteOperation = Modify;
    type Response = Option<u64>;

    /// The `dispatch` function contains the logic for the immutable operations.
    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
        match op {
            Access::Get(key) => self.storage.get(&key).map(|v| *v),
        }
    }

    /// The `dispatch_mut` function contains the logic for the mutable operations.
    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
        match op {
            Modify::Put(key, value) => self.storage.insert(key, value),
        }
    }
}

fn main() {
    // Setup logging and some constants.
    let _r = env_logger::try_init();

    const NUM_THREADS: usize = 4;

    // We start with 4 replicas.
    let initial_replicas: NonZeroUsize = NonZeroUsize::new(4).unwrap();
    let finished = Arc::new(AtomicBool::new(false));

    // The node-replicated hashmap is wrapped in an Arc<RwLock<>> to allow for
    // the RwLock is currently needed because `add_replica` and `remove_replica`
    // are not yet thread-safe. We will remove this in the future:
    let nrht = Arc::new(RwLock::new(
        NodeReplicated::<NrHashMap>::new(initial_replicas, |_rid| 0).unwrap(),
    ));

    // The worker threads will just issue operations until the `finished` flag is set.
    let thread_loop =
        |replica: Arc<RwLock<NodeReplicated<NrHashMap>>>, ttkn, finished: Arc<AtomicBool>| {
            let mut i = 0;
            while !finished.load(Ordering::Relaxed) {
                let _r = match i % 2 {
                    0 => replica
                        .read()
                        .unwrap()
                        .execute_mut(Modify::Put(i, i + 1), ttkn),
                    1 => {
                        let response = replica.read().unwrap().execute(Access::Get(i - 1), ttkn);
                        assert_eq!(response, Some(i));
                        response
                    }
                    _ => unreachable!(),
                };
                i += 1;

                if i % 1_000_000 == 0 {
                    println!("Thread {:?} executed {} operations", ttkn, i);
                }
            }
        };

    let mut threads = Vec::with_capacity(NUM_THREADS);
    for t in 0..NUM_THREADS {
        let nrht_cln = nrht.clone();
        let finished = finished.clone();
        threads.push(std::thread::spawn(move || {
            let ttkn = nrht_cln
                .read()
                .unwrap()
                .register(t % initial_replicas)
                .expect(
                    format!(
                        "Unable to register thread with replica {}",
                        t % initial_replicas
                    )
                    .as_str(),
                );
            thread_loop(nrht_cln, ttkn, finished);
        }));
    }

    // First we move from 4 to 1 replica by removing one every 3 seconds:
    for next_rid in &[3, 2, 1] {
        std::thread::sleep(std::time::Duration::from_secs(3));
        println!("About to remove replica {:?}", next_rid);
        let x = nrht.write().unwrap().remove_replica(*next_rid).unwrap();
        println!("Removed replica {:?}", x);
    }

    // Then we increase back from 1 to 4 replicas:
    for next_rid in &[1, 2, 3] {
        std::thread::sleep(std::time::Duration::from_secs(3));
        println!("About to add replica {:?}", next_rid);
        let x = nrht.write().unwrap().add_replica().unwrap();
        println!("Added replica {:?}", x);
    }

    finished.store(true, Ordering::Relaxed);
    // Wait for all the threads to finish
    for thread in threads {
        thread.join().unwrap();
    }
}
