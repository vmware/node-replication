// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A minimal example that impements a replicated BTreeSet.
#![feature(generic_associated_types)]

use crossbeam_skiplist::SkipSet;

use std::sync::Arc;

use node_replication::cnr::{Dispatch, Log, LogMapper, LogMetaData, Replica};

#[derive(Default)]
struct CnrBtreeSet {
    storage: SkipSet<u64>,
}

#[derive(Clone, Debug, PartialEq)]
enum Modify {
    Put(u64),
    Delete(u64),
}

impl LogMapper for Modify {
    fn hash(&self, _nlogs: usize, logs: &mut Vec<usize>) {
        logs.push(0);
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Access {
    Get(u64),
    Contains(u64),
}

impl LogMapper for Access {
    fn hash(&self, _nlogs: usize, logs: &mut Vec<usize>) {
        logs.push(0);
    }
}

impl Dispatch for CnrBtreeSet {
    type ReadOperation<'rop> = Access;
    type WriteOperation = Modify;
    type Response = Option<u64>;

    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
        match op {
            Access::Get(key) => self.storage.get(&key).map(|v| *v),
            Access::Contains(key) => {
                let response = self.storage.contains(&key);
                Some(response as u64)
            }
        }
    }

    fn dispatch_mut(&self, op: Self::WriteOperation) -> Self::Response {
        match op {
            Modify::Put(key) => {
                let response = self.storage.insert(key);
                Some(*response)
            }
            Modify::Delete(key) => {
                let response = self.storage.remove(&key).unwrap();
                Some(*response)
            }
        }
    }
}

/// We initialize a log, and two replicas for a B-tree, register with the replica
/// and then execute operations.
fn main() {
    const N_OPS: u64 = 10_000;
    // The operation log for storing `WriteOperation`, it has a size of 2 MiB:
    let log = Arc::new(
        Log::<<CnrBtreeSet as Dispatch>::WriteOperation>::new_with_bytes(
            2 * 1024 * 1024,
            LogMetaData::new(1),
        ),
    );

    // Create two replicas of the b-tree
    let replica1 = Replica::<CnrBtreeSet>::new(vec![log.clone()]);
    let replica2 = Replica::<CnrBtreeSet>::new(vec![log.clone()]);

    // The replica executes Modify or Access operations by calling
    // 'execute_mut' and `execute`. Eventually they end up in the `Dispatch` trait.
    let thread_loop = |replica: &Arc<Replica<CnrBtreeSet>>, starting_point: u64, ridx| {
        for i in starting_point..starting_point + N_OPS {
            let _r = match i % 4 {
                0 => {
                    let response = replica.execute_mut(Modify::Put(i), ridx);
                    assert_eq!(response, Some(i));
                    response
                }
                1 => {
                    let response = replica.execute(Access::Contains(i - 1), ridx);
                    assert_eq!(response, Some(1));
                    response
                }
                2 => {
                    let response = replica.execute(Access::Get(i - 2), ridx);
                    assert_eq!(response, Some(i - 2));
                    response
                }
                3 => {
                    let response = replica.execute_mut(Modify::Delete(i - 3), ridx);
                    assert_eq!(response, Some(i - 3));
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
    threads.push(
        std::thread::Builder::new()
            .name("thread 1".to_string())
            .spawn(move || {
                let ridx = replica11.register().expect("Unable to register with log");
                thread_loop(&replica11, 0, ridx);
            }),
    );

    let replica12 = replica1.clone();
    threads.push(
        std::thread::Builder::new()
            .name("thread 2".to_string())
            .spawn(move || {
                let ridx = replica12.register().expect("Unable to register with log");
                thread_loop(&replica12, 100000, ridx);
            }),
    );

    threads.push(
        std::thread::Builder::new()
            .name("thread 3".to_string())
            .spawn(move || {
                let ridx = replica2.register().expect("Unable to register with log");
                thread_loop(&replica2, 200000, ridx);
            }),
    );

    // Wait for all the threads to finish
    for thread in threads {
        thread
            .expect("all threads should complete")
            .join()
            .unwrap_or_default();
    }
}
