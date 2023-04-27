// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A minimal example that impements a replicated BTreeSet.
#![feature(generic_associated_types)]

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use node_replication::nr::Dispatch;
use node_replication::nr::NodeReplicated;

#[derive(Default, Clone)]
struct NrBtreeSet {
    storage: BTreeSet<u64>,
}

#[derive(Clone, Debug, PartialEq)]
enum Modify {
    Put(u64),
    Delete(u64),
}

#[derive(Clone, Debug, PartialEq)]
enum Access {
    Get(u64),
    Contains(u64),
}

impl Dispatch for NrBtreeSet {
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

    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
        match op {
            Modify::Put(key) => {
                let response = self.storage.insert(key);
                Some(response as u64)
            }
            Modify::Delete(key) => {
                let response = self.storage.take(&key);
                response
            }
        }
    }
}

fn main() {
    let _r = env_logger::try_init();
    const N_OPS: u64 = 1_000_000;

    for n_replicas in 1..=4 {
        for thread_num in 1..=4 {
            let num_replica = NonZeroUsize::new(n_replicas).unwrap();
            let nrht = Arc::new(NodeReplicated::<NrBtreeSet>::new(num_replica, |_rid| 0).unwrap());

            let thread_loop = |replica: Arc<NodeReplicated<NrBtreeSet>>, ttkn, thread_id| {
                for i in (thread_id as u64) * N_OPS..(thread_id as u64 + 1) * N_OPS {
                    let _r = match i % 4 {
                        0 => replica.execute_mut(Modify::Put(i), ttkn),
                        1 => {
                            let val = replica.execute(Access::Contains(i - 1), ttkn);
                            assert_eq!(val, Some(1));
                            val
                        }
                        2 => {
                            let val = replica.execute(Access::Get(i - 2), ttkn);
                            assert_eq!(val, Some(i - 2));
                            val
                        }
                        3 => {
                            let val = replica.execute_mut(Modify::Delete(i - 3), ttkn);
                            assert_eq!(val, Some(i - 3));
                            val
                        }
                        _ => unreachable!(),
                    };
                }
            };

            let t_now = std::time::Instant::now();

            print!(
                "Running with {} replicas and {} threads",
                n_replicas, thread_num
            );

            let mut threads = Vec::with_capacity(thread_num);
            for t in 0..thread_num {
                let nrht_cln = nrht.clone();
                threads.push(std::thread::spawn(move || {
                    let ttkn = nrht_cln.register(t % n_replicas).expect(
                        format!("Unable to register thread with replica {}.", t % n_replicas)
                            .as_str(),
                    );
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    thread_loop(nrht_cln, ttkn, t);
                }));
            }

            for thread in threads {
                thread.join().unwrap();
            }

            println!(
                " ({} ns/op)",
                t_now.elapsed().as_nanos() / (thread_num as u128 * N_OPS as u128)
            );
        }
    }
}
