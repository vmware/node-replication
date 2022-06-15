// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A minimal example that implements a replicated BTreeSet
use std::collections::BTreeSet;
use std::sync::Arc;

use node_replication::Dispatch;
use node_replication::Log;
use node_replication::Replica;

/// The node-replicated BTreeSet uses a std::collections::BTreeSet internally.
#[derive(Default)]
struct NrBtreeSet {
	storage: BTreeSet<u64>,
}

/// We support mutable put and delete operations on the set.
#[derive(Clone, Debug, PartialEq)]
enum Modify {
	Put(u64),
	Delete(u64)
}

/// We support immutable get and contains operations on the set.
#[derive(Clone, Debug, PartialEq)]
enum Access {
	Get(u64),
	Contains(u64),
}


/// The Dispatch traits executes `ReadOperation` (our Access enum)
/// and `WriteOperation` (our `Modify` enum) against the replicated
/// data-structure.
impl Dispatch for NrBtreeSet {
	type ReadOperation = Access;
	type WriteOperation = Modify;
	type Response = Option<u64>;

	/// Implement immutable read-only operations for the BTreeSet.
	fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
		match op {
			Access::Get(key) => self.storage.get(&key).map(|v| *v),
			Access::Contains(key) => {
				let response = self.storage.contains(&key);
				Some(response as u64)
			},
		}
	}

	/// Implement mutable write and read operations for the BTreeSet.
	fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
		match op {
			Modify::Put(key) => {
				let response = self.storage.insert(key);
				Some(response as u64)
			},
			Modify::Delete(key) => {
				let response = self.storage.take(&key);
				response
			},
		}
	}
}



/// We initialize a log, and two replicas for a BTreeSet, register with the replica
/// and then execute operations.
fn main() {
    let _r = env_logger::try_init().expect("init logging");
    // The operation log for storing `WriteOperation`, it has a size of 2 MiB:
    let log = Arc::new(Log::<<NrBtreeSet as Dispatch>::WriteOperation>::new(
        2 * 1024 * 1024,
    ));

    // Next, we create two replicas of the hashmap
    let replica1 = Replica::<NrBtreeSet>::new(&log);
    let replica2 = Replica::<NrBtreeSet>::new(&log);
    const N_OPS: u64 = 2_948_048;

    // The replica executes a Modify or Access operations by calling
    // `execute_mut` and `execute`. Eventually they end up in the `Dispatch` trait.
	let thread_loop = |replica: Arc<Replica<NrBtreeSet>>, ttkn, thread_id| {
		println!("thread_id {} with assigned range: {:?}", thread_id, (thread_id as u64)*N_OPS..(thread_id as u64 + 1)*N_OPS);
		assert_eq!((thread_id as u64) * N_OPS % 4, 0);
		for i in (thread_id as u64)*N_OPS..(thread_id as u64 + 1)*N_OPS{
			let _r = match i % 4 {
				0 => {
					replica.execute_mut(Modify::Put(i), ttkn)
				},
				1 => {
					let val = replica.execute(Access::Contains(i-1), ttkn);
					assert_eq!(val, Some(1));
					val
				},
				2 => {
					let val = replica.execute(Access::Get(i-2), ttkn);
					assert_eq!(val, Some(i-2));
					val
				},
				3 => {
					let val = replica.execute_mut(Modify::Delete(i-3), ttkn);
					assert_eq!(val, Some(i-3));
					val
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
        thread_loop(replica11, ridx, 0);
    }));

    let replica12 = replica1.clone();
    threads.push(std::thread::spawn(move || {
        let ridx = replica12.register().expect("Unable to register with log");
        thread_loop(replica12, ridx, 1);
    }));

    let replica21 = replica2.clone();
    let _bgthread = std::thread::spawn(move || {
        let ridx = replica21.register().expect("Unable to register with log");
        loop {
            replica21.sync(ridx);
        }
    });

    let replica22 = replica2.clone();
    threads.push(std::thread::spawn(move || {
        let ridx = replica22.register().expect("Unable to register with log");
        thread_loop(replica22, ridx, 2);
    }));

    // Wait for all the threads to finish
    for thread in threads {
        thread.join().unwrap();
    }
}
