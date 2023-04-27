// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A minimal example that implements a replicated hashmap and inserts and reads
//! from it using async functions.
#![feature(generic_associated_types)]

use std::collections::HashMap;
use std::num::NonZeroUsize;

use futures::future::join_all;

use node_replication::nr::reusable_box::ReusableBoxFuture;
use node_replication::nr::Dispatch;
use node_replication::nr::NodeReplicated;

const CAPACITY: usize = 32;

/// The node-replicated hashmap uses a std hashmap internally.
#[derive(Clone)]
struct NrHashMap {
    storage: HashMap<usize, usize>,
}

impl Default for NrHashMap {
    fn default() -> Self {
        let mut storage = HashMap::with_capacity(CAPACITY);
        for i in 0..CAPACITY {
            storage.insert(i, i + 1);
        }
        NrHashMap { storage }
    }
}

/// We support mutable put operation on the hashmap.
#[derive(Clone, Debug, PartialEq)]
enum Modify {
    Put(usize, usize),
}

/// We support an immutable read operation to lookup a key from the hashmap.
#[derive(Clone, Debug, PartialEq)]
enum Access {
    Get(usize),
}

/// The Dispatch traits executes `ReadOperation` (our Access enum)
/// and `WriteOperation` (our `Modify` enum) against the replicated
/// data-structure.
impl Dispatch for NrHashMap {
    type ReadOperation<'rop> = Access;
    type WriteOperation = Modify;
    type Response = usize;

    /// The `dispatch` function applies the immutable operations.
    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
        match op {
            Access::Get(key) => *self.storage.get(&key).unwrap(),
        }
    }

    /// The `dispatch_mut` function applies the mutable operations.
    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
        match op {
            Modify::Put(key, value) => self.storage.insert(key, value).unwrap(),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let replicas = NonZeroUsize::new(1).unwrap();
    let async_nrht =
        NodeReplicated::<NrHashMap>::new(replicas, |_ac| 0).expect("Can't create NrHashMap");
    let ttkn = async_nrht.register(0).expect("Unable to register with log");

    // Issue multiple Put operations
    let mut i = 0;
    let mut futures = Vec::with_capacity(CAPACITY);
    for _ in 0..CAPACITY {
        futures.push(ReusableBoxFuture::new(async move { Default::default() }));
    }
    for fut in &mut futures {
        match i % 2 {
            0 => {
                async_nrht
                    .async_execute_mut(Modify::Put(i, i + 1), ttkn, fut)
                    .await
            }
            1 => async_nrht.async_execute(Access::Get(i), ttkn, fut),
            _ => unreachable!(),
        }
        i += 1;
    }
    assert_eq!(futures.len(), CAPACITY);
    let resp = join_all(futures).await;
    assert_eq!(resp.len(), CAPACITY);

    // Verify responses
    for (i, item) in resp.iter().enumerate().take(CAPACITY) {
        println!("resp is {:?}", item);
        assert_eq!(*item, i + 1);
    }
}
