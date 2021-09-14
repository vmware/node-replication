//! A minimal example that implements a replicated hashmap
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use futures::future::join_all;
use futures::Future;

use node_replication::Dispatch;
use node_replication::Log;
use node_replication::Replica;

const CAPACITY: usize = 32;

/// The node-replicated hashmap uses a std hashmap internally.
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
    type ReadOperation = Access;
    type WriteOperation = Modify;
    type Response = usize;

    /// The `dispatch` function applies the immutable operations.
    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
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
    // The operation log for storing `WriteOperation`, it has a size of 2 MiB:
    let log = Arc::new(Log::<<NrHashMap as Dispatch>::WriteOperation>::new(
        2 * 1024 * 1024,
    ));

    // Next, we create a replica of the hashmap
    let replica = Replica::<NrHashMap>::new(&log);
    let ridx = replica.register().expect("Unable to register with log");

    // Issue multiple Put operations
    let mut futures = Vec::with_capacity(CAPACITY);
    for i in 0..CAPACITY {
        match i % 2 {
            0 => futures.push(
                Box::pin(replica.async_execute_mut(Modify::Put(i, i + 1), ridx).await)
                    as Pin<Box<dyn Future<Output = <NrHashMap as Dispatch>::Response>>>,
            ),
            1 => futures.push(Box::pin(replica.async_execute(Access::Get(i), ridx).await)),
            _ => unreachable!(),
        }
    }
    let resp = join_all(futures).await;

    // Verify responses
    for (i, item) in resp.iter().enumerate().take(CAPACITY) {
        assert_eq!(*item, i + 1);
    }
}
