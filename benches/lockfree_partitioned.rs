// Copyright © VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Implements ReplicaTrait for a bunch of different lockfree DS implementations.

use std::convert::TryInto;
use std::marker::Sync;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::fmt::Debug;

use crossbeam::epoch;
use crossbeam::queue::SegQueue;
use crossbeam_skiplist::SkipList;

use node_replication::{Dispatch, Log, ReplicaToken};

use crate::mkbench::ReplicaTrait;

use super::{QueueConcurrent, SkipListConcurrent, INITIAL_CAPACITY};

/// A wrapper that implements ReplicaTrait which just submits everything against
/// the data-structure that is already concurrent.
///
/// Useful to compare against the competition.
///
/// Obviously this makes the most sense when run with `ReplicaStrategy::One`.
pub struct ConcurrentDs<T> {
    registered: AtomicUsize,
    data_structure: T,
}

unsafe impl<T> Sync for ConcurrentDs<T> {}

impl<T> ReplicaTrait for ConcurrentDs<T>
where 
    T: Dispatch<ReadOperation = SkipListConcurrent>,
    T: Default + Sync,
{
    type D = T;

    fn new_arc(
        _log: &Arc<Log<'static, <Self::D as Dispatch>::WriteOperation>>,
    ) -> std::sync::Arc<Self> {
        Arc::new(ConcurrentDs {
            registered: AtomicUsize::new(0),
            data_structure: Self::D::default(),
        })
    }

    fn register_me(&self) -> Option<ReplicaToken> {
        let rt = unsafe { ReplicaToken::new(self.registered.fetch_add(1, Ordering::SeqCst)) };
        Some(rt)
    }

    fn sync_me(&self, _idx: ReplicaToken) {
        /* NOP */
    }

    fn exec(
        &self,
        _op: <Self::D as Dispatch>::WriteOperation,
        _idx: ReplicaToken,
    ) -> <Self::D as Dispatch>::Response {
        unreachable!("All opertations must be read ops")
    }

    fn exec_ro(
        &self,
        op: <Self::D as Dispatch>::ReadOperation,
        idx: ReplicaToken,
    ) -> <Self::D as Dispatch>::Response {
        let op = match op {
            SkipListConcurrent::Get(key) => {
                SkipListConcurrent::Get(key + (idx.0 * 25_000_000) as u64)
            }
            SkipListConcurrent::Push(key, val) => {
                //log::error!("{} {} {}", key, idx.0, key + (idx.0 * 25_000_000) as u64 );
                SkipListConcurrent::Push(key + (idx.0 * 25_000_000) as u64, val)
            }
        };
        self.data_structure.dispatch(op)
    }
}


pub struct SkipListWrapper(SkipList<u64, u64>);

impl Default for SkipListWrapper {
    fn default() -> Self {
        use rand::{distributions::Distribution, Rng, RngCore, rngs::SmallRng, SeedableRng};
        use rand_xorshift::XorShiftRng;

        let guard = &epoch::pin();
        let mut rng = XorShiftRng::from_entropy();

        let storage = SkipList::new(epoch::default_collector().clone());
        log::error!("start init");
        for i in 0..INITIAL_CAPACITY {
            storage.insert(rng.next_u64() % (192*25_000_000), i as u64, guard);
        }
        log::error!("done init");
        SkipListWrapper(storage)
    }
}

impl Dispatch for SkipListWrapper {
    type ReadOperation = SkipListConcurrent;
    type WriteOperation = ();
    type Response = Result<Option<u64>, ()>;

    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            SkipListConcurrent::Get(key) => {
                Ok(self.0.get(&key, &epoch::pin()).map(|e| *e.value()))
            }
            SkipListConcurrent::Push(key, val) => {
                self.0.insert(key, val, &epoch::pin());
                Ok(Some(key))
            }
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(&mut self, _op: Self::WriteOperation) -> Self::Response {
        unreachable!("dispatch_mut should not be called here")
    }
}
