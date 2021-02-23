// Copyright © VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Implements ReplicaTrait for a bunch of different lockfree DS implementations.

use std::convert::TryInto;
use std::marker::Sync;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam::epoch;
use crossbeam::queue::SegQueue;
use crossbeam_skiplist::SkipList;

use cnr::{Dispatch, Log, ReplicaToken};

use crate::mkbench::ReplicaTrait;

use super::{QueueConcurrent, SkipListConcurrent, INITIAL_CAPACITY};

/// A wrapper that implements ReplicaTrait which just submits everything against
/// the data-structure that is already concurrent.
///
/// Useful to compare against the competition.
///
/// Obviously this makes the most sense when run with `ReplicaStrategy::One`.
pub struct ConcurrentDs<T: Dispatch + Sync> {
    registered: AtomicUsize,
    data_structure: T,
}

unsafe impl<T> Sync for ConcurrentDs<T> where T: Dispatch + Default + Sync {}

impl<T> ReplicaTrait for ConcurrentDs<T>
where
    T: Dispatch + Default + Sync,
{
    type D = T;

    fn new_arc(
        _log: &Arc<Log<'static, <Self::D as Dispatch>::WriteOperation>>,
    ) -> std::sync::Arc<Self> {
        Arc::new(ConcurrentDs {
            registered: AtomicUsize::new(0),
            data_structure: T::default(),
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
        _idx: ReplicaToken,
    ) -> <Self::D as Dispatch>::Response {
        self.data_structure.dispatch(op)
    }
}

pub struct SegQueueWrapper(SegQueue<u64>);

impl Default for SegQueueWrapper {
    fn default() -> Self {
        let storage = SegQueue::new();
        for i in 0..INITIAL_CAPACITY {
            storage.push(i as u64);
        }
        SegQueueWrapper(storage)
    }
}

impl Dispatch for SegQueueWrapper {
    type ReadOperation = QueueConcurrent;
    type WriteOperation = ();
    type Response = Result<Option<u64>, ()>;

    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            QueueConcurrent::Push(val) => {
                self.0.push(val);
                Ok(Some(val))
            }
            QueueConcurrent::Pop => Ok(self.0.pop().ok()),
            QueueConcurrent::Len => Ok(Some(self.0.len().try_into().unwrap())),
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(&mut self, _op: Self::WriteOperation) -> Self::Response {
        unreachable!("dispatch_mut should not be called here")
    }
}

#[derive(Debug, Clone)]
pub struct SkipListWrapper(SkipList<u64, u64>);

impl Default for SkipListWrapper {
    fn default() -> Self {
        let guard = &epoch::pin();

        let storage = SkipList::new(epoch::default_collector().clone());
        for i in 0..INITIAL_CAPACITY {
            storage.insert(i as u64, i as u64, guard);
        }
        SkipListWrapper(storage)
    }
}

impl Dispatch for SkipListWrapper {
    type ReadOperation = SkipListConcurrent;
    type WriteOperation = ();
    type Response = Result<Option<u64>, ()>;

    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            SkipListConcurrent::Get(key) => Ok(self.0.get(&key, &epoch::pin()).map(|e| *e.value())),
            /*SkipListConcurrent::Push(key, val) => {
                self.0.insert(key, val, &epoch::pin());
                Ok(Some(key))
            }*/
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(&mut self, _op: Self::WriteOperation) -> Self::Response {
        unreachable!("dispatch_mut should not be called here")
    }
}
