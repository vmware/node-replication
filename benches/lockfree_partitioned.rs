// Copyright © VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![feature(bench_black_box)]

//! Implements ReplicaTrait for a bunch of different lockfree DS implementations.
use std::marker::Sync;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;

use cnr::{Dispatch, Log, ReplicaToken};

use crate::mkbench::ReplicaTrait;

use super::{OpWr, SkipListConcurrent, INITIAL_CAPACITY};

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
    T: Dispatch<WriteOperation = OpWr>,
    T: Default + Sync,
{
    type D = T;

    fn new_arc(
        _log: Vec<Arc<Log<'static, <Self::D as Dispatch>::WriteOperation>>>,
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

    fn log_sync(&self, _idx: ReplicaToken, _logid: usize) {
        /* NOP */
    }

    fn exec(
        &self,
        op: <Self::D as Dispatch>::WriteOperation,
        _idx: ReplicaToken,
    ) -> <Self::D as Dispatch>::Response {
        let op = match op {
            OpWr::Push(key, val) => {
                //log::error!("{} {} {}", key, idx.0, key + (idx.0 * 25_000_000) as u64 );
                OpWr::Push(key, val)
            }
        };
        self.data_structure.dispatch_mut(op)
    }

    fn exec_scan(
        &self,
        _op: <Self::D as Dispatch>::WriteOperation,
        _idx: ReplicaToken,
    ) -> <Self::D as Dispatch>::Response {
        unreachable!("No scan op for lockfree partitioned DS!");
    }

    fn exec_ro(
        &self,
        op: <Self::D as Dispatch>::ReadOperation,
        _idx: ReplicaToken,
    ) -> <Self::D as Dispatch>::Response {
        let op = match op {
            SkipListConcurrent::Get(key) => SkipListConcurrent::Get(key),
        };
        self.data_structure.dispatch(op)
    }
}

pub struct SkipListWrapper(SkipMap<u64, u64>);

impl Default for SkipListWrapper {
    fn default() -> Self {
        let storage = SkipMap::new();
        for i in 0..INITIAL_CAPACITY {
            storage.insert(i as u64, i as u64);
        }
        SkipListWrapper(storage)
    }
}

impl Dispatch for SkipListWrapper {
    type ReadOperation = SkipListConcurrent;
    type WriteOperation = OpWr;
    type Response = Result<Option<u64>, ()>;

    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            SkipListConcurrent::Get(key) => Ok(self.0.get(&key).map(|e| *e.value())),
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(&self, op: Self::WriteOperation) -> Self::Response {
        match op {
            OpWr::Push(key, val) => {
                self.0.insert(key, val);
                Ok(Some(key))
            }
        }
    }
}
