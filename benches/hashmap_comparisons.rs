// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Implements ReplicaTrait for a bunch of different concurrent hashmap implementations.

use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::marker::Sync;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use urcu_sys;

use node_replication::log::Log;

use node_replication::Dispatch;

use crate::mkbench::ReplicaTrait;

use super::{OpConcurrent, INITIAL_CAPACITY};

/// It looks like a Replica but it's really a fully partitioned data-structure.
///
/// This only makes sense to run with `ReplicaStrategy::PerThread`.
pub struct Partitioner<T: node_replication::Dispatch> {
    registered: AtomicUsize,
    data_structure: UnsafeCell<T>,
}

// Ok because if more than one thread tries to register we would fail.
// This implies we have to run with `ReplicaStrategy::PerThread`.
unsafe impl<T> Sync for Partitioner<T> where T: node_replication::Dispatch + Default + Sync {}

impl<T> ReplicaTrait<T> for Partitioner<T>
where
    T: node_replication::Dispatch + Default + Sync,
{
    fn new_arc(_log: &Arc<Log<'static, <T as Dispatch>::WriteOperation>>) -> std::sync::Arc<Self> {
        Arc::new(Partitioner {
            registered: AtomicUsize::new(0),
            data_structure: UnsafeCell::new(T::default()),
        })
    }

    fn register_me(&self) -> Option<usize> {
        let r = self.registered.compare_and_swap(0, 1, Ordering::SeqCst);
        if r == 0 {
            Some(0)
        } else {
            // Can't register more than one thread on partitioned DS
            None
        }
    }

    fn sync_me<F: FnMut(<T as Dispatch>::WriteOperation, usize)>(&self, _d: F) {
        /* NOP */
    }

    fn exec(
        &self,
        op: <T as Dispatch>::WriteOperation,
        idx: usize,
    ) -> Result<<T as Dispatch>::Response, <T as Dispatch>::ResponseError> {
        debug_assert_eq!(idx, 0);
        unsafe { (&mut *self.data_structure.get()).dispatch_mut(op) }
    }

    fn exec_ro(
        &self,
        op: <T as Dispatch>::ReadOperation,
        idx: usize,
    ) -> Result<<T as Dispatch>::Response, <T as Dispatch>::ResponseError> {
        debug_assert_eq!(idx, 0);
        unsafe { (&*self.data_structure.get()).dispatch(op) }
    }
}

/// A wrapper that implements ReplicaTrait which just submits everything against
/// the data-structure that is already concurrent.
///
/// Useful to compare against the competition.
///
/// Obviously this makes the most sense when run with `ReplicaStrategy::One`.
pub struct ConcurrentDs<T: node_replication::Dispatch + std::marker::Sync> {
    registered: AtomicUsize,
    data_structure: T,
}

unsafe impl<T> Sync for ConcurrentDs<T> where T: node_replication::Dispatch + Default + Sync {}

impl<T> ReplicaTrait<T> for ConcurrentDs<T>
where
    T: node_replication::Dispatch + Default + Sync,
{
    fn new_arc(_log: &Arc<Log<'static, <T as Dispatch>::WriteOperation>>) -> std::sync::Arc<Self> {
        Arc::new(ConcurrentDs {
            registered: AtomicUsize::new(0),
            data_structure: T::default(),
        })
    }

    fn register_me(&self) -> Option<usize> {
        Some(self.registered.fetch_add(1, Ordering::SeqCst))
    }

    fn sync_me<F: FnMut(<T as Dispatch>::WriteOperation, usize)>(&self, _d: F) {
        /* NOP */
    }

    fn exec(
        &self,
        _op: <T as Dispatch>::WriteOperation,
        _idx: usize,
    ) -> Result<<T as Dispatch>::Response, <T as Dispatch>::ResponseError> {
        unreachable!("All opertations must be read ops")
    }

    fn exec_ro(
        &self,
        op: <T as Dispatch>::ReadOperation,
        _idx: usize,
    ) -> Result<<T as Dispatch>::Response, <T as Dispatch>::ResponseError> {
        self.data_structure.dispatch(op)
    }
}

/// chashmap implementation
pub struct CHashMapWrapper(chashmap::CHashMap<u64, u64>);

impl Default for CHashMapWrapper {
    fn default() -> Self {
        let storage = chashmap::CHashMap::with_capacity(INITIAL_CAPACITY);
        for i in 0..INITIAL_CAPACITY {
            storage.insert(i as u64, (i + 1) as u64);
        }
        CHashMapWrapper(storage)
    }
}

impl Dispatch for CHashMapWrapper {
    type ReadOperation = OpConcurrent;
    type WriteOperation = ();
    type Response = Option<u64>;
    type ResponseError = ();

    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpConcurrent::Get(key) => Ok(self.0.get(&key).map(|v| *v)),
            OpConcurrent::Put(key, val) => {
                self.0.insert(key, val);
                Ok(None)
            }
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(
        &mut self,
        _op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        unreachable!("dispatch_mut should not be called here")
    }
}

/// rwlock<hashmap> implementation
pub struct StdWrapper(parking_lot::RwLock<std::collections::HashMap<u64, u64>>);

impl Default for StdWrapper {
    fn default() -> Self {
        let mut storage = std::collections::HashMap::with_capacity(INITIAL_CAPACITY);
        for i in 0..INITIAL_CAPACITY {
            storage.insert(i as u64, (i + 1) as u64);
        }
        StdWrapper(parking_lot::RwLock::new(storage))
    }
}

impl Dispatch for StdWrapper {
    type ReadOperation = OpConcurrent;
    type WriteOperation = ();
    type Response = Option<u64>;
    type ResponseError = ();

    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpConcurrent::Get(key) => Ok(self.0.read().get(&key).map(|&v| v)),
            OpConcurrent::Put(key, val) => {
                self.0.write().insert(key, val);
                Ok(None)
            }
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(
        &mut self,
        _op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        unreachable!("dispatch_mut should not be called here")
    }
}

/// flurry implementation
pub struct FlurryWrapper(flurry::HashMap<u64, u64>);

impl Default for FlurryWrapper {
    fn default() -> Self {
        let storage = flurry::HashMap::with_capacity(INITIAL_CAPACITY);
        for i in 0..INITIAL_CAPACITY {
            storage.pin().insert(i as u64, (i + 1) as u64);
        }
        FlurryWrapper(storage)
    }
}

impl Dispatch for FlurryWrapper {
    type ReadOperation = OpConcurrent;
    type WriteOperation = ();
    type Response = Option<u64>;
    type ResponseError = ();

    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpConcurrent::Get(key) => Ok(self.0.pin().get(&key).map(|v| *v)),
            OpConcurrent::Put(key, val) => {
                self.0.pin().insert(key, val);
                Ok(None)
            }
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(
        &mut self,
        _op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        unreachable!("dispatch_mut should not be called here")
    }
}

/// dashmap implementation
pub struct DashWrapper(dashmap::DashMap<u64, u64>);

impl Default for DashWrapper {
    fn default() -> Self {
        let storage = dashmap::DashMap::with_capacity(INITIAL_CAPACITY);
        for i in 0..INITIAL_CAPACITY {
            storage.insert(i as u64, (i + 1) as u64);
        }
        DashWrapper(storage)
    }
}

impl Dispatch for DashWrapper {
    type ReadOperation = OpConcurrent;
    type WriteOperation = ();
    type Response = Option<u64>;
    type ResponseError = ();

    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpConcurrent::Get(key) => Ok(self.0.get(&key).map(|v| *v)),
            OpConcurrent::Put(key, val) => {
                self.0.insert(key, val);
                Ok(None)
            }
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(
        &mut self,
        _op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        unreachable!("dispatch_mut should not be called here")
    }
}

// rcu wrapper
pub struct RcuHashMap {
    test_ht: *mut urcu_sys::cds_lfht,
}

unsafe impl Sync for RcuHashMap {}
unsafe impl Send for RcuHashMap {}

impl Default for RcuHashMap {
    fn default() -> Self {
        unsafe {
            // Not quite using 5M entries since cds_lfht needs power-of-twos
            let test_ht: *mut urcu_sys::cds_lfht = urcu_sys::cds_lfht_new(
                1 << 22, // initial hash-buckes  2^22
                1 << 22, // minimal hash-buckets 2^22
                1 << 24, // maximum hash-buckets 2^23
                urcu_sys::CDS_LFHT_AUTO_RESIZE as i32,
                ptr::null_mut(),
            );
            assert_ne!(test_ht, ptr::null_mut());
            RcuHashMap { test_ht }
        }
    }
}

#[repr(C)]
struct lfht_test_node {
    node: urcu_sys::cds_lfht_node,
    key: u64,
    data: u64,
    /* cache-cold for iteration */
    head: urcu_sys::rcu_head,
}

unsafe extern "C" fn test_match(node: *mut urcu_sys::cds_lfht_node, key: *const c_void) -> i32 {
    let my_key = key as u64;
    let test_node: *mut lfht_test_node = to_test_node(node);
    (my_key == (*test_node).key) as i32
}

unsafe fn to_test_node(node: *mut urcu_sys::cds_lfht_node) -> *mut lfht_test_node {
    mem::transmute(node)
}

impl Dispatch for RcuHashMap {
    type ReadOperation = OpConcurrent;
    type WriteOperation = ();
    type Response = Option<u64>;
    type ResponseError = ();

    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        unsafe {
            match op {
                OpConcurrent::Get(key) => {
                    urcu_sys::rcu_read_lock();

                    let mut iter: urcu_sys::cds_lfht_iter =
                        mem::MaybeUninit::zeroed().assume_init();
                    urcu_sys::cds_lfht_lookup(
                        self.test_ht,
                        key,
                        Some(test_match),
                        key as *const c_void,
                        &mut iter as *mut urcu_sys::cds_lfht_iter,
                    );
                    let found_node: *mut urcu_sys::cds_lfht_node =
                        urcu_sys::cds_lfht_iter_get_node(&mut iter);

                    let value = if found_node != ptr::null_mut() {
                        (*to_test_node(found_node)).data
                    } else {
                        0
                    };

                    urcu_sys::rcu_read_unlock();
                    if value != 0 {
                        Ok(Some(value))
                    } else {
                        Ok(None)
                    }
                }
                OpConcurrent::Put(key, val) => {
                    urcu_sys::rcu_read_lock();
                    let layout = std::alloc::Layout::new::<lfht_test_node>();
                    let new_node: *mut lfht_test_node =
                        std::alloc::alloc_zeroed(layout) as *mut lfht_test_node;
                    (*new_node).key = key;
                    (*new_node).data = val;

                    let old_node: *mut urcu_sys::cds_lfht_node = urcu_sys::cds_lfht_add_replace(
                        self.test_ht,
                        key,
                        Some(test_match),
                        key as *const c_void,
                        &mut (*new_node).node as *mut urcu_sys::cds_lfht_node,
                    );

                    urcu_sys::rcu_read_unlock();

                    if old_node != ptr::null_mut() {
                        // 1 - wait till readers are done
                        urcu_sys::synchronize_rcu();
                        // 2 - free the node
                        std::alloc::dealloc(old_node as *mut u8, layout);
                    }
                    Ok(None)
                }
            }
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(
        &mut self,
        _op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        unreachable!("dispatch_mut should not be called here")
    }
}
