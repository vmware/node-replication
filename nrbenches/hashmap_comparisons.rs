// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Implements DsInterface for a bunch of different concurrent hashmap implementations.

use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::marker::Sync;
use std::mem;
use std::num::NonZeroUsize;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use node_replication::nr::{replica::ReplicaId, replica::ReplicaToken, Dispatch, ThreadToken};
use urcu_sys;

use super::{OpConcurrent, INITIAL_CAPACITY};
use crate::mkbench::DsInterface;

/// It looks like a Replica but it's really a fully partitioned data-structure.
///
/// This only makes sense to run with `ReplicaStrategy::PerThread`.
pub struct Partitioner<T: Dispatch> {
    registered: AtomicUsize,
    data_structure: UnsafeCell<T>,
}

// Ok because if more than one thread tries to register we would fail.
// This implies we have to run with `ReplicaStrategy::PerThread`.
unsafe impl<T> Sync for Partitioner<T> where T: Dispatch + Default + Sync {}

impl<T> DsInterface for Partitioner<T>
where
    T: Dispatch + Default + Sync,
{
    type D = T;

    fn new(_replicas: NonZeroUsize, _logs: NonZeroUsize, _log_size: usize) -> std::sync::Arc<Self> {
        Arc::new(Partitioner {
            registered: AtomicUsize::new(0),
            data_structure: UnsafeCell::new(T::default()),
        })
    }

    fn register(&self, rid: ReplicaId) -> Option<ThreadToken> {
        let r = self
            .registered
            .compare_exchange_weak(0, 1, Ordering::SeqCst, Ordering::SeqCst);
        if r == Ok(0) {
            Some(unsafe { ThreadToken::new(rid, ReplicaToken::new(1)) })
        } else {
            // Can't register more than one thread on partitioned DS
            None
        }
    }

    fn execute_mut(
        &self,
        op: <Self::D as Dispatch>::WriteOperation,
        _idx: ThreadToken,
    ) -> <Self::D as Dispatch>::Response {
        unsafe { (&mut *self.data_structure.get()).dispatch_mut(op) }
    }

    fn execute_scan(
        &self,
        op: <Self::D as Dispatch>::WriteOperation,
        _idx: ThreadToken,
    ) -> <Self::D as Dispatch>::Response {
        unsafe { (&mut *self.data_structure.get()).dispatch_mut(op) }
    }

    fn execute<'rop>(
        &self,
        op: <T as Dispatch>::ReadOperation<'rop>,
        _idx: ThreadToken,
    ) -> <T as Dispatch>::Response {
        unsafe { (&*self.data_structure.get()).dispatch(op) }
    }
}

/// A wrapper that implements DsInterface which just submits everything against
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

impl<T> DsInterface for ConcurrentDs<T>
where
    T: Dispatch + Default + Sync,
{
    type D = T;

    fn new(_replicas: NonZeroUsize, _logs: NonZeroUsize, _log_size: usize) -> std::sync::Arc<Self> {
        Arc::new(ConcurrentDs {
            registered: AtomicUsize::new(0),
            data_structure: T::default(),
        })
    }

    fn register(&self, rid: ReplicaId) -> Option<ThreadToken> {
        let rt = unsafe {
            ThreadToken::new(
                rid,
                ReplicaToken::new(self.registered.fetch_add(1, Ordering::SeqCst)),
            )
        };
        Some(rt)
    }

    fn execute_mut(
        &self,
        _op: <Self::D as Dispatch>::WriteOperation,
        _idx: ThreadToken,
    ) -> <Self::D as Dispatch>::Response {
        unreachable!("All operations must go through execute()")
    }

    fn execute_scan(
        &self,
        _op: <Self::D as Dispatch>::WriteOperation,
        _idx: ThreadToken,
    ) -> <Self::D as Dispatch>::Response {
        unreachable!("All operations must go through execute()")
    }

    fn execute<'rop>(
        &self,
        op: <Self::D as Dispatch>::ReadOperation<'rop>,
        _idx: ThreadToken,
    ) -> <Self::D as Dispatch>::Response {
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
    type ReadOperation<'rop> = OpConcurrent;
    type WriteOperation = ();
    type Response = Result<Option<u64>, ()>;

    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
        match op {
            OpConcurrent::Get(key) => Ok(self.0.get(&key).map(|v| *v)),
            OpConcurrent::Put(key, val) => {
                self.0.insert(key, val);
                Ok(None)
            }
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(&mut self, _op: Self::WriteOperation) -> Self::Response {
        unreachable!("dispatch_mut should not be called")
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
    type ReadOperation<'rop> = OpConcurrent;
    type WriteOperation = ();
    type Response = Result<Option<u64>, ()>;

    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
        match op {
            OpConcurrent::Get(key) => Ok(self.0.read().get(&key).map(|&v| v)),
            OpConcurrent::Put(key, val) => {
                self.0.write().insert(key, val);
                Ok(None)
            }
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(&mut self, _op: Self::WriteOperation) -> Self::Response {
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
    type ReadOperation<'rop> = OpConcurrent;
    type WriteOperation = ();
    type Response = Result<Option<u64>, ()>;

    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
        match op {
            OpConcurrent::Get(key) => Ok(self.0.pin().get(&key).map(|v| *v)),
            OpConcurrent::Put(key, val) => {
                self.0.pin().insert(key, val);
                Ok(None)
            }
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(&mut self, _op: Self::WriteOperation) -> Self::Response {
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
    type ReadOperation<'rop> = OpConcurrent;
    type WriteOperation = ();
    type Response = Result<Option<u64>, ()>;

    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
        match op {
            OpConcurrent::Get(key) => Ok(self.0.get(&key).map(|v| *v)),
            OpConcurrent::Put(key, val) => {
                self.0.insert(key, val);
                Ok(None)
            }
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(&mut self, _op: Self::WriteOperation) -> Self::Response {
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
            let test_ht: *mut urcu_sys::cds_lfht = urcu_sys::cds_lfht_new(
                crate::INITIAL_CAPACITY as u64, // initial hash-buckes
                crate::INITIAL_CAPACITY as u64, // minimal hash-buckets
                crate::INITIAL_CAPACITY as u64, // maximum hash-buckets
                urcu_sys::CDS_LFHT_AUTO_RESIZE as i32,
                ptr::null_mut(),
            );

            assert_ne!(test_ht, ptr::null_mut());
            let ht = RcuHashMap { test_ht };
            for i in 0..crate::KEY_SPACE {
                ht.dispatch(OpConcurrent::Put(i as u64, (i + 1) as u64))
                    .expect("Can't fill RCU map");
            }

            ht
        }
    }
}

impl Drop for RcuHashMap {
    fn drop(&mut self) {
        // Welcome to C land:
        unsafe {
            // Deallocate all entries in HT
            urcu_sys::rcu_read_lock();
            for key in 0..crate::KEY_SPACE {
                let mut iter: urcu_sys::cds_lfht_iter = mem::MaybeUninit::zeroed().assume_init();
                urcu_sys::cds_lfht_lookup(
                    self.test_ht,
                    key as u64,
                    Some(test_match),
                    key as u64 as *const c_void,
                    &mut iter as *mut urcu_sys::cds_lfht_iter,
                );
                let found_node: *mut urcu_sys::cds_lfht_node =
                    urcu_sys::cds_lfht_iter_get_node(&mut iter);

                if found_node != ptr::null_mut() {
                    let r = urcu_sys::cds_lfht_del(self.test_ht, found_node);
                    std::alloc::dealloc(
                        found_node as *mut u8,
                        std::alloc::Layout::new::<lfht_test_node>(),
                    );
                    assert_eq!(r, 0);
                };
            }
            urcu_sys::rcu_read_unlock();
            // Deallocate the HT itself
            let r = urcu_sys::cds_lfht_destroy(self.test_ht, ptr::null_mut());
            assert_eq!(r, 0);
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
    type ReadOperation<'rop> = OpConcurrent;
    type WriteOperation = ();
    type Response = Result<Option<u64>, ()>;

    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
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
    fn dispatch_mut(&mut self, _op: Self::WriteOperation) -> Self::Response {
        unreachable!("dispatch_mut should not be called here")
    }
}
