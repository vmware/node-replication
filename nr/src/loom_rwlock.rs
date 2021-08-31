// Copyright © 2019-2021 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! An implementation of the custom NR RwLock API for loom tests. It justs wraps
//! the default Rwlock from loom.

#![cfg(loom)]

pub struct RwLock<T>
where
    T: Sized + Sync,
{
    inner: loom::sync::RwLock<T>,
}

impl<T> Default for RwLock<T>
where
    T: Sized + Default + Sync,
{
    fn default() -> RwLock<T> {
        RwLock {
            inner: loom::sync::RwLock::new(Default::default()),
        }
    }
}

impl<T> RwLock<T>
where
    T: Sized + Sync,
{
    pub fn new(t: T) -> Self {
        Self {
            inner: loom::sync::RwLock::new(t),
        }
    }

    pub fn write(&self, _n: usize) -> loom::sync::RwLockWriteGuard<'_, T> {
        self.inner.write().unwrap()
    }

    pub fn read(&self, _tid: usize) -> loom::sync::RwLockReadGuard<'_, T> {
        self.inner.read().unwrap()
    }
}
