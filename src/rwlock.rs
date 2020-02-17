// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use core::cell::UnsafeCell;
use core::default::Default;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{spin_loop_hint, AtomicBool, AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

/// Maximum amount of reader (threads) that this lock supports.
const MAX_READER_THREADS: usize = 128;

/// The struct declares a Reader-writer lock. The reader-writer lock implementation is
/// inspired by Sec 5.5 NR paper ASPLOS 2017. There are separate locks for readers and
/// writers. Writer must acquire the writer lock and wait for all the readers locks to
/// be released, without acquiring them. And the readers only acquire the lock when there
/// is no thread with an acquired writer lock.
pub struct RwLock<T>
where
    T: Sized + Default + Sync,
{
    /// This field is used for the writer lock. There can only be one writer at a time.
    wlock: CachePadded<AtomicBool>,

    /// Each reader use different reader lock for accessing the underlying data-structure.
    rlock: [CachePadded<AtomicUsize>; MAX_READER_THREADS],

    /// This field wraps the underlying data-structure in an `UnsafeCell`.
    data: UnsafeCell<T>,
}

/// This structure is used by the readers.
pub struct ReadGuard<'a, T: ?Sized + Default + Sync + 'a> {
    /// The thread-id is needed at the drop time to unlock the readlock for a particular thread.
    tid: usize,

    /// A reference to the Rwlock and underlying data-structure.
    lock: &'a RwLock<T>,
}

/// This structure is used by the writers.
pub struct WriteGuard<'a, T: ?Sized + Default + Sync + 'a> {
    /// A reference to the Rwlock and underlying data-structure.
    lock: &'a RwLock<T>,
}

impl<T> Default for RwLock<T>
where
    T: Sized + Default + Sync,
{
    /// Create a new instance of a RwLock.
    fn default() -> RwLock<T> {
        use arr_macro::arr;

        RwLock {
            wlock: CachePadded::new(AtomicBool::new(false)),
            rlock: arr![Default::default(); 128],
            data: UnsafeCell::new(T::default()),
        }
    }
}

impl<T> RwLock<T>
where
    T: Sized + Default + Sync,
{
    /// Lock the underlying data-structure in write mode. The application can get a mutable
    /// reference from `WriteGuard`. Only one writer should succeed in acquiring this type
    /// of lock.
    ///
    /// `n` is the number of active readers for the replica and we only check read-lock for
    /// only `n` threads before acquiring the write-lock.
    pub fn write(&self, n: usize) -> WriteGuard<T> {
        unsafe {
            // Acquire the writer lock.
            while self.wlock.compare_and_swap(false, true, Ordering::Acquire) {
                spin_loop_hint();
            }

            // Wait for all the reader to exit before returning.
            loop {
                let is_all_zero = self
                    .rlock
                    .iter()
                    .take(n)
                    .all(|item| item.load(Ordering::Relaxed) == 0);
                if !is_all_zero {
                    spin_loop_hint();
                    continue;
                }
                return WriteGuard::new(self);
            }
        }
    }

    /// Lock the underlying data-structure in read mode. The application can get a mutable
    /// reference from `ReadGuard`. Multiple reader can acquire this type of of lock at a time.
    pub fn read(&self, tid: usize) -> ReadGuard<T> {
        unsafe {
            loop {
                // Since the next check is merely a performance enhancement and we acquire the
                // writer-lock again after acquiring read-lock, we can read `wlock` without forcing
                // the atomic load.
                let write_lock_ptr = &*(&self.wlock
                    as *const crossbeam_utils::CachePadded<core::sync::atomic::AtomicBool>
                    as *const bool);
                if *write_lock_ptr {
                    // Spin when there is an active writer.
                    spin_loop_hint();
                    continue;
                }

                self.rlock[tid].fetch_add(1, Ordering::SeqCst);
                if self.wlock.load(Ordering::Relaxed) {
                    self.rlock[tid].fetch_sub(1, Ordering::Release);
                } else {
                    return ReadGuard::new(self, tid);
                }
            }
        }
    }

    /// Private function to unlock the writelock; called through drop() function.
    pub(in crate::rwlock) unsafe fn write_unlock(&self) {
        if !self.wlock.compare_and_swap(true, false, Ordering::Acquire) {
            panic!("write_unlock() is called without acquiring the write lock");
        }
    }

    /// Private function to unlock the readlock; called through the drop() function.
    pub(in crate::rwlock) unsafe fn read_unlock(&self, tid: usize) {
        if self.rlock[tid].fetch_sub(1, Ordering::Release) == 0 {
            panic!("read_unlock() is called without acquiring the read lock");
        }
    }
}

impl<'rwlock, T: ?Sized + Default + Sync> ReadGuard<'rwlock, T> {
    unsafe fn new(lock: &'rwlock RwLock<T>, tid: usize) -> ReadGuard<'rwlock, T> {
        ReadGuard { tid, lock }
    }
}

impl<'rwlock, T: ?Sized + Default + Sync> WriteGuard<'rwlock, T> {
    unsafe fn new(lock: &'rwlock RwLock<T>) -> WriteGuard<'rwlock, T> {
        WriteGuard { lock }
    }
}

/// `Sync` trait allows `RwLock` to be shared amoung threads.
unsafe impl<T: ?Sized + Default + Sync> Sync for RwLock<T> {}

/// `Deref` trait allows the application to use T from ReadGuard.
/// ReadGuard can only be dereferenced into immutable reference.
impl<T: ?Sized + Default + Sync> Deref for ReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}

/// `Deref` trait allows the application to use T from WriteGuard as
/// immutable reference.
impl<T: ?Sized + Default + Sync> Deref for WriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}

/// `DerefMut` trait allow the application to use T from WriteGuard as
/// mutable reference.
impl<T: ?Sized + Default + Sync> DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.data.get() }
    }
}

/// `Drop` trait helps the `ReadGuard` to implement unlock logic
/// for a readlock, once the readlock goes out of the scope.
impl<T: ?Sized + Default + Sync> Drop for ReadGuard<'_, T> {
    fn drop(&mut self) {
        unsafe {
            let tid = self.tid;
            self.lock.read_unlock(tid);
        }
    }
}

/// `Drop` trait helps the `WriteGuard` to implement unlock logic
/// for a writelock, once the writelock goes out of the scope.
impl<T: ?Sized + Default + Sync> Drop for WriteGuard<'_, T> {
    fn drop(&mut self) {
        unsafe {
            self.lock.write_unlock();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RwLock;
    use std::sync::Arc;
    use std::thread;
    use std::vec::Vec;

    /// This test checks if write-lock can return an mutable reference for a data-structure.
    #[test]
    fn test_writer_lock() {
        let lock = RwLock::<usize>::default();
        let val = 10;
        {
            let mut a = lock.write(1);
            *a = val;
        }
        assert_eq!(*lock.write(1), val);
    }

    /// This test checks if write-lock is dropped once the variable goes out of the scope.
    #[test]
    fn test_reader_lock() {
        let lock = RwLock::<usize>::default();
        let val = 10;
        {
            let mut a = lock.write(1);
            *a = val;
        }
        assert_eq!(*lock.read(1), val);
    }

    /// This test checks that multiple readers and writer can acquire the lock in an
    /// application if the scope of a writer doesn't interfere with the readers.
    #[test]
    fn test_different_lock_combinations() {
        let l = RwLock::<usize>::default();
        drop(l.read(1));
        drop(l.write(1));
        drop((l.read(1), l.read(2)));
        drop(l.write(1));
    }

    /// This test checks that the writes to the underlying data-structure are atomic.
    #[test]
    fn test_parallel_writer_sequential_writer() {
        let lock = Arc::new(RwLock::<usize>::default());
        let t = 100;

        let mut threads = Vec::new();
        for _i in 0..t {
            let l = lock.clone();
            let child = thread::spawn(move || {
                let mut ele = l.write(t);
                *ele += 1;
            });
            threads.push(child);
        }

        for _i in 0..threads.len() {
            let _retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Thread didn't finish successfully.");
        }
        assert_eq!(*lock.read(1), t);
    }

    /// This test checks that the multiple readers can work in parallel.
    #[test]
    fn test_parallel_writer_readers() {
        let lock = Arc::new(RwLock::<usize>::default());
        let t = 100;

        let mut threads = Vec::new();
        for _i in 0..t {
            let l = lock.clone();
            let child = thread::spawn(move || {
                let mut ele = l.write(t);
                *ele += 1;
            });
            threads.push(child);
        }

        for _i in 0..threads.len() {
            let _retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Writing didn't finish successfully.");
        }

        for i in 0..t {
            let l = lock.clone();
            let child = thread::spawn(move || {
                let ele = l.read(i);
                assert_eq!(*ele, t);
            });
            threads.push(child);
        }

        for _i in 0..threads.len() {
            let _retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Reading didn't finish successfully.");
        }
    }

    /// This test checks that the multiple readers can work in parallel in a single thread.
    #[test]
    fn test_parallel_readers_single_thread() {
        let lock = Arc::new(RwLock::<usize>::default());
        let t = 100;

        let mut threads = Vec::new();
        for _i in 0..t {
            let l = lock.clone();
            let child = thread::spawn(move || {
                let ele = l.read(1);
                assert_eq!(*ele, 0);
            });
            threads.push(child);
        }

        for _i in 0..threads.len() {
            let _retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Readers didn't finish successfully.");
        }
    }

    /// This test checks that write_unlock() shouldn't be called without acquiring the write lock.
    #[test]
    #[should_panic]
    fn test_writer_unlock_without_lock() {
        let lock = RwLock::<usize>::default();
        unsafe { lock.write_unlock() };
    }

    /// This test checks that read_unlock() shouldn't be called without acquiring the read lock.
    #[test]
    #[should_panic]
    fn test_reader_unlock_without_lock() {
        let lock = RwLock::<usize>::default();
        unsafe { lock.read_unlock(1) };
    }

    /// Second lock operation in this test would block indefinitely, and the main thread
    /// should panic after some time. The main thread won't panic if somehow there is a way
    /// such that one writer followed by a reader are in the critical section.
    #[test]
    #[should_panic(expected = "This test should always panic")]
    fn test_reader_after_writer() {
        let lock = RwLock::<usize>::default();
        let shared = Arc::new(std::sync::RwLock::new(0));

        let shared1 = shared.clone();
        let lock_thread = thread::spawn(move || {
            drop((lock.write(1), lock.read(0)));
            *shared1.write().unwrap() = 1;
        });

        thread::sleep(std::time::Duration::from_secs(2));
        if *shared.read().unwrap() == 0 {
            panic!("This test should always panic");
        }
        lock_thread.join().unwrap();
    }

    /// Second lock operation in this test would block indefinitely, and the main thread
    /// should panic after some time. The main thread won't panic if somehow there is a way
    /// such that one reader followed by a writer are in the critical section.
    #[test]
    #[should_panic(expected = "This test should always panic")]
    fn test_writer_after_reader() {
        let lock = RwLock::<usize>::default();
        let shared = Arc::new(std::sync::RwLock::new(0));

        let shared1 = shared.clone();
        let lock_thread = thread::spawn(move || {
            drop((lock.read(0), lock.write(1)));
            *shared1.write().unwrap() = 1;
        });

        thread::sleep(std::time::Duration::from_secs(2));
        if *shared.read().unwrap() == 0 {
            panic!("This test should always panic");
        }
        lock_thread.join().unwrap();
    }

    /// Second lock operation in this test would block indefinitely, and the main thread
    /// should panic after some time. The main thread won't panic if somehow there is a way
    /// such that one writer followed by a writer are in the critical section.
    #[test]
    #[should_panic(expected = "This test should always panic")]
    fn test_writer_after_writer() {
        let lock = RwLock::<usize>::default();
        let shared = Arc::new(std::sync::RwLock::new(0));

        let shared1 = shared.clone();
        let lock_thread = thread::spawn(move || {
            drop((lock.write(0), lock.write(0)));
            *shared1.write().unwrap() = 1;
        });

        thread::sleep(std::time::Duration::from_secs(2));
        if *shared.read().unwrap() == 0 {
            panic!("This test should always panic");
        }
        lock_thread.join().unwrap();
    }
}
