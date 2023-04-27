// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::cell::UnsafeCell;
use core::default::Default;
use core::hint::spin_loop;
use core::ops::FnMut;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

pub use crate::log::DEFAULT_LOG_BYTES;
pub use crate::log::MAX_REPLICAS_PER_LOG;

pub use crate::log::LogToken;
pub use crate::log::GC_FROM_HEAD;
pub use crate::log::WARN_THRESHOLD;

/// Callback function which indicates which replicas need to be advanced for GC
/// to make progress.
type CallbackFn = dyn FnMut(&[AtomicBool; MAX_REPLICAS_PER_LOG], usize);

/// The meta-data we need to store in the log entries to make scan (multi-log)
/// operations works.
#[derive(Default)]
pub struct EntryMetaData {
    /// Identifies if the operation is of scan type or not.
    is_scan: bool,

    /// Identifies the replica-local thread-id that issued the operation.
    thread: usize,

    /// Identifies if the operation is immutable scan or not.
    is_read_op: bool,

    /// If operation is of scan type, then `depends_on` stores
    /// the offsets in other logs this operation depends on.
    depends_on: Option<Arc<Vec<usize>>>,

    /// Used to remove operation once all the replica consumes the entry.
    refcnt: AtomicUsize,
}

/// The additional meta-data we need to store in the log for CNR operations.
pub struct LogMetaData {
    /// A global unique id for each log.
    pub(crate) idx: usize,

    /// Use to append scan op atomically to all the logs.
    pub(crate) scanlock: CachePadded<AtomicUsize>,

    /// The application can provide a callback function to the log. The function is invoked
    /// when one or more replicas lag and stop the log to garbage collected the entries.
    /// One example of a callback function is to notify the application with a lagging
    /// replica number for this log. The application can then take action to start the log
    /// consumption on that replica. If the application is proactively taking measures to
    /// consume the log on all the replicas, then the variable can be initialized to default.
    gc: UnsafeCell<Box<CallbackFn>>,

    /// Check if the log can issue a GC callback; reset
    /// after the GC is done in `advance_head` function.
    notify_replicas: CachePadded<AtomicBool>,

    /// Use this array in GC callback function to notify other replicas to make progress.
    /// Assumes that the callback handler clears the replica-ids which need to do GC.
    dormant_replicas: [AtomicBool; MAX_REPLICAS_PER_LOG],
}

impl LogMetaData {
    pub fn new(idx: usize) -> Self {
        #[allow(clippy::declare_interior_mutable_const)]
        const DORMANT_DEFAULT: AtomicBool = AtomicBool::new(false);

        Self {
            idx,
            scanlock: CachePadded::new(AtomicUsize::new(0)),
            gc: UnsafeCell::new(Box::new(
                |_rid: &[AtomicBool; MAX_REPLICAS_PER_LOG], _lid: usize| {},
            )),
            notify_replicas: CachePadded::new(AtomicBool::new(true)),
            dormant_replicas: [DORMANT_DEFAULT; MAX_REPLICAS_PER_LOG],
        }
    }
}

impl Default for LogMetaData {
    fn default() -> Self {
        Self::new(1)
    }
}

pub type Log<T> = crate::log::Log<T, LogMetaData, EntryMetaData>;

impl<T> Log<T>
where
    T: Sized + Clone,
{
    /// Adds a batch of operations to the shared log.
    ///
    /// # Note
    /// `append` is not intended as a public interface. It is marked
    /// as public due to being used by the benchmarking code.
    #[inline(always)]
    #[doc(hidden)]
    pub fn append<F: FnMut(T, usize, usize, bool, bool, Option<Arc<Vec<usize>>>) -> bool>(
        &self,
        ops: &[(T, usize, bool)],
        idx: &LogToken,
        mut s: F,
    ) {
        let nops = ops.len();
        let mut iteration = 1;
        let mut waitgc = 1;

        // Keep trying to reserve entries and add operations to the log until
        // we succeed in doing so.
        loop {
            if iteration % WARN_THRESHOLD == 0 {
                warn!(
                    "append(ops.len()={}, {}) takes too many iterations ({}) to complete...",
                    ops.len(),
                    idx.0,
                    iteration,
                );
            }
            iteration += 1;

            let tail = self.tail.load(Ordering::Relaxed);
            let head = self.head.load(Ordering::Relaxed);

            // Head and tail doesn't wrap around; so it works.
            let used = tail - head + 1;

            if used > self.slog.len() / 3 {
                let mut is_stuck = false;
                let cur_local_tail = self.ltails[&(idx.0)].load(Ordering::Relaxed);

                // Find the smallest local tail across all replicas.
                for idx_iter in 1..MAX_REPLICAS_PER_LOG {
                    let local_tail = self.ltails[&(idx_iter - 1)].load(Ordering::Relaxed);
                    if cur_local_tail > local_tail
                        && cur_local_tail - local_tail > self.slog.len() / 3
                    {
                        self.metadata.dormant_replicas[idx_iter - 1].store(true, Ordering::Relaxed);
                        is_stuck = true;
                    }
                }

                if is_stuck
                    && self.metadata.notify_replicas.compare_exchange_weak(
                        true,
                        false,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) == Ok(true)
                {
                    unsafe {
                        (*self.metadata.gc.get())(
                            &self.metadata.dormant_replicas,
                            self.metadata.idx,
                        )
                    };
                }
            }

            // If there are fewer than `GC_FROM_HEAD` entries on the log, then just
            // try again. The replica that reserved entry (h + self.size - GC_FROM_HEAD)
            // is currently trying to advance the head of the log. Keep refreshing the
            // replica against the log to make sure that it isn't deadlocking GC.
            if tail > head + self.slog.len() - GC_FROM_HEAD {
                if waitgc % WARN_THRESHOLD == 0 {
                    warn!(
                        "append(ops.len()={}, {}) takes too many iterations ({}) waiting for gc...",
                        ops.len(),
                        idx.0,
                        waitgc,
                    );
                }
                waitgc += 1;
                self.exec(idx, &mut s);
                continue;
            }

            // If on adding in the above entries there would be fewer than `GC_FROM_HEAD`
            // entries left on the log, then we need to advance the head of the log.
            let mut advance = false;
            if tail + nops > head + self.slog.len() - GC_FROM_HEAD {
                advance = true
            };

            // Try reserving slots for the operations. If that fails, then restart
            // from the beginning of this loop.
            if self.tail.compare_exchange_weak(
                tail,
                tail + nops,
                Ordering::Acquire,
                Ordering::Acquire,
            ) != Ok(tail)
            {
                continue;
            };

            // Successfully reserved entries on the shared log. Add the operations in.
            for (i, op) in ops.iter().enumerate().take(nops) {
                unsafe { self.update_entry(tail + i, op, idx.0, false, None) };
            }

            // If needed, advance the head of the log forward to make room on the log.
            if advance {
                self.advance_head(idx, &mut s);
            }

            return;
        }
    }

    /// Adds a scan operation to the shared log.
    #[inline(always)]
    #[doc(hidden)]
    pub(crate) fn try_append_scan<
        F: FnMut(T, usize, usize, bool, bool, Option<Arc<Vec<usize>>>) -> bool,
    >(
        &self,
        op: &(T, usize, bool),
        idx: &LogToken,
        offset: &[usize],
        mut s: F,
    ) -> Result<usize, usize> {
        let nops = 1;
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);

        // If there are fewer than `GC_FROM_HEAD` entries on the log, then just
        // try again. The replica that reserved entry (h + self.size - GC_FROM_HEAD)
        // is currently trying to advance the head of the log. Keep refreshing the
        // replica against the log to make sure that it isn't deadlocking GC.
        if tail > head + self.slog.len() - GC_FROM_HEAD {
            self.exec(idx, &mut s);
            return Err(0);
        }

        // If on adding in the above entries there would be fewer than `GC_FROM_HEAD`
        // entries left on the log, then we need to advance the head of the log.
        let mut advance = false;
        if tail + nops > head + self.slog.len() - GC_FROM_HEAD {
            advance = true;
        }

        // Try reserving slots for the operations. If that fails, then restart
        // from the beginning of this loop.
        if self
            .tail
            .compare_exchange_weak(tail, tail + nops, Ordering::Acquire, Ordering::Relaxed)
            != Ok(tail)
        {
            return Err(0);
        }

        // Successfully reserved entries on the shared log. Add the operations in.
        let log_offset = tail;
        if self.metadata.idx != 1 {
            unsafe {
                self.update_entry(log_offset, op, idx.0, true, Some(Arc::new(vec![offset[0]])))
            }
        }

        // If needed, advance the head of the log forward to make room on the log.
        if advance {
            self.advance_head(idx, &mut s);
        }

        Ok(log_offset)
    }

    /// Update the depends_on field for scan operation. Replica mainatins the
    /// offset for the scan entry and later it updates the remaining offset there.
    pub(crate) fn fix_scan_entry(
        &self,
        op: &(T, usize, bool),
        idx: &LogToken,
        offsets: Arc<Vec<usize>>,
    ) {
        unsafe { self.update_entry(offsets[0], op, idx.0, true, Some(offsets)) };
    }

    #[inline(always)]
    unsafe fn update_entry(
        &self,
        offset: usize,
        op: &(T, usize, bool),
        idx: usize,
        is_scan: bool,
        depends_on: Option<Arc<Vec<usize>>>,
    ) {
        let num_replicas = self.replica_count();
        let e = self.slog[self.index(offset)].as_ptr();
        let mut m = self.lmasks[&(idx - 1)].get();

        // This entry was just reserved so it should be dead (!= m). However, if
        // the log has wrapped around, then the alive mask has flipped. In this
        // case, we flip the mask we were originally going to write into the
        // allocated entry. We cannot flip lmasks[idx - 1] because this replica
        // might still need to execute a few entries before the wrap around.
        if (*e).alivef.load(Ordering::Relaxed) == m {
            m = !m;
        }

        (*e).operation = Some(op.0.clone());
        (*e).replica = idx;
        (*e).metadata.thread = op.1;
        (*e).metadata.is_scan = is_scan;
        (*e).metadata.is_read_op = op.2;
        (*e).metadata.depends_on = depends_on;
        (*e).metadata.refcnt = AtomicUsize::new(num_replicas);
        (*e).alivef.store(m, Ordering::Release);
    }

    /// Try to acquire the scan lock.
    fn try_scan_lock(&self, tid: usize) -> bool {
        if self.metadata.scanlock.compare_exchange_weak(
            0,
            tid,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) != Ok(0)
        {
            /* cas failed, we don't hold the lock */
            return false;
        }

        /* successfully acquired the lock */
        true
    }

    /// Acquire the scan lock.
    pub(crate) fn acquire_scan_lock(&self, tid: usize) {
        while !self.try_scan_lock(tid) {
            spin_loop();
        }
    }

    /// Release the scan lock.
    pub(crate) fn release_scan_lock(&self) {
        self.metadata.scanlock.store(0, Ordering::Release);
    }

    /// Executes a passed in closure (`d`) on all operations starting from
    /// a replica's local tail on the shared log. The replica is identified through an
    /// `idx` passed in as an argument.
    ///
    /// The passed in closure is expected to take in two arguments: The operation
    /// from the shared log to be executed and the replica that issued it.
    #[inline(always)]
    pub(crate) fn exec<F: FnMut(T, usize, usize, bool, bool, Option<Arc<Vec<usize>>>) -> bool>(
        &self,
        idx: &LogToken,
        d: &mut F,
    ) {
        // Load the logical log offset from which we must execute operations.
        let ltail = self.ltails[&(idx.0 - 1)].load(Ordering::Relaxed);

        // Check if we have any work to do by comparing our local tail with the log's
        // global tail. If they're equal, then we're done here and can simply return.
        let gtail = self.tail.load(Ordering::Relaxed);
        if ltail == gtail {
            return;
        }

        let h = self.head.load(Ordering::Relaxed);

        // Make sure we're within the shared log. If we aren't, then panic.
        if ltail > gtail || ltail < h {
            panic!("Local tail not within the shared log!")
        };

        // Execute all operations from the passed in offset to the shared log's tail. Check if
        // the entry is live first; we could have a replica that has reserved entries, but not
        // filled them into the log yet.
        for i in ltail..gtail {
            let mut iteration = 1;
            let e = self.slog[self.index(i)].as_ptr();

            while unsafe { (*e).alivef.load(Ordering::Acquire) != self.lmasks[&(idx.0 - 1)].get() }
            {
                if iteration % WARN_THRESHOLD == 0 {
                    warn!(
                        "alivef not being set for self.index(i={}) = {} (self.lmasks[{}] is {})...",
                        i,
                        self.index(i),
                        idx.0 - 1,
                        self.lmasks[&(idx.0 - 1)].get()
                    );
                }
                iteration += 1;
            }

            let mut depends_on = None;
            unsafe {
                if (*e).metadata.is_scan {
                    depends_on = Some((*e).metadata.depends_on.as_ref().unwrap().clone());
                }

                if !d(
                    (*e).operation.as_ref().unwrap().clone(),
                    (*e).replica,
                    (*e).metadata.thread,
                    (*e).metadata.is_scan,
                    (*e).metadata.is_read_op,
                    depends_on,
                ) {
                    // if the operation is unable to complete; then update the ctail for
                    // already executed operations and return. Only happends for scan ops.
                    self.ctail.fetch_max(i, Ordering::Relaxed);
                    return;
                }
                if (*e).metadata.refcnt.fetch_sub(1, Ordering::Release) == 1 {
                    (*e).operation = None;
                }
            }

            // Increment ltail for each operations, needed for scan
            // operations as the rubberband is ltail sensitive.
            self.ltails[&(idx.0 - 1)].fetch_add(1, Ordering::Relaxed);

            // Looks like we're going to wrap around now; flip this replica's local mask.
            if self.index(i) == self.slog.len() - 1 {
                self.lmasks[&(idx.0 - 1)].set(!self.lmasks[&(idx.0 - 1)].get());
                //trace!("idx: {} lmask: {}", idx, self.lmasks[idx - 1].get());
            }
        }

        // Update the completed tail after we've executed these operations.
        // Also update this replica's local tail.
        self.ctail.fetch_max(gtail, Ordering::Relaxed);
        self.ltails[&(idx.0 - 1)].store(gtail, Ordering::Relaxed);
    }

    /// Advances the head of the log forward. If a replica has stopped making progress,
    /// then this method will never return. Accepts a closure that is passed into exec()
    /// to ensure that this replica does not deadlock GC.
    #[inline(always)]
    fn advance_head<F: FnMut(T, usize, usize, bool, bool, Option<Arc<Vec<usize>>>) -> bool>(
        &self,
        rid: &LogToken,
        mut s: &mut F,
    ) {
        // Keep looping until we can advance the head and create some free space
        // on the log. If one of the replicas has stopped making progress, then
        // this method might never return.
        let mut iteration = 1;
        loop {
            let global_head = self.head.load(Ordering::Relaxed);
            let f = self.tail.load(Ordering::Relaxed);
            let (_min_replica_idx, min_local_tail) = self.find_min_tail();
            // If we cannot advance the head further, then start
            // from the beginning of this loop again. Before doing so, try consuming
            // any new entries on the log to prevent deadlock.
            if min_local_tail == global_head {
                if iteration % WARN_THRESHOLD == 0 {
                    warn!("Spending a long time in `advance_head`, are we starving?");
                }
                iteration += 1;
                self.exec(rid, &mut s);
                continue;
            }

            // There are entries that can be freed up; update the head offset.
            self.head.store(min_local_tail, Ordering::Relaxed);

            // Reset notify replicas after the GC.
            self.metadata.notify_replicas.store(true, Ordering::Relaxed);

            // Make sure that we freed up enough space so that threads waiting for
            // GC in append can make progress. Otherwise, try to make progress again.
            // If we're making progress again, then try consuming entries on the log.
            if f < min_local_tail + self.slog.len() - GC_FROM_HEAD {
                return;
            } else {
                self.exec(rid, &mut s);
            }
        }
    }

    /// The application calls this function to update the callback function.
    /// The application does not need to call this function if it knows that all
    /// the replicas are active for this log and no replica will lag behind.
    ///
    /// # Example
    ///
    /// ```
    /// use node_replication::cnr::Log;
    /// use node_replication::cnr::LogMetaData;
    /// use core::sync::atomic::AtomicBool;
    ///
    /// // Operation type that will go onto the log.
    /// #[derive(Clone)]
    /// enum Operation {
    ///     Read,
    ///     Write(u64),
    ///     Invalid,
    /// }
    ///
    /// // Creates a 1 Mega Byte sized log.
    /// let mut l = Log::<Operation>::new_with_bytes(1 * 1024 * 1024, LogMetaData::new(1));
    ///
    /// // Update the callback function for the log.
    /// let callback_func = |rid: &[AtomicBool; 16], idx: usize| {
    ///     // Take action on log `idx` and replicas in `rid`.
    /// };
    /// l.update_closure(callback_func)
    /// ```
    pub fn update_closure(
        &mut self,
        gc: impl FnMut(&[AtomicBool; MAX_REPLICAS_PER_LOG], usize) + 'static,
    ) {
        unsafe { *self.metadata.gc.get() = Box::new(gc) };
    }
}

#[cfg(test)]
mod tests {
    // Import std so that we have an allocator for our unit tests.
    extern crate std;

    use super::*;
    use crate::log::{Entry, LogToken};
    use std::sync::Arc;

    // Define operations along with their arguments that go onto the log.
    #[derive(Clone)] // Traits required by the log interface.
    #[derive(Debug, PartialEq)] // Traits required for testing.
    enum Operation {
        Read,
        Write(u64),
        Invalid,
    }

    // Required so that we can unit test Entry.
    impl Default for Operation {
        fn default() -> Operation {
            Operation::Invalid
        }
    }

    // Test that we can default construct entries correctly.
    #[test]
    fn test_entry_create_default() {
        let e = Entry::<Operation, EntryMetaData>::default();
        assert_eq!(e.operation, None);
        assert_eq!(e.replica, 0);
        assert_eq!(e.alivef.load(Ordering::Relaxed), false);
    }

    // Test that our entry_size() method returns the correct size.
    #[test]
    fn test_log_entry_size() {
        assert_eq!(Log::<Operation>::entry_size(), 64);
    }

    // Tests if a small log can be correctly constructed.
    #[test]
    fn test_log_create() {
        let l = Log::<Operation>::new_with_bytes(1024 * 1024, LogMetaData::new(1));
        let n = (1024 * 1024) / Log::<Operation>::entry_size();
        assert_eq!(l.slog.len(), n);
        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 0);
        assert_eq!(l.ctail.load(Ordering::Relaxed), 0);

        for i in 0..MAX_REPLICAS_PER_LOG {
            assert_eq!(l.ltails[&i].load(Ordering::Relaxed), 0);
        }

        for i in 0..MAX_REPLICAS_PER_LOG {
            assert_eq!(l.lmasks[&i].get(), true);
        }
    }

    // Test that we can correctly append an entry into the log.
    #[test]
    fn test_log_append() {
        let l = Log::<Operation>::new_with_metadata(LogMetaData::new(1));
        let tkn = l.register().unwrap();
        let o = [(Operation::Read, 1, false)];
        l.append(&o, &tkn, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });

        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 1);
        let slog = l.slog[0].take();
        assert_eq!(slog.operation, Some(Operation::Read));
        assert_eq!(slog.replica, 1);
    }

    // Test that multiple entries can be appended to the log.
    #[test]
    fn test_log_append_multiple() {
        let l = Log::<Operation>::new_with_metadata(LogMetaData::new(1));
        let tkn = l.register().unwrap();
        let o = [
            (Operation::Read, 1, false),
            (Operation::Write(119), 1, false),
        ];
        l.append(&o, &tkn, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });

        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 2);
    }

    // Tests that we can advance the head of the log to the smallest of all replica-local tails.
    #[test]
    fn test_log_advance_head() {
        let l = Log::<Operation>::new_with_metadata(LogMetaData::new(1));
        let tkn = l.register().unwrap();

        for i in 0..4 {
            l.replica_inventory
                .compare_and_swap(i, false, true, Ordering::Relaxed);
        }

        l.ltails[&0].store(1023, Ordering::Relaxed);
        l.ltails[&1].store(224, Ordering::Relaxed);
        l.ltails[&2].store(4096, Ordering::Relaxed);
        l.ltails[&3].store(799, Ordering::Relaxed);

        l.advance_head(&tkn, &mut |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });
        assert_eq!(l.head.load(Ordering::Relaxed), 224);
    }

    // Tests that the head of the log is advanced when we're close to filling up the entire log.
    #[test]
    fn test_log_append_gc() {
        let l = Log::<Operation>::new_with_metadata(LogMetaData::new(1));
        let tkn = l.register().unwrap();

        let o: [(Operation, usize, bool); 4] = unsafe {
            let mut a: [(Operation, usize, bool); 4] =
                ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, (Operation::Read, 1, false));
            }
            a
        };

        l.tail
            .store(l.slog.len() - GC_FROM_HEAD - 1, Ordering::Relaxed);
        l.ltails[&0].store(1024, Ordering::Relaxed);
        l.append(&o, &tkn, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });

        assert_eq!(l.head.load(Ordering::Relaxed), 1024);
        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.slog.len() - GC_FROM_HEAD + 3
        );
    }

    // Tests that on log wrap around, the local mask stays
    // the same because entries have not been executed yet.
    #[test]
    fn test_log_append_wrap() {
        let l = Log::<Operation>::new_with_metadata(LogMetaData::new(1));
        let tkn = l.register().unwrap();

        let o: [(Operation, usize, bool); 1024] = unsafe {
            let mut a: [(Operation, usize, bool); 1024] =
                ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, (Operation::Read, 1, false));
            }
            a
        };

        for i in 0..5 {
            l.replica_inventory
                .compare_and_swap(i, false, true, Ordering::Relaxed);
        }

        l.head.store(2 * 8192, Ordering::Relaxed);
        l.tail.store(l.slog.len() - 10, Ordering::Relaxed);
        l.append(&o, &tkn, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });

        assert_eq!(l.lmasks[&0].get(), true);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.slog.len() + 1014);
    }

    // Test that we can execute operations appended to the log.
    #[test]
    fn test_log_exec() {
        let l = Log::<Operation>::new_with_metadata(LogMetaData::new(1));
        let tkn = l.register().unwrap();

        let o = [(Operation::Read, 1, false)];
        let mut f = |op: Operation, i: usize, _, _, _, _| -> bool {
            assert_eq!(op, Operation::Read);
            assert_eq!(i, 1);
            true
        };

        l.append(&o, &tkn, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });
        l.exec(&tkn, &mut f);

        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.ctail.load(Ordering::Relaxed)
        );
        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.ltails[&0].load(Ordering::Relaxed)
        );
    }

    // Test that exec() doesn't do anything when the log is empty.
    #[test]
    fn test_log_exec_empty() {
        let l = Log::<Operation>::new_with_metadata(LogMetaData::new(1));
        let tkn = l.register().unwrap();

        let mut f = |_o: Operation, _i: usize, _, _, _, _| -> bool {
            assert!(false);
            true
        };

        l.exec(&tkn, &mut f);
    }

    // Test that exec() doesn't do anything if we're already up-to-date.
    #[test]
    fn test_log_exec_zero() {
        let l = Log::<Operation>::new_with_metadata(LogMetaData::new(1));
        let tkn = l.register().unwrap();

        let o = [(Operation::Read, 1, false)];
        let mut f = |op: Operation, i: usize, _, _, _, _| -> bool {
            assert_eq!(op, Operation::Read);
            assert_eq!(i, 1);
            true
        };
        let mut g = |_op: Operation, _i: usize, _, _, _, _| -> bool {
            assert!(false);
            true
        };

        l.append(&o, &tkn, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });
        l.exec(&tkn, &mut f);
        l.exec(&tkn, &mut g);
    }

    // Test that multiple entries on the log can be executed correctly.
    #[test]
    fn test_log_exec_multiple() {
        let l = Log::<Operation>::new_with_metadata(LogMetaData::new(1));
        let tkn = l.register().unwrap();

        let o = [
            (Operation::Read, 1, false),
            (Operation::Write(119), 1, false),
        ];
        let mut s = 0;
        let mut f = |op: Operation, _i: usize, _, _, _, _| -> bool {
            match op {
                Operation::Read => s += 121,
                Operation::Write(v) => s += v,
                Operation::Invalid => assert!(false),
            }
            true
        };

        l.append(&o, &tkn, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });
        l.exec(&tkn, &mut f);
        assert_eq!(s, 240);

        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.ctail.load(Ordering::Relaxed)
        );
        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.ltails[&0].load(Ordering::Relaxed)
        );
    }

    // Test that the replica local mask is updated correctly when executing over
    // a wrapped around log.
    #[test]
    fn test_log_exec_wrap() {
        let l = Log::<Operation>::new_with_metadata(LogMetaData::new(1));
        let tkn = l.register().unwrap();

        let o: [(Operation, usize, bool); 1024] = unsafe {
            let mut a: [(Operation, usize, bool); 1024] =
                ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, (Operation::Read, 1, false));
            }
            a
        };
        let mut f = |op: Operation, i: usize, _, _, _, _| -> bool {
            assert_eq!(op, Operation::Read);
            assert_eq!(i, 1);
            true
        };

        l.append(&o, &tkn, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        }); // Required for GC to work correctly.
        l.head.store(2 * 8192, Ordering::SeqCst);
        l.tail.store(l.slog.len() - 10, Ordering::SeqCst);
        l.append(&o, &tkn, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });

        l.ltails[&0].store(l.slog.len() - 10, Ordering::SeqCst);
        l.exec(&tkn, &mut f);

        assert_eq!(l.lmasks[&0].get(), false);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.slog.len() + 1014);
    }

    // Tests that exec() panics if the head of the log advances beyond the tail.
    #[test]
    #[should_panic]
    fn test_exec_panic() {
        let l = Log::<Operation>::new_with_metadata(LogMetaData::new(1));
        let tkn = l.register().unwrap();

        let o: [(Operation, usize, bool); 1024] = unsafe {
            let mut a: [(Operation, usize, bool); 1024] =
                ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, (Operation::Read, 1, false));
            }
            a
        };
        let mut f = |_op: Operation, _i: usize, _, _, _, _| -> bool {
            assert!(false);
            true
        };

        l.append(&o, &tkn, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });
        l.head.store(8192, Ordering::SeqCst);

        l.exec(&tkn, &mut f);
    }

    // Tests that operations are cloned when added to the log, and that
    // they are correctly dropped once overwritten.
    #[test]
    fn test_log_change_refcount() {
        let l = Log::<Arc<Operation>>::new_with_metadata(LogMetaData::new(1));
        let o1 = [(Arc::new(Operation::Read), 1, false)];
        let o2 = [(Arc::new(Operation::Read), 1, false)];
        assert_eq!(Arc::strong_count(&o1[0].0), 1);
        assert_eq!(Arc::strong_count(&o2[0].0), 1);
        let tkn = l.register().unwrap();

        l.append(
            &o1[..],
            &tkn,
            |_o: Arc<Operation>, _i: usize, _, _, _, _| -> bool { true },
        );
        assert_eq!(Arc::strong_count(&o1[0].0), 2);
        l.append(
            &o1[..],
            &tkn,
            |_o: Arc<Operation>, _i: usize, _, _, _, _| -> bool { true },
        );
        assert_eq!(Arc::strong_count(&o1[0].0), 3);

        unsafe { l.reset() };

        // Over here, we overwrite entries that were written to by the two
        // previous appends. This decreases the refcount of o1 and increases
        // the refcount of o2.
        l.append(
            &o2[..],
            &tkn,
            |_o: Arc<Operation>, _i: usize, _, _, _, _| -> bool { true },
        );
        assert_eq!(Arc::strong_count(&o1[0].0), 2);
        assert_eq!(Arc::strong_count(&o2[0].0), 2);
        l.append(
            &o2[..],
            &tkn,
            |_o: Arc<Operation>, _i: usize, _, _, _, _| -> bool { true },
        );
        assert_eq!(Arc::strong_count(&o1[0].0), 1);
        assert_eq!(Arc::strong_count(&o2[0].0), 3);
    }

    // Tests that operations are cloned when added to the log, and that
    // they are correctly dropped once overwritten after the GC.
    #[test]
    fn test_log_refcount_change_with_gc() {
        let entry_size = 64;
        let total_entries = 16384;

        assert_eq!(Log::<Arc<Operation>>::entry_size(), entry_size);
        let _size: usize = total_entries * entry_size;
        let l = Log::<Arc<Operation>>::new_with_entries(16384, LogMetaData::new(1));
        assert_eq!(l.slog.len(), total_entries);

        // Intentionally not using `register()`, (will fail the test due to GC).
        // let tkn = LogToken(1);
        // l.replica_inventory.set_bit(1,true);

        let tkn = l.register().unwrap();

        let o1 = [(Arc::new(Operation::Read), 1, false)];
        let o2 = [(Arc::new(Operation::Read), 1, false)];
        assert_eq!(Arc::strong_count(&o1[0].0), 1);
        assert_eq!(Arc::strong_count(&o2[0].0), 1);

        for i in 1..(total_entries + 1) {
            l.append(
                &o1[..],
                &tkn,
                |_o: Arc<Operation>, _i: usize, _, _, _, _| -> bool { true },
            );
            assert_eq!(Arc::strong_count(&o1[0].0), i + 1);
        }
        assert_eq!(Arc::strong_count(&o1[0].0), total_entries + 1);

        for i in 1..(total_entries + 1) {
            l.append(
                &o2[..],
                &tkn,
                |_o: Arc<Operation>, _i: usize, _, _, _, _| -> bool { true },
            );
            assert_eq!(Arc::strong_count(&o1[0].0), (total_entries + 1) - i);
            assert_eq!(Arc::strong_count(&o2[0].0), i + 1);
        }
        assert_eq!(Arc::strong_count(&o1[0].0), 1);
        assert_eq!(Arc::strong_count(&o2[0].0), total_entries + 1);
    }

    // Tests that is_replica_synced_for_read() works correctly; it returns
    // false when a replica is not synced up and true when it is.
    #[test]
    fn test_replica_synced_for_read() {
        let l = Log::<Operation>::new_with_metadata(LogMetaData::new(1));

        let one = l.register().unwrap();
        let two = l.register().unwrap();

        assert_eq!(one, LogToken(1));
        assert_eq!(two, LogToken(2));

        let o = [(Operation::Read, 1, false)];
        let mut f = |op: Operation, i: usize, _, _, _, _| -> bool {
            assert_eq!(op, Operation::Read);
            assert_eq!(i, 1);
            true
        };

        l.append(&o, &one, |_o: Operation, _i: usize, _, _, _, _| -> bool {
            true
        });
        l.exec(&one, &mut f);
        assert_eq!(l.is_replica_synced_for_reads(&one, l.get_ctail()), true);
        assert_eq!(l.is_replica_synced_for_reads(&two, l.get_ctail()), false);

        l.exec(&two, &mut f);
        assert_eq!(l.is_replica_synced_for_reads(&two, l.get_ctail()), true);
    }
}
