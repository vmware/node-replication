// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Contains the shared Log, in a nutshell it's a multi-producer, multi-consumer
//! circular-buffer.

use core::sync::atomic::Ordering;

pub use crate::log::LogToken;
pub use crate::log::DEFAULT_LOG_BYTES;
pub use crate::log::MAX_REPLICAS_PER_LOG;

pub use crate::log::GC_FROM_HEAD;

pub use crate::log::WARN_THRESHOLD;

pub type Log<T> = crate::log::Log<T, (), ()>;

impl<T> Log<T>
where
    T: Sized + Clone,
{
    /// Inserts a slice of operations into the log.
    ///
    /// # Example
    ///
    /// ```
    /// use node_replication::log::Log;
    ///
    /// // Operation type that will go onto the log.
    /// #[derive(Clone)]
    /// enum Operation {
    ///     Read,
    ///     Write(u64),
    /// }
    ///
    /// let l = Log::<Operation, (), ()>::new_with_bytes(1 * 1024 * 1024, ());
    /// let idx = l.register().expect("Failed to register with the Log.");
    ///
    /// // The set of operations we would like to append. The order will
    /// // be preserved by the interface.
    /// let ops = [Operation::Write(100), Operation::Read];
    ///
    /// // `append()` might have to garbage collect the log. When doing so,
    /// // it might encounter operations added in by another replica/thread.
    /// // This closure allows us to consume those operations. `mine` identifies
    /// // if it was 'our` replica that added in those operations.
    /// let f = |op: Operation, mine: bool| {
    ///     match(op) {
    ///         Operation::Read => println!("Read by me? {}", mine),
    ///         Operation::Write(x) => println!("Write({}) by me? {}", x, mine),
    ///     }
    /// };
    ///
    /// // Append the operations. These operations will be marked with `idx`,
    /// // and will be linearized at the tail of the log.
    /// l.append(&ops, &idx, f);
    /// ```
    ///
    /// If there isn't enough space to perform the append, this method busy for
    /// a while waits to see if it can advance the head. Accepts a replica
    /// `idx`; all appended operations/entries will be marked with this
    /// replica-identifier. Also accepts a closure `s`; when waiting for GC,
    /// this closure is passed into exec() to ensure that this replica does'nt
    /// cause a deadlock.
    ///
    /// # Returns
    /// This will return Ok if all `ops` were successfully appended to the log.
    /// It might return `Ok(Some(usize))` if all operations were added, but we
    /// couldn't run GC after adding the ops (`usize` indicating which replica
    /// we're waiting for). This will return `Err(usize)` if the append failed
    /// because we waited too long for the replica indicated by the `usize`.
    ///
    /// # Note
    /// Documentation for this function is hidden since `append` is currently
    /// not intended as a public interface. It is marked as public due to being
    /// used by the benchmarking code.
    #[inline(always)]
    #[doc(hidden)]
    pub fn append<F: FnMut(T, bool)>(
        &self,
        ops: &[T],
        idx: &LogToken,
        mut s: F,
    ) -> Result<Option<usize>, usize> {
        let nops = ops.len();
        let mut iteration = 1;
        let mut waitgc = 1;

        // Keep trying to reserve entries and add operations to the log until
        // we succeed in doing so.
        loop {
            if iteration % WARN_THRESHOLD == 0 {
                let (min_replica_idx, _min_local_tail) = self.find_min_tail();
                warn!(
                    "append(ops.len()={}, {}) takes too many iterations ({}) to complete (waiting for {})...",
                    ops.len(),
                    idx.0,
                    iteration,
                    min_replica_idx,
                );
                let r = self.next.load(Ordering::Relaxed);
                let (mut min_replica_idx, mut min_local_tail) =
                    (0, self.ltails[0].load(Ordering::Relaxed));

                // Find the smallest local tail across all replicas.
                for idx in 1..r {
                    let cur_local_tail = self.ltails[idx - 1].load(Ordering::Relaxed);
                    info!(
                        "Replica {} cur_local_tail {} head {}.",
                        idx - 1,
                        cur_local_tail,
                        self.head.load(Ordering::Relaxed)
                    );

                    if cur_local_tail < min_local_tail {
                        min_local_tail = cur_local_tail;
                        min_replica_idx = idx - 1;
                    }
                }

                return Err(min_replica_idx);
            }
            iteration += 1;

            let tail = self.tail.load(Ordering::Relaxed);
            let head = self.head.load(Ordering::Relaxed);

            // If there are fewer than `GC_FROM_HEAD` entries on the log, then just
            // try again. The replica that reserved entry (h + self.slog.len() - GC_FROM_HEAD)
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
                    let (min_replica_idx, _min_local_tail) = self.find_min_tail();
                    return Err(min_replica_idx);
                }
                waitgc += 1;
                self.exec(idx, &mut s);
                self.advance_head(idx, &mut s)?;

                #[cfg(loom)]
                loom::thread::yield_now();
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
                let e = self.slog[self.index(tail + i)].as_ptr();
                let mut m = self.lmasks[idx.0 - 1].get();

                // This entry was just reserved so it should be dead (!= m). However, if
                // the log has wrapped around, then the alive mask has flipped. In this
                // case, we flip the mask we were originally going to write into the
                // allocated entry. We cannot flip lmasks[idx - 1] because this replica
                // might still need to execute a few entries before the wrap around.
                if unsafe { (*e).alivef.load(Ordering::Relaxed) == m } {
                    m = !m;
                }

                unsafe { (*e).operation = Some(op.clone()) };
                unsafe { (*e).replica = idx.0 };
                unsafe { (*e).alivef.store(m, Ordering::Release) };
            }

            // If needed, advance the head of the log forward to make room on the log.
            return if advance {
                // If `advance_head()` fails to advance it will return the
                // replica we're waiting for as an error. If a client calls
                // `combine()` this isn't considered an error as we have
                // succesfully applied the operations. But, we should still make
                // sure to eventually `unstuck` the replica we waited for, so
                // we transform the error to an Ok(Option<usize>) to convey this
                // information to clients.
                match self.advance_head(idx, &mut s) {
                    Ok(_) => Ok(None),
                    Err(min_replica_idx) => Ok(Some(min_replica_idx)),
                }
            } else {
                Ok(None)
            };
        }
    }

    /// Executes a passed in closure (`d`) on all operations starting from a
    /// replica's local tail on the shared log. The replica is identified
    /// through an `idx` passed in as an argument.
    ///
    /// The passed in closure is expected to accept two arguments: The operation
    /// from the shared log to be executed and a bool that's true only if the
    /// operation was issued originally on this replica.
    ///
    /// # Example
    ///
    /// (Example ignored for lack of access to `exec` in doctests.)
    ///
    /// ```ignore
    /// use node_replication::nr::Log;
    ///
    /// // Operation type that will go onto the log.
    /// #[derive(Clone)]
    /// enum Operation {
    ///     Read,
    ///     Write(u64),
    /// }
    ///
    /// let l = Log::<Operation>::new_with_bytes(1 * 1024 * 1024, ());
    /// let idx = l.register().expect("Failed to register with the Log.");
    /// let ops = [Operation::Write(100), Operation::Read];
    ///
    /// let f = |op: Operation, mine: bool| {
    ///     match(op) {
    ///         Operation::Read => println!("Read by {} me?", mine),
    ///         Operation::Write(x) => println!("Write({}) by me? {}", x, mine),
    ///     }
    /// };
    /// l.append(&ops, &idx, f);
    ///
    /// // This closure is executed on every operation appended to the
    /// // since the last call to `exec()` by this replica/thread.
    /// let mut d = 0;
    /// let mut g = |op: Operation, mine: bool| {
    ///     match(op) {
    ///         // The write happened before the read.
    ///         Operation::Read => assert_eq!(100, d),
    ///         Operation::Write(x) => d += 100,
    ///     }
    /// };
    /// l.exec(&idx, &mut g);
    /// ```
    #[inline(always)]
    pub(crate) fn exec<F: FnMut(T, bool)>(&self, idx: &LogToken, d: &mut F) {
        // Load the logical log offset from which we must execute operations.
        let ltail = self.ltails[idx.0 - 1].load(Ordering::Relaxed);

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

        // Execute all operations from the passed in offset to the shared log's tail.
        // Check if the entry is live first; we could have a replica that has reserved
        // entries, but not filled them into the log yet.
        for i in ltail..gtail {
            let mut iteration = 1;
            let e = self.slog[self.index(i)].as_ptr();

            while unsafe { (*e).alivef.load(Ordering::Acquire) != self.lmasks[idx.0 - 1].get() } {
                if iteration % WARN_THRESHOLD == 0 {
                    warn!(
                        "alivef not being set for self.index(i={}) = {} (self.lmasks[{}] is {})...",
                        i,
                        self.index(i),
                        idx.0 - 1,
                        self.lmasks[idx.0 - 1].get()
                    );
                }
                iteration += 1;

                #[cfg(loom)]
                loom::thread::yield_now();
            }

            unsafe {
                d(
                    (*e).operation.as_ref().unwrap().clone(),
                    (*e).replica == idx.0,
                )
            };

            // Looks like we're going to wrap around now; flip this replica's local mask.
            if self.index(i) == self.slog.len() - 1 {
                self.lmasks[idx.0 - 1].set(!self.lmasks[idx.0 - 1].get());
                //trace!("idx: {} lmask: {}", idx, self.lmasks[idx - 1].get());
            }
        }

        // Update the completed tail after we've executed these operations. Also update
        // this replica's local tail.
        self.ctail.fetch_max(gtail, Ordering::Relaxed);
        self.ltails[idx.0 - 1].store(gtail, Ordering::Relaxed);
    }

    /// Advances the head of the log forward. If a replica has stopped making
    /// progress, then this method will never return. Accepts a closure that is
    /// passed into exec() to ensure that this replica does not deadlock GC.
    #[inline(always)]
    fn advance_head<F: FnMut(T, bool)>(&self, rid: &LogToken, mut s: &mut F) -> Result<(), usize> {
        // Keep looping until we can advance the head and create some free space
        // on the log. If one of the replicas has stopped making progress, then
        // this method might never return.
        let mut iteration = 1;
        loop {
            let global_head = self.head.load(Ordering::Relaxed);
            let f = self.tail.load(Ordering::Relaxed);
            let (min_replica_idx, min_local_tail) = self.find_min_tail();

            // If we cannot advance the head further, then start
            // from the beginning of this loop again. Before doing so, try consuming
            // any new entries on the log to prevent deadlock.
            if min_local_tail == global_head {
                if iteration % WARN_THRESHOLD == 0 {
                    warn!("Spending a long time in `advance_head`, are we starving (min_replica_idx = {})?", min_replica_idx);
                    return Err(min_replica_idx);
                }
                iteration += 1;
                self.exec(rid, &mut s);

                #[cfg(loom)]
                loom::thread::yield_now();
                continue;
            }

            // There are entries that can be freed up; update the head offset.
            self.head.store(min_local_tail, Ordering::Relaxed);

            // Make sure that we freed up enough space so that threads waiting for
            // GC in append can make progress. Otherwise, try to make progress again.
            // If we're making progress again, then try consuming entries on the log.
            if f < min_local_tail + self.slog.len() - GC_FROM_HEAD {
                return Ok(());
            } else {
                self.exec(rid, &mut s);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Import std so that we have an allocator for our unit tests.
    extern crate std;

    use super::*;
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

    // Test that we can correctly append an entry into the log.
    #[test]
    fn test_log_append() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o = [Operation::Read];
        assert!(l.append(&o, &lt, |_o: Operation, _mine: bool| {}).is_ok());

        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 1);
        let slog = l.slog[0].take();
        assert_eq!(slog.operation, Some(Operation::Read));
        assert_eq!(slog.replica, 1);
    }

    // Test that multiple entries can be appended to the log.
    #[test]
    fn test_log_append_multiple() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o = [Operation::Read, Operation::Write(119)];
        assert!(l.append(&o, &lt, |_o: Operation, _mine: bool| {}).is_ok());

        assert_eq!(l.head.load(Ordering::Relaxed), 0);
        assert_eq!(l.tail.load(Ordering::Relaxed), 2);
    }

    // Tests that we can advance the head of the log to the smallest of all replica-local tails.
    #[test]
    fn test_log_advance_head() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        l.next.store(5, Ordering::Relaxed);
        l.ltails[0].store(1023, Ordering::Relaxed);
        l.ltails[1].store(224, Ordering::Relaxed);
        l.ltails[2].store(4096, Ordering::Relaxed);
        l.ltails[3].store(799, Ordering::Relaxed);

        assert!(l
            .advance_head(&lt, &mut |_o: Operation, _mine: bool| {})
            .is_ok());
        assert_eq!(l.head.load(Ordering::Relaxed), 224);
    }

    // Tests that the head of the log is advanced when we're close to filling up the entire log.
    #[test]
    fn test_log_append_gc() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o: [Operation; 4] = unsafe {
            let mut a: [Operation; 4] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, Operation::Read);
            }
            a
        };

        l.next.store(2, Ordering::Relaxed);
        l.tail
            .store(l.slog.len() - GC_FROM_HEAD - 1, Ordering::Relaxed);
        l.ltails[0].store(1024, Ordering::Relaxed);
        assert!(l.append(&o, &lt, |_o: Operation, _mine: bool| {}).is_ok());

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
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o: [Operation; 1024] = unsafe {
            let mut a: [Operation; 1024] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, Operation::Read);
            }
            a
        };

        l.next.store(2, Ordering::Relaxed);
        l.head.store(2 * 8192, Ordering::Relaxed);
        l.tail.store(l.slog.len() - 10, Ordering::Relaxed);
        assert!(l.append(&o, &lt, |_o: Operation, _mine: bool| {}).is_ok());

        assert_eq!(l.lmasks[0].get(), true);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.slog.len() + 1014);
    }

    // Test that we can execute operations appended to the log.
    #[test]
    fn test_log_exec() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o = [Operation::Read];
        let mut f = |op: Operation, mine: bool| {
            assert_eq!(op, Operation::Read);
            assert!(mine);
        };

        assert!(l.append(&o, &lt, |_o: Operation, _mine: bool| {}).is_ok());
        l.exec(&lt, &mut f);

        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.ctail.load(Ordering::Relaxed)
        );
        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.ltails[0].load(Ordering::Relaxed)
        );
    }

    // Test that exec() doesn't do anything when the log is empty.
    #[test]
    fn test_log_exec_empty() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let mut f = |_o: Operation, _mine| {
            assert!(false);
        };

        l.exec(&lt, &mut f);
    }

    // Test that exec() doesn't do anything if we're already up-to-date.
    #[test]
    fn test_log_exec_zero() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o = [Operation::Read];
        let mut f = |op: Operation, mine: bool| {
            assert_eq!(op, Operation::Read);
            assert!(mine);
        };
        let mut g = |_op: Operation, _mine: bool| {
            assert!(false);
        };

        assert!(l.append(&o, &lt, |_o: Operation, _mine| {}).is_ok());
        l.exec(&lt, &mut f);
        l.exec(&lt, &mut g);
    }

    // Test that multiple entries on the log can be executed correctly.
    #[test]
    fn test_log_exec_multiple() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o = [Operation::Read, Operation::Write(119)];
        let mut s = 0;
        let mut f = |op: Operation, _mine| match op {
            Operation::Read => s += 121,
            Operation::Write(v) => s += v,
            Operation::Invalid => assert!(false),
        };

        assert!(l.append(&o, &lt, |_o: Operation, _mine: bool| {}).is_ok());
        l.exec(&lt, &mut f);
        assert_eq!(s, 240);

        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.ctail.load(Ordering::Relaxed)
        );
        assert_eq!(
            l.tail.load(Ordering::Relaxed),
            l.ltails[0].load(Ordering::Relaxed)
        );
    }

    // Test that the replica local mask is updated correctly when executing over
    // a wrapped around log.
    #[test]
    fn test_log_exec_wrap() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o: [Operation; 1024] = unsafe {
            let mut a: [Operation; 1024] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, Operation::Read);
            }
            a
        };
        let mut f = |op: Operation, mine: bool| {
            assert_eq!(op, Operation::Read);
            assert!(mine);
        };

        assert!(l.append(&o, &lt, |_o: Operation, _mine| {}).is_ok()); // Required for GC to work correctly.
        l.next.store(2, Ordering::SeqCst);
        l.head.store(2 * 8192, Ordering::SeqCst);
        l.tail.store(l.slog.len() - 10, Ordering::SeqCst);
        assert!(l.append(&o, &lt, |_o: Operation, _mine| {}).is_ok());

        l.ltails[0].store(l.slog.len() - 10, Ordering::SeqCst);
        l.exec(&lt, &mut f);

        assert_eq!(l.lmasks[0].get(), false);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.slog.len() + 1014);
    }

    // Tests that exec() panics if the head of the log advances beyond the tail.
    #[test]
    #[should_panic]
    fn test_exec_panic() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o: [Operation; 1024] = unsafe {
            let mut a: [Operation; 1024] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, Operation::Read);
            }
            a
        };
        let mut f = |_op: Operation, _mine| {
            assert!(false);
        };

        assert!(l.append(&o, &lt, |_o: Operation, _mine| {}).is_ok());
        l.head.store(8192, Ordering::SeqCst);

        l.exec(&lt, &mut f);
    }

    // Tests that operations are cloned when added to the log, and that
    // they are correctly dropped once overwritten.
    #[test]
    fn test_log_change_refcount() {
        let l = Log::<Arc<Operation>>::default();
        let lt = l.register().unwrap();

        let o1 = [Arc::new(Operation::Read)];
        let o2 = [Arc::new(Operation::Read)];
        assert_eq!(Arc::strong_count(&o1[0]), 1);
        assert_eq!(Arc::strong_count(&o2[0]), 1);

        assert!(l
            .append(&o1[..], &lt, |_o: Arc<Operation>, _mine| {})
            .is_ok());
        assert_eq!(Arc::strong_count(&o1[0]), 2);
        assert!(l
            .append(&o1[..], &lt, |_o: Arc<Operation>, _mine| {})
            .is_ok());
        assert_eq!(Arc::strong_count(&o1[0]), 3);

        unsafe { l.reset() };

        // Over here, we overwrite entries that were written to by the two
        // previous appends. This decreases the refcount of o1 and increases
        // the refcount of o2.
        assert!(l
            .append(&o2[..], &lt, |_o: Arc<Operation>, _mine| {})
            .is_ok());
        assert_eq!(Arc::strong_count(&o1[0]), 2);
        assert_eq!(Arc::strong_count(&o2[0]), 2);
        assert!(l
            .append(&o2[..], &lt, |_o: Arc<Operation>, _mine| {})
            .is_ok());
        assert_eq!(Arc::strong_count(&o1[0]), 1);
        assert_eq!(Arc::strong_count(&o2[0]), 3);
    }

    // Tests that operations are cloned when added to the log, and that
    // they are correctly dropped once overwritten after the GC.
    #[test]
    fn test_log_refcount_change_with_gc() {
        let entry_size = 64;
        let total_entries = 16384;

        assert_eq!(Log::<Arc<Operation>>::entry_size(), entry_size);
        let size: usize = total_entries * entry_size;
        let l = Log::<Arc<Operation>>::new_with_bytes(size, ());
        let lt = l.register().unwrap();
        let o1 = [Arc::new(Operation::Read)];
        let o2 = [Arc::new(Operation::Read)];
        assert_eq!(Arc::strong_count(&o1[0]), 1);
        assert_eq!(Arc::strong_count(&o2[0]), 1);

        for i in 1..(total_entries + 1) {
            assert!(l
                .append(&o1[..], &lt, |_o: Arc<Operation>, _mine| {})
                .is_ok());
            assert_eq!(Arc::strong_count(&o1[0]), i + 1);
        }
        assert_eq!(Arc::strong_count(&o1[0]), total_entries + 1);

        for i in 1..(total_entries + 1) {
            assert!(l
                .append(&o2[..], &lt, |_o: Arc<Operation>, _mine| {})
                .is_ok());
            assert_eq!(Arc::strong_count(&o1[0]), (total_entries + 1) - i);
            assert_eq!(Arc::strong_count(&o2[0]), i + 1);
        }
        assert_eq!(Arc::strong_count(&o1[0]), 1);
        assert_eq!(Arc::strong_count(&o2[0]), total_entries + 1);
    }

    // Tests that is_replica_synced_for_read() works correctly; it returns
    // false when a replica is not synced up and true when it is.
    #[test]
    fn test_replica_synced_for_read() {
        let l = Log::<Operation>::default();
        let one = l.register().unwrap();
        let two = l.register().unwrap();

        assert_eq!(one, LogToken(1));
        assert_eq!(two, LogToken(2));

        let o = [Operation::Read];
        let mut f = |op: Operation, mine: bool| {
            assert_eq!(op, Operation::Read);
            assert!(mine);
        };

        assert!(l.append(&o, &one, |_o: Operation, _mine| {}).is_ok());
        l.exec(&one, &mut f);
        assert_eq!(l.is_replica_synced_for_reads(&one, l.get_ctail()), true);
        assert_eq!(l.is_replica_synced_for_reads(&two, l.get_ctail()), false);

        let mut f = |op: Operation, mine: bool| {
            assert_eq!(op, Operation::Read);
            assert!(!mine);
        };
        l.exec(&two, &mut f);
        assert_eq!(l.is_replica_synced_for_reads(&two, l.get_ctail()), true);
    }
}
