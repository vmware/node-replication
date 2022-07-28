// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! CNR specific Context.

use alloc::vec::Vec;
use core::sync::atomic::Ordering;

pub use crate::context::MAX_PENDING_OPS;

/// Pending operation meta-data for CNR.
///
/// Tuple contains: `hash`, `is_scan`, `is_read_only`
pub(crate) type PendingMetaData = (usize, bool, bool);

/// The CNR per-thread context.
///
/// It stores every outstanding request (`T`) and response (`R`) pair with some
/// additional [`PendingMetaData`].
pub(crate) type Context<T, R> = crate::context::Context<T, R, PendingMetaData>;

impl<T, R> Context<T, R>
where
    T: Sized + Clone,
    R: Sized + Clone,
{
    /// Adds any pending operations on this context to a passed in buffer.
    /// Returns the the number of such operations that were added in.
    #[inline(always)]
    pub(crate) fn ops(
        &self,
        buffer: &mut Vec<(T, usize, bool)>,
        scan_buffer: &mut Vec<(T, usize, bool)>,
        hash: usize,
    ) -> usize {
        let h = self.comb.load(Ordering::Relaxed);
        let t = self.tail.load(Ordering::Relaxed);

        // No operations on this thread; return to the caller indicating so.
        if h == t {
            return 0;
        };

        if h > t {
            panic!("Combiner Head of thread-local batch has advanced beyond tail!");
        }

        // Iterate from `comb` to `tail`, adding pending operations into the
        // passed in buffer. Return the number of operations that were added.
        let mut n = 0;
        for i in h..t {
            // By construction, we know that everything between `comb` and
            // `tail` is a valid operation ready for flat combining. Hence,
            // calling unwrap() here on the operation is safe.
            let op = self.batch[self.index(i)].op.take().unwrap();
            let (entry_hash, is_scan, is_read_only) = self.batch[self.index(i)].meta.get();
            let hash_match = entry_hash == hash;

            if hash_match {
                if is_scan {
                    scan_buffer.push((op, self.idx, is_read_only))
                } else {
                    buffer.push((op, self.idx, is_read_only))
                }

                n += 1;
            }
        }

        n
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use std::vec;

    // context.
    #[test]
    fn test_context_ops() {
        let c = Context::<usize, usize>::default();
        let mut o = vec![];
        let mut scan = vec![];

        for idx in 0..MAX_PENDING_OPS / 2 {
            assert!(c.enqueue(idx * idx, (1, false, false)))
        }

        assert_eq!(c.ops(&mut o, &mut scan, 1), MAX_PENDING_OPS / 2);
        assert_eq!(o.len(), MAX_PENDING_OPS / 2);
        assert_eq!(scan.len(), 0);
        assert_eq!(c.tail.load(Ordering::Relaxed), MAX_PENDING_OPS / 2);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);

        for idx in 0..MAX_PENDING_OPS / 2 {
            assert_eq!(o[idx].0, idx * idx)
        }
    }

    // Tests whether scan ops() can successfully retrieve operations enqueued on this context.
    #[test]
    fn test_context_ops_scan() {
        let c = Context::<usize, usize>::default();
        let mut o = vec![];
        let mut scan = vec![];

        for idx in 0..MAX_PENDING_OPS / 2 {
            assert!(c.enqueue(idx * idx, (1, true, false)))
        }

        assert_eq!(c.ops(&mut o, &mut scan, 1), MAX_PENDING_OPS / 2);
        assert_eq!(o.len(), 0);
        assert_eq!(scan.len(), MAX_PENDING_OPS / 2);
        assert_eq!(c.tail.load(Ordering::Relaxed), MAX_PENDING_OPS / 2);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);

        for idx in 0..MAX_PENDING_OPS / 2 {
            assert_eq!(scan[idx].0, idx * idx)
        }
    }

    // Tests whether ops() returns nothing when we don't have any pending operations.
    #[test]
    fn test_context_ops_empty() {
        let c = Context::<usize, usize>::default();
        let mut o = vec![];
        let mut scan = vec![];

        c.tail.store(8, Ordering::Relaxed);
        c.comb.store(8, Ordering::Relaxed);

        assert_eq!(c.ops(&mut o, &mut scan, 0), 0);
        assert_eq!(o.len(), 0);
        assert_eq!(c.tail.load(Ordering::Relaxed), 8);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 8);
    }

    // Tests whether ops() panics if the combiner head advances beyond the tail.
    #[test]
    #[should_panic]
    fn test_context_ops_panic() {
        let c = Context::<usize, usize>::default();
        let mut o = vec![];
        let mut scan = vec![];

        c.tail.store(6, Ordering::Relaxed);
        c.comb.store(9, Ordering::Relaxed);

        assert_eq!(c.ops(&mut o, &mut scan, 0), 0);
    }
}
