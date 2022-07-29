// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! CNR specific Context.

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

#[cfg(test)]
mod test {
    use super::*;
    use core::sync::atomic::Ordering;

    // test context for retrieving non-scan ops.
    #[test]
    fn test_context_ops() {
        let c = Context::<usize, usize>::default();
        for idx in 0..MAX_PENDING_OPS / 2 {
            assert!(c.enqueue(idx * idx, (1, false, false)))
        }

        let ctxt_iter = c.iter();
        assert_eq!(ctxt_iter.len(), MAX_PENDING_OPS / 2);

        for (idx, (op, (hash, is_scan, is_read_only))) in ctxt_iter.enumerate() {
            assert_eq!(op, idx * idx);
            assert_eq!(hash, 1);
            assert!(!is_read_only);
            assert!(!is_scan);
        }

        assert_eq!(c.tail.load(Ordering::Relaxed), MAX_PENDING_OPS / 2);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);
    }

    // Tests whether scans can successfully be retrieved from operations
    // enqueued on this context.
    #[test]
    fn test_context_ops_scan() {
        let c = Context::<usize, usize>::default();
        for idx in 0..MAX_PENDING_OPS / 2 {
            assert!(c.enqueue(idx * idx, (1, true, false)))
        }

        let ctxt_iter = c.iter();
        assert_eq!(ctxt_iter.len(), MAX_PENDING_OPS / 2);
        for (idx, (op, (hash, is_scan, is_read_only))) in ctxt_iter.enumerate() {
            assert_eq!(op, idx * idx);
            assert_eq!(hash, 1);
            assert!(!is_read_only);
            assert!(is_scan);
        }

        assert_eq!(c.tail.load(Ordering::Relaxed), MAX_PENDING_OPS / 2);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);
    }

    // Tests whether iterator is empty nothing when we don't have any pending operations.
    #[test]
    fn test_context_ops_empty() {
        let c = Context::<usize, usize>::default();

        c.tail.store(8, Ordering::Relaxed);
        c.comb.store(8, Ordering::Relaxed);

        let ctxt_iter = c.iter();
        assert_eq!(ctxt_iter.len(), 0);

        assert_eq!(c.tail.load(Ordering::Relaxed), 8);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 8);
    }

    // Tests whether iter() panics if the combiner head advances beyond the
    // tail.
    #[test]
    #[should_panic]
    fn test_context_ops_panic() {
        let c = Context::<usize, usize>::default();

        c.tail.store(6, Ordering::Relaxed);
        c.comb.store(9, Ordering::Relaxed);

        let _ctxt_iter = c.iter(); // panics
    }
}
