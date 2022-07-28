// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! NR specific Context.

use alloc::vec::Vec;
use core::sync::atomic::Ordering;

pub use crate::context::MAX_PENDING_OPS;

/// The NR per-thread context.
///
/// It stores every outstanding request (`T`) and response (`R`) pair.
pub(crate) type Context<T, R> = crate::context::Context<T, R, ()>;

impl<T, R> Context<T, R>
where
    T: Sized + Clone,
    R: Sized + Clone,
{
    /// Adds any pending operations on this context to a passed in buffer.
    /// Returns the the number of such operations that were added in.
    #[inline(always)]
    pub(crate) fn ops(&self, buffer: &mut Vec<T>) -> usize {
        let h = self.comb.load(Ordering::Relaxed);
        let t = self.tail.load(Ordering::Relaxed);

        // No operations on this thread; return to the caller indicating so.
        if h == t {
            return 0;
        }

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
            let op = self.batch[self.index(i)].op.get();
            unsafe { buffer.push((&mut *op).clone().unwrap()) };
            n += 1;
        }

        assert_eq!(n, t - h, "just return t-h?");
        n
    }
}
