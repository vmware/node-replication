// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use alloc::vec::Vec;
use core::cell::Cell;
use core::default::Default;
use core::sync::atomic::Ordering;

use crossbeam_utils::CachePadded;

pub use crate::context::MAX_PENDING_OPS;

pub(crate) type Context<T, R> = crate::context::Context<T, R, ()>;

impl<T, R> Context<T, R>
where
    T: Sized + Clone,
    R: Sized + Clone,
{
    /// Enqueues an operation onto this context's batch of pending operations.
    ///
    /// Returns true if the operation was successfully enqueued. False
    /// otherwise.
    #[inline(always)]
    pub(crate) fn enqueue(&self, op: T) -> bool {
        let t = self.tail.load(Ordering::Relaxed);
        let h = self.head.load(Ordering::Relaxed);

        // Check if we have space in the batch to hold this operation. If we
        // don't, then return false to the caller thread.
        if t - h == MAX_PENDING_OPS {
            return false;
        }

        // Add in the operation to the batch. Once added, update the tail so
        // that the combiner sees this operation. Relying on TSO here to make
        // sure that the tail is updated only after the operation has been
        // written in.
        let e = self.batch[self.index(t)].op.as_ptr();
        unsafe { (*e).0 = Some(op) };

        self.tail.store(t + 1, Ordering::Relaxed);
        true
    }

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
            let e = self.batch[self.index(i)].op.as_ptr();
            let op = unsafe { (*e).0.as_ref().unwrap().clone() };
            buffer.push(op);
            n += 1;
        }

        n
    }
}
