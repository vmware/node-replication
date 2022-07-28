// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Context where threads "buffer" operations until they are completed.
//!
//! This allows the combiner to find and flat-combine operations.

use core::cell::{Cell, UnsafeCell};
use core::default::Default;
use core::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;
use static_assertions::const_assert;

/// The maximum number of operations that can be batched inside this context.
#[cfg(not(loom))]
pub const MAX_PENDING_OPS: usize = 32;
#[cfg(loom)]
pub(crate) const MAX_PENDING_OPS: usize = 1;
// This constant must be a power of two for `index()` to work.
const_assert!(MAX_PENDING_OPS.is_power_of_two());

/// Data for a pending operation.
///
/// It is a combination of the actual operation (`T`), the corresponding
/// expected result (`R`), along with potential meta-data (`M`) to keep track of
/// other things.
pub struct PendingOperation<T, R, M: Default> {
    pub(crate) op: UnsafeCell<Option<T>>,
    pub(crate) resp: Cell<Option<R>>,
    pub(crate) meta: UnsafeCell<M>,
}

impl<T, R, M> Default for PendingOperation<T, R, M>
where
    M: Default,
{
    fn default() -> Self {
        PendingOperation {
            op: UnsafeCell::new(None),
            resp: Cell::new(None),
            meta: Default::default(),
        }
    }
}

/// Contains state of a particular thread w.r.g. to outstanding operations.
///
/// The primary purpose of this type is to batch operations issued on a thread
/// before appending them to the shared log. This is achieved using a fix sized
/// buffer. Once operations are executed against the replica, the results of the
/// operations are stored back into the same buffer.
///
/// # Generic arguments
///
/// - `T` should identify operations issued by the thread (an opcode of sorts)
/// and should also contain arguments/parameters required to execute these
/// operations on the replicas.
///
/// - `R` is the type of the result obtained when an operation is executed
/// against the replica.
///
/// - `M` is the type of meta-data that is associated with each operation.
#[repr(align(64))]
pub(crate) struct Context<T, R, M>
where
    T: Sized + Clone,
    R: Sized + Clone,
    M: Default,
{
    /// Array that will hold all pending operations to be appended to the shared
    /// log as well as the results obtained on executing them against a replica.
    pub(crate) batch: [CachePadded<PendingOperation<T, R, M>>; MAX_PENDING_OPS],

    /// Logical array index at which new operations will be enqueued into the
    /// batch. This variable is updated by the thread that owns this context,
    /// and is read by the combiner.
    pub tail: CachePadded<AtomicUsize>,

    /// Logical array index from which any attempt to dequeue responses will be
    /// made. This variable is only accessed by the thread that owns this
    /// context.
    pub head: CachePadded<AtomicUsize>,

    /// Logical array index from which the operations will be dequeued for flat
    /// combining. This variable is updated by the combiner, and is read by the
    /// thread that owns this context.
    pub comb: CachePadded<AtomicUsize>,

    /// Identifies the context number within a replica. It also maps to the
    /// thread-id because the partitioned nature of the contexts in the replica.
    pub idx: usize,
}

impl<T, R, M> Default for Context<T, R, M>
where
    T: Sized + Clone,
    R: Sized + Clone,
    M: Default,
{
    /// Default constructor for the context.
    fn default() -> Self {
        let mut batch: [CachePadded<PendingOperation<T, R, M>>; MAX_PENDING_OPS] =
            unsafe { ::core::mem::MaybeUninit::zeroed().assume_init() };
        for elem in &mut batch[..] {
            *elem = CachePadded::new(Default::default());
        }

        Context {
            batch,
            tail: CachePadded::new(AtomicUsize::new(0)),
            head: CachePadded::new(AtomicUsize::new(0)),
            comb: CachePadded::new(AtomicUsize::new(0)),
            idx: 0,
        }
    }
}

impl<T, R, M> Context<T, R, M>
where
    T: Sized + Clone,
    R: Sized + Clone,
    M: Default,
{
    pub fn new(idx: usize) -> Self {
        Self {
            idx,
            ..Default::default()
        }
    }

    /// Enqueues an operation onto this context's batch of pending operations.
    ///
    /// Returns true if the operation was successfully enqueued. False
    /// otherwise.
    #[inline(always)]
    pub(crate) fn enqueue(&self, op: T, meta: M) -> bool {
        let t = self.tail.load(Ordering::Acquire);
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
        let e = self.batch[self.index(t)].op.get();
        unsafe { *e = Some(op) };
        let me = self.batch[self.index(t)].meta.get();
        unsafe { *me = meta };

        self.tail.store(t + 1, Ordering::Release);
        true
    }

    /// Enqueues a batch of responses onto this context. This is invoked by the combiner
    /// after it has executed operations (obtained through a call to ops()) against the
    /// replica this thread is registered against.
    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) fn enqueue_resps(&self, responses: &[R]) {
        let n = responses.len();

        // Empty slice passed in; no work to do, so simply return.
        if n == 0 {
            return;
        };

        // Starting from `comb`, write all responses into the batch. Assume here that
        // the slice above doesn't cause us to cross the tail of the batch.
        for response in responses.iter().take(n) {
            self.enqueue_resp(response.clone());
        }
    }

    /// Enqueues a response onto this context. This is invoked by the combiner
    /// after it has executed operations (obtained through a call to ops()) against the
    /// replica this thread is registered against.
    #[inline(always)]
    pub(crate) fn enqueue_resp(&self, response: R) {
        let h = self.comb.load(Ordering::Relaxed);
        self.batch[self.index(h)].resp.replace(Some(response));
        self.comb.store(h + 1, Ordering::Relaxed);
    }

    /// Returns a single response if available. Otherwise, returns None.
    #[inline(always)]
    pub(crate) fn res(&self) -> Option<R> {
        let s = self.head.load(Ordering::Relaxed);
        let f = self.comb.load(Ordering::Relaxed);

        // No responses ready yet; return to the caller.
        if s == f {
            return None;
        };

        if s > f {
            panic!("Head of thread-local batch has advanced beyond combiner of.store!");
        }

        self.head.store(s + 1, Ordering::Relaxed);
        self.batch[self.index(s)].resp.take()
    }

    /// Returns the maximum number of operations that will go pending on this context.
    #[inline(always)]
    pub(crate) fn batch_size() -> usize {
        MAX_PENDING_OPS
    }

    /// Given a logical address, returns an index into the batch at which it falls.
    #[inline(always)]
    pub(crate) fn index(&self, logical: usize) -> usize {
        logical & (MAX_PENDING_OPS - 1)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::vec;

    // Tests whether we can successfully default construct a context.
    #[test]
    fn test_context_create_default() {
        let c = Context::<u64, Result<u64, ()>, ()>::default();
        assert_eq!(c.batch.len(), MAX_PENDING_OPS);
        assert_eq!(c.tail.load(Ordering::Relaxed), 0);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);
    }

    // Tests whether we can successfully enqueue an operation onto the context.
    #[test]
    fn test_context_enqueue() {
        let c = Context::<u64, Result<u64, ()>, ()>::default();
        assert!(c.enqueue(121, ()));
        unsafe { assert_eq!((*c.batch[0].op.get()), Some(121)) };
        assert_eq!(c.tail.load(Ordering::Relaxed), 1);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);
    }

    // Tests that enqueues on the context fail when it's batch of operations is full.
    #[test]
    fn test_context_enqueue_full() {
        let c = Context::<u64, Result<u64, ()>, ()>::default();
        c.tail.store(MAX_PENDING_OPS, Ordering::Relaxed);

        assert!(!c.enqueue(100, ()));
        assert_eq!(c.tail.load(Ordering::Relaxed), MAX_PENDING_OPS);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);
    }

    // Tests that we can successfully enqueue responses onto the context.
    #[test]
    fn test_context_enqueue_resps() {
        let c = Context::<u64, Result<u64, ()>, ()>::default();
        let r = [Ok(11), Ok(12), Ok(13), Ok(14)];

        c.tail.store(16, Ordering::Relaxed);
        c.comb.store(12, Ordering::Relaxed);
        c.enqueue_resps(&r);

        assert_eq!(c.tail.load(Ordering::Relaxed), 16);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 16);

        assert_eq!(c.batch[12].resp.get(), Some(r[0]));
        assert_eq!(c.batch[13].resp.get(), Some(r[1]));
        assert_eq!(c.batch[14].resp.get(), Some(r[2]));
        assert_eq!(c.batch[15].resp.get(), Some(r[3]));
    }

    // Tests that attempting to enqueue an empty batch of responses on the context
    // does nothing.
    #[test]
    fn test_context_enqueue_resps_empty() {
        let c = Context::<u64, Result<u64, ()>, ()>::default();
        let r = [];

        c.tail.store(16, Ordering::Relaxed);
        c.comb.store(12, Ordering::Relaxed);
        c.enqueue_resps(&r);

        assert_eq!(c.tail.load(Ordering::Relaxed), 16);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 12);

        assert_eq!(c.batch[12].resp.get(), None);
    }

    // Tests whether we can retrieve responses enqueued on this context.
    #[test]
    fn test_context_res() {
        let c = Context::<u64, Result<u64, ()>, ()>::default();
        let r = [Ok(11), Ok(12), Ok(13), Ok(14)];

        c.tail.store(16, Ordering::Relaxed);
        c.enqueue_resps(&r);

        assert_eq!(c.tail.load(Ordering::Relaxed), 16);
        assert_eq!(c.comb.load(Ordering::Relaxed), 4);

        assert_eq!(c.res(), Some(r[0]));
        assert_eq!(c.head.load(Ordering::Relaxed), 1);

        assert_eq!(c.res(), Some(r[1]));
        assert_eq!(c.head.load(Ordering::Relaxed), 2);

        assert_eq!(c.res(), Some(r[2]));
        assert_eq!(c.head.load(Ordering::Relaxed), 3);

        assert_eq!(c.res(), Some(r[3]));
        assert_eq!(c.head.load(Ordering::Relaxed), 4);
    }

    // Tests that we cannot retrieve responses when none were enqueued to begin with.
    #[test]
    fn test_context_res_empty() {
        let c = Context::<usize, usize, ()>::default();

        c.tail.store(8, Ordering::Relaxed);

        assert_eq!(c.tail.load(Ordering::Relaxed), 8);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);

        assert_eq!(c.res(), None);
    }

    // Tests that res panics if the head moves beyond the combiner offset.
    #[test]
    #[should_panic]
    fn test_context_res_panic() {
        let c = Context::<usize, usize, ()>::default();

        c.tail.store(8, Ordering::Relaxed);
        c.comb.store(4, Ordering::Relaxed);
        c.head.store(6, Ordering::Relaxed);

        assert_eq!(c.res(), None);
    }

    // Tests that batch_size() works correctly.
    #[test]
    fn test_context_batch_size() {
        assert_eq!(Context::<usize, usize, ()>::batch_size(), MAX_PENDING_OPS);
    }

    // Tests that index() works correctly.
    #[test]
    fn test_index() {
        let c = Context::<u64, Result<u64, ()>, ()>::default();
        assert_eq!(c.index(100), 100 % MAX_PENDING_OPS);
    }
}
