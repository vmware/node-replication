// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use alloc::vec::Vec;
use core::cell::Cell;
use core::default::Default;
use core::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

/// The maximum number of operations that can be batched inside this context.
/// NOTE: This constant must be a power of two for index() to work.
pub(crate) const MAX_PENDING_OPS: usize = 32;
const_assert!(MAX_PENDING_OPS >= 1 && (MAX_PENDING_OPS & (MAX_PENDING_OPS - 1) == 0));

/// A pending operation is a combination of the its op-code (T),
/// and the corresponding result (R).
/// Cell contains: Operatio, hash, response, is_scan, is_read_only
type PendingOperation<T, R> = Cell<(
    Option<T>,
    Option<usize>,
    Option<R>,
    Option<bool>,
    Option<bool>,
)>;

/// Contains all state local to a particular thread.
///
/// The primary purpose of this type is to batch operations issued on a thread before
/// appending them to the shared log. This is achieved using a fixed sized array. Once
/// executed against the replica, the results of these operations are stored back into
/// the same array.
///
/// `T` is a type parameter required by the struct. `T` should identify operations
/// issued by the thread (an opcode of sorts) and should also contain arguments/parameters
/// required to execute these operations on the replicas.
///
/// `R` is a type parameter required by the struct. It is the type on the result obtained
/// when an operation is executed against the replica.
#[repr(align(64))]
pub(crate) struct Context<T, R>
where
    T: Sized + Clone,
    R: Sized + Clone,
{
    /// Array that will hold all pending operations to be appended to the shared log as
    /// well as the results obtained on executing them against a replica.
    batch: [CachePadded<PendingOperation<T, R>>; MAX_PENDING_OPS],

    /// Logical array index at which new operations will be enqueued into the batch.
    /// This variable is updated by the thread that owns this context, and is read by the
    /// combiner. We can avoid making it an atomic by assuming we're on x86.
    pub tail: CachePadded<AtomicUsize>,

    /// Logical array index from which any attempt to dequeue responses will be made.
    /// This variable is only accessed by the thread that owns this context.
    pub head: CachePadded<AtomicUsize>,

    /// Logical array index from which the operations will be dequeued for flat combining.
    /// This variable is updated by the combiner, and is read by the thread that owns this context.
    /// We can avoid making it an atomic by assuming we're on x86.
    pub comb: CachePadded<AtomicUsize>,

    /// Identifies the context number with-in a replica. Id also maps to the thread-id because
    /// the partitioned nature of the contexts in the replica.
    idx: usize,
}

impl<T, R> Default for Context<T, R>
where
    T: Sized + Clone,
    R: Sized + Clone,
{
    /// Default constructor for the context.
    fn default() -> Context<T, R> {
        let mut batch: [CachePadded<PendingOperation<T, R>>; MAX_PENDING_OPS] =
            unsafe { ::core::mem::MaybeUninit::zeroed().assume_init() };
        for elem in &mut batch[..] {
            *elem = CachePadded::new(Cell::new((None, None, None, None, None)));
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

impl<T, R> Context<T, R>
where
    T: Sized + Clone,
    R: Sized + Clone,
{
    pub fn new(idx: usize) -> Context<T, R> {
        Self {
            idx,
            ..Default::default()
        }
    }

    /// Enqueues an operation onto this context's batch of pending operations.
    ///
    /// Returns true if the operation was successfully enqueued. False otherwise.
    #[inline(always)]
    pub(crate) fn enqueue(&self, op: T, hash: usize, is_scan: bool, is_read_only: bool) -> bool {
        let t = self.tail.load(Ordering::Relaxed);
        let h = self.head.load(Ordering::Relaxed);

        // Check if we have space in the batch to hold this operation. If we don't, then
        // return false to the caller thread.
        if t - h == MAX_PENDING_OPS {
            return false;
        };

        // Add in the operation to the batch. Once added, update the tail so that the
        // combiner sees this operation. Relying on TSO here to make sure that the tail
        // is updated only after the operation has been written in.
        let e = self.batch[self.index(t)].as_ptr();
        unsafe { (*e).0 = Some(op) };
        unsafe { (*e).1 = Some(hash) };
        unsafe { (*e).3 = Some(is_scan) };
        unsafe { (*e).4 = Some(is_read_only) };

        self.tail.store(t + 1, Ordering::Relaxed);
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
    pub(crate) fn enqueue_resp(&self, responses: R) {
        let h = self.comb.load(Ordering::Relaxed);

        let e = self.batch[self.index(h)].as_ptr();
        unsafe {
            (*e).2 = Some(responses);
        }

        self.comb.store(h + 1, Ordering::Relaxed);
    }

    /// Adds any pending operations on this context to a passed in buffer. Returns the
    /// the number of such operations that were added in.
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
            // By construction, we know that everything between `comb` and `tail` is a
            // valid operation ready for flat combining. Hence, calling unwrap() here
            // on the operation is safe.
            let e = self.batch[self.index(i)].as_ptr();
            if unsafe { (*e).1 } == Some(hash) {
                match unsafe { (*e).3.unwrap() } {
                    true => unsafe {
                        scan_buffer.push((
                            (*e).0.as_ref().unwrap().clone(),
                            self.idx,
                            (*e).4.unwrap(),
                        ))
                    },
                    false => unsafe {
                        buffer.push(((*e).0.as_ref().unwrap().clone(), self.idx, (*e).4.unwrap()))
                    },
                }

                n += 1;
            }
        }

        n
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
        unsafe { (*self.batch[self.index(s)].as_ptr()).2.clone() }
    }

    /// Returns the maximum number of operations that will go pending on this context.
    #[inline(always)]
    pub(crate) fn batch_size() -> usize {
        MAX_PENDING_OPS
    }

    /// Given a logical address, returns an index into the batch at which it falls.
    #[inline(always)]
    fn index(&self, logical: usize) -> usize {
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
        let c = Context::<u64, Result<u64, ()>>::default();
        assert_eq!(c.batch.len(), MAX_PENDING_OPS);
        assert_eq!(c.tail.load(Ordering::Relaxed), 0);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);
    }

    // Tests whether we can successfully enqueue an operation onto the context.
    #[test]
    fn test_context_enqueue() {
        let c = Context::<u64, Result<u64, ()>>::default();
        assert!(c.enqueue(121, 0, false, true));
        unsafe { assert_eq!((*c.batch[0].as_ptr()).0, Some(121)) };
        assert_eq!(c.tail.load(Ordering::Relaxed), 1);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);
    }

    // Tests that enqueues on the context fail when it's batch of operations is full.
    #[test]
    fn test_context_enqueue_full() {
        let c = Context::<u64, Result<u64, ()>>::default();
        c.tail.store(MAX_PENDING_OPS, Ordering::Relaxed);

        assert!(!c.enqueue(100, 0, false, true));
        assert_eq!(c.tail.load(Ordering::Relaxed), MAX_PENDING_OPS);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);
    }

    // Tests that we can successfully enqueue responses onto the context.
    #[test]
    fn test_context_enqueue_resps() {
        let c = Context::<u64, Result<u64, ()>>::default();
        let r = [Ok(11), Ok(12), Ok(13), Ok(14)];

        c.tail.store(16, Ordering::Relaxed);
        c.comb.store(12, Ordering::Relaxed);
        c.enqueue_resps(&r);

        assert_eq!(c.tail.load(Ordering::Relaxed), 16);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 16);

        assert_eq!(c.batch[12].get().2, Some(r[0]));
        assert_eq!(c.batch[13].get().2, Some(r[1]));
        assert_eq!(c.batch[14].get().2, Some(r[2]));
        assert_eq!(c.batch[15].get().2, Some(r[3]));
    }

    // Tests that attempting to enqueue an empty batch of responses on the context
    // does nothing.
    #[test]
    fn test_context_enqueue_resps_empty() {
        let c = Context::<u64, Result<u64, ()>>::default();
        let r = [];

        c.tail.store(16, Ordering::Relaxed);
        c.comb.store(12, Ordering::Relaxed);
        c.enqueue_resps(&r);

        assert_eq!(c.tail.load(Ordering::Relaxed), 16);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 12);

        assert_eq!(c.batch[12].get().2, None);
    }

    // Tests whether ops() can successfully retrieve operations enqueued on this context.
    #[test]
    fn test_context_ops() {
        let c = Context::<usize, usize>::default();
        let mut o = vec![];
        let mut scan = vec![];

        for idx in 0..MAX_PENDING_OPS / 2 {
            assert!(c.enqueue(idx * idx, 1, false, false))
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
            assert!(c.enqueue(idx * idx, 1, true, false))
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

    // Tests whether we can retrieve responses enqueued on this context.
    #[test]
    fn test_context_res() {
        let c = Context::<u64, Result<u64, ()>>::default();
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
        let c = Context::<usize, usize>::default();

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
        let c = Context::<usize, usize>::default();

        c.tail.store(8, Ordering::Relaxed);
        c.comb.store(4, Ordering::Relaxed);
        c.head.store(6, Ordering::Relaxed);

        assert_eq!(c.res(), None);
    }

    // Tests that batch_size() works correctly.
    #[test]
    fn test_context_batch_size() {
        assert_eq!(Context::<usize, usize>::batch_size(), MAX_PENDING_OPS);
    }

    // Tests that index() works correctly.
    #[test]
    fn test_index() {
        let c = Context::<u64, Result<u64, ()>>::default();
        assert_eq!(c.index(100), 100 % MAX_PENDING_OPS);
    }
}
