// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use core::cell::Cell;
use core::default::Default;

use crossbeam_utils::CachePadded;

/// The maximum number of operations that can be batched inside this context.
/// NOTE: This constant must be a power of two for index() to work.
const MAX_PENDING_OPS: usize = 32;

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
pub struct Context<T, R, E>
where
    T: Sized + Clone,
    R: Sized + Copy + Default,
    E: Sized + Copy + Default,
{
    /// Array that will hold all pending operations to be appended to the shared log as
    /// well as the results obtained on executing them against a replica.
    batch: [CachePadded<Cell<(Option<T>, Result<R, E>)>>; MAX_PENDING_OPS],

    /// Logical array index at which new operations will be enqueued into the batch.
    /// This variable is updated by the thread that owns this context, and is read by the
    /// combiner. We can avoid making it an atomic by assuming we're on x86.
    pub tail: CachePadded<Cell<usize>>,

    /// Logical array index from which any attempt to dequeue responses will be made.
    /// This variable is only accessed by the thread that owns this context.
    pub head: CachePadded<Cell<usize>>,

    /// Logical array index from which the operations will be dequeued for flat combining.
    /// This variable is updated by the combiner, and is read by the thread that owns this context.
    /// We can avoid making it an atomic by assuming we're on x86.
    pub comb: CachePadded<Cell<usize>>,
}

impl<T, R, E> Default for Context<T, R, E>
where
    T: Sized + Clone,
    R: Sized + Copy + Default,
    E: Sized + Copy + Default,
{
    /// Default constructor for the context.
    fn default() -> Context<T, R, E> {
        let mut batch: [CachePadded<Cell<(Option<T>, Result<R, E>)>>; MAX_PENDING_OPS] =
            unsafe { ::core::mem::MaybeUninit::zeroed().assume_init() };
        for elem in &mut batch[..] {
            *elem = CachePadded::new(Cell::new((None, Err(Default::default()))));
        }

        Context {
            batch: batch,
            tail: CachePadded::new(Cell::new(Default::default())),
            head: CachePadded::new(Cell::new(Default::default())),
            comb: CachePadded::new(Cell::new(Default::default())),
        }
    }
}

impl<T, R, E> Context<T, R, E>
where
    T: Sized + Clone,
    R: Sized + Copy + Default,
    E: Sized + Copy + Default,
{
    /// Enqueues an operation onto this context's batch of pending operations.
    ///
    /// Returns true if the operation was successfully enqueued. False otherwise.
    #[inline(always)]
    pub fn enqueue(&self, op: T) -> bool {
        let t = self.tail.get();
        let h = self.head.get();

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

        self.tail.set(t + 1);
        true
    }

    /// Enqueues a batch of responses onto this context. This is invoked by the combiner
    /// after it has executed operations (obtained through a call to ops()) against the
    /// replica this thread is registered against.
    #[inline(always)]
    pub fn enqueue_resps(&self, responses: &[Result<R, E>]) {
        let h = self.comb.get();
        let n = responses.len();

        // Empty slice passed in; no work to do, so simply return.
        if n == 0 {
            return;
        };

        // Starting from `comb`, write all responses into the batch. Assume here that
        // the slice above doesn't cause us to cross the tail of the batch.
        for i in 0..n {
            let e = self.batch[self.index(h + i)].as_ptr();
            unsafe {
                (*e).1 = responses[i];
            }
        }

        self.comb.set(h + n);
    }

    /// Adds any pending operations on this context to a passed in buffer. Returns the
    /// the number of such operations that were added in.
    #[inline(always)]
    pub fn ops(&self, buffer: &mut Vec<T>) -> usize {
        let mut h = self.comb.get();
        let t = self.tail.get();

        // No operations on this thread; return to the caller indicating so.
        if h == t {
            return 0;
        };

        if h > t {
            panic!("Head of thread-local batch has advanced beyond tail!");
        }

        // Iterate from `comb` to `tail`, adding pending operations into the
        // passed in buffer. Return the number of operations that were added.
        let mut n = 0;
        loop {
            if h == t {
                break;
            };

            unsafe {
                // enqueue() will always execute before ops(), so its safe to unwarp the Op.
                buffer.push(
                    (*self.batch[self.index(h)].as_ptr())
                        .0
                        .as_ref()
                        .unwrap()
                        .clone(),
                );
            }
            h += 1;
            n += 1;
        }

        n
    }

    /// Appends any responses/results to enqueued operations into a passed in buffer.
    #[inline(always)]
    pub fn res(&self, buffer: &mut Vec<Result<R, E>>) {
        let mut s = self.head.get();
        let f = self.comb.get();

        // No responses ready yet; return to the caller.
        if s == f {
            return;
        };

        if s > f {
            panic!("Head of thread-local batch has advanced beyond combiner offset!");
        }

        // Iterate from `head` to `comb`, adding responses into the passed in buffer.
        // Once we're done, update `head` to the value of `comb` we read above.
        loop {
            if s == f {
                break;
            };

            unsafe {
                buffer.push((*self.batch[self.index(s)].as_ptr()).1.clone());
            }
            s += 1;
        }

        self.head.set(f);
    }

    /// Returns the maximum number of operations that will go pending on this context.
    #[inline(always)]
    pub fn batch_size() -> usize {
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

    // Tests whether we can successfully default construct a context.
    #[test]
    fn test_context_create_default() {
        let c = Context::<u64, u64, ()>::default();
        assert_eq!(c.batch.len(), MAX_PENDING_OPS);
        assert_eq!(c.tail.get(), 0);
        assert_eq!(c.head.get(), 0);
        assert_eq!(c.comb.get(), 0);
    }

    // Tests whether we can successfully enqueue an operation onto the context.
    #[test]
    fn test_context_enqueue() {
        let c = Context::<u64, u64, ()>::default();
        assert!(c.enqueue(121));
        unsafe { assert_eq!((*c.batch[0].as_ptr()).0, Some(121)) };
        assert_eq!(c.tail.take(), 1);
        assert_eq!(c.head.take(), 0);
        assert_eq!(c.comb.take(), 0);
    }

    // Tests that enqueues on the context fail when it's batch of operations is full.
    #[test]
    fn test_context_enqueue_full() {
        let c = Context::<u64, u64, ()>::default();
        c.tail.set(MAX_PENDING_OPS);

        assert!(!c.enqueue(100));
        assert_eq!(c.tail.get(), MAX_PENDING_OPS);
        assert_eq!(c.head.get(), 0);
        assert_eq!(c.comb.get(), 0);
    }

    // Tests that we can successfully enqueue responses onto the context.
    #[test]
    fn test_context_enqueue_resps() {
        let c = Context::<u64, u64, ()>::default();
        let r = [Ok(11), Ok(12), Ok(13), Ok(14)];

        c.tail.set(16);
        c.comb.set(12);
        c.enqueue_resps(&r);

        assert_eq!(c.tail.get(), 16);
        assert_eq!(c.head.get(), 0);
        assert_eq!(c.comb.get(), 16);

        assert_eq!(c.batch[12].get().1, r[0]);
        assert_eq!(c.batch[13].get().1, r[1]);
        assert_eq!(c.batch[14].get().1, r[2]);
        assert_eq!(c.batch[15].get().1, r[3]);
    }

    // Tests that attempting to enqueue an empty batch of responses on the context
    // does nothing.
    #[test]
    fn test_context_enqueue_resps_empty() {
        let c = Context::<u64, u64, ()>::default();
        let r = [];

        c.tail.set(16);
        c.comb.set(12);
        c.enqueue_resps(&r);

        assert_eq!(c.tail.get(), 16);
        assert_eq!(c.head.get(), 0);
        assert_eq!(c.comb.get(), 12);
    }

    // Tests whether ops() can successfully retrieve operations enqueued on this context.
    #[test]
    fn test_context_ops() {
        let c = Context::<usize, usize, ()>::default();
        let mut o = vec![];

        for idx in 0..MAX_PENDING_OPS / 2 {
            assert!(c.enqueue(idx * idx))
        }

        assert_eq!(c.ops(&mut o), MAX_PENDING_OPS / 2);
        assert_eq!(o.len(), MAX_PENDING_OPS / 2);
        assert_eq!(c.tail.get(), MAX_PENDING_OPS / 2);
        assert_eq!(c.head.get(), 0);
        assert_eq!(c.comb.get(), 0);

        for idx in 0..MAX_PENDING_OPS / 2 {
            assert_eq!(o[idx], idx * idx)
        }
    }

    // Tests whether ops() returns nothing when we don't have any pending operations.
    #[test]
    fn test_context_ops_empty() {
        let c = Context::<usize, usize, ()>::default();
        let mut o = vec![];

        c.tail.set(8);
        c.comb.set(8);

        assert_eq!(c.ops(&mut o), 0);
        assert_eq!(o.len(), 0);
        assert_eq!(c.tail.get(), 8);
        assert_eq!(c.head.get(), 0);
        assert_eq!(c.comb.get(), 8);
    }

    // Tests whether we can retrieve responses enqueued on this context.
    #[test]
    fn test_context_res() {
        let c = Context::<u64, u64, ()>::default();
        let r = [Ok(11), Ok(12), Ok(13), Ok(14)];
        let mut o = vec![];

        c.tail.set(16);
        c.enqueue_resps(&r);
        c.res(&mut o);

        assert_eq!(c.tail.get(), 16);
        assert_eq!(c.head.get(), 4);
        assert_eq!(c.comb.get(), 4);

        assert_eq!(o.len(), 4);
        assert_eq!(o[0], r[0]);
        assert_eq!(o[1], r[1]);
        assert_eq!(o[2], r[2]);
        assert_eq!(o[3], r[3]);
    }

    // Tests that we cannot retrieve responses when none were enqueued to begin with.
    #[test]
    fn test_context_res_empty() {
        let c = Context::<usize, usize, ()>::default();
        let mut o = vec![];

        c.tail.set(8);
        c.res(&mut o);

        assert_eq!(c.tail.get(), 8);
        assert_eq!(c.head.get(), 0);
        assert_eq!(c.comb.get(), 0);

        assert_eq!(o.len(), 0);
    }

    // Tests that batch_size() works correctly.
    #[test]
    fn test_context_batch_size() {
        assert_eq!(Context::<usize, usize, ()>::batch_size(), MAX_PENDING_OPS);
    }

    // Tests that index() works correctly.
    #[test]
    fn test_index() {
        let c = Context::<u64, u64, ()>::default();
        assert_eq!(c.index(100), 100 % MAX_PENDING_OPS);
    }
}
