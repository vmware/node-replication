use core::default::Default;

/// The maximum number of operations that can be batched inside this context.
const MAX_PENDING_OPS: usize = 32;

/// Contains all state local to a particular thread.
///
/// The primary purpose of this type is to batch operations issued on a thread before
/// appending them to the shared log. This is achieved using a fixed sized array.
///
/// `T` is a type parameter required by the struct. `T` should identify operations
/// issued by the thread (an opcode of sorts) and should also contain arguments/parameters
/// required to execute these operations on the replicas.
///
/// This class is *not* thread-safe. It however provides two methods called acquire() and
/// release() that can be used for thread safety. It is upto the module that uses this
/// class/type to correctly use these methods.
#[repr(align(64))]
#[derive(Default)]
pub struct Context<T>
where
    T: Sized + Copy + Default,
{
    /// Number of pending operations on this thread to be appended to the shared log.
    nops: usize,

    /// Array that will hold all pending operations to be appended to the shared log.
    /// Each operation is represented by an opcode and its parameters. Statically
    /// sized to `MAX_PENDING_OPS`.
    batch: [T; MAX_PENDING_OPS],
}

impl<T> Context<T>
where
    T: Sized + Copy + Default,
{
    /// Enqueues an operation onto this context's batch of pending operations.
    ///
    /// Returns true if the operation was successfully enqueued. False otherwise.
    pub fn enqueue(&mut self, op: T) -> bool {
        // If our thread local batch is full, then return false. We can next
        // enqueue only after one round of flat combining on the NUMA node.
        if self.nops >= self.batch.len() {
            return false;
        }

        // There is space for the operation. Enqueue it and return.
        self.batch[self.nops] = op;
        self.nops += 1;

        true
    }

    /// Returns an option which will contain a slice of operations pending on this
    /// context if any. Otherwise, returns `None`.
    pub fn ops(&self) -> Option<&[T]> {
        if self.nops == 0 {
            return None;
        }

        Some(&self.batch[0..self.nops])
    }

    /// Empties out the current batch of pending operations in the context.
    pub fn reset_ops(&mut self) {
        self.nops = 0;
    }

    /// Returns the maximum number of operations that will go pending on this context.
    pub fn batch_size() -> usize {
        MAX_PENDING_OPS
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // Tests whether we can successfully default construct a context.
    #[test]
    fn test_context_create_default() {
        let c = Context::<u64>::default();
        assert_eq!(c.nops, 0);
        assert_eq!(c.batch.len(), MAX_PENDING_OPS);
    }

    // Tests whether we can successfully enqueue an operation onto the context.
    #[test]
    fn test_context_enqueue() {
        let mut c = Context::<u64>::default();
        assert!(c.enqueue(121));
        assert_eq!(c.nops, 1);
        assert_eq!(c.batch[0], 121);
    }

    // Tests that enqueues on the context fail when it's batch of operations is full.
    #[test]
    fn test_context_enqueue_full() {
        let mut c = Context::<u64>::default();
        for _idx in 0..MAX_PENDING_OPS {
            c.enqueue(121);
        }
        assert_eq!(c.nops, MAX_PENDING_OPS);
        assert!(!c.enqueue(100));
        assert_eq!(c.nops, MAX_PENDING_OPS);
    }

    // Tests whether ops() can successfully retrieve operations enqueued on this context.
    #[test]
    fn test_context_ops() {
        let mut c = Context::<usize>::default();
        for idx in 0..MAX_PENDING_OPS / 2 {
            c.enqueue(idx * idx);
        }

        let o = c.ops();
        assert!(o.is_some());

        let o = o.unwrap();
        assert_eq!(o.len(), MAX_PENDING_OPS / 2);
        for idx in 0..MAX_PENDING_OPS / 2 {
            assert_eq!(o[idx], idx * idx);
        }
    }

    // Tests whether ops() returns 'None' when we don't have any pending operations.
    #[test]
    fn test_context_ops_empty() {
        let c = Context::<usize>::default();
        let o = c.ops();
        assert!(o.is_none());
    }

    // Tests whether reset_ops() successfully sets the number of pending ops to zero.
    #[test]
    fn test_context_reset_ops() {
        let mut c = Context::<usize>::default();
        for idx in 0..MAX_PENDING_OPS / 2 {
            c.enqueue(idx * idx);
        }
        c.reset_ops();
        assert_eq!(c.nops, 0);
        assert!(c.ops().is_none());
    }

    // Tests that batch_size() works correctly.
    #[test]
    fn test_context_batch_size() {
        assert_eq!(Context::<usize>::batch_size(), MAX_PENDING_OPS);
    }
}
