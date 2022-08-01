// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Contains the shared Log, in a nutshell it's a multi-producer, multi-consumer
//! circular-buffer.

pub use crate::log::LogToken;
pub use crate::log::DEFAULT_LOG_BYTES;
pub use crate::log::MAX_REPLICAS_PER_LOG;

pub use crate::log::GC_FROM_HEAD;

pub use crate::log::WARN_THRESHOLD;

pub type Log<T> = crate::log::Log<T, (), ()>;

#[cfg(test)]
mod tests {
    // Import std so that we have an allocator for our unit tests.
    extern crate std;
    use core::sync::atomic::Ordering;
    use std::sync::Arc;

    use super::*;

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

        let o = [(Operation::Read, ())];
        assert!(l
            .append(&o, &lt, |_o: Operation, _mine: bool, _md: ()| { true })
            .is_ok());

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

        let o = [(Operation::Read, ()), (Operation::Write(119), ())];
        assert!(l
            .append(&o, &lt, |_o: Operation, _mine: bool, _md: ()| { true })
            .is_ok());

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
            .advance_head(&lt, &mut |_o: Operation, _mine: bool, _md: ()| { true })
            .is_ok());
        assert_eq!(l.head.load(Ordering::Relaxed), 224);
    }

    // Tests that the head of the log is advanced when we're close to filling up the entire log.
    #[test]
    fn test_log_append_gc() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o: [(Operation, ()); 4] = unsafe {
            let mut a: [(Operation, ()); 4] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, (Operation::Read, ()));
            }
            a
        };

        l.next.store(2, Ordering::Relaxed);
        l.tail
            .store(l.slog.len() - GC_FROM_HEAD - 1, Ordering::Relaxed);
        l.ltails[0].store(1024, Ordering::Relaxed);
        assert!(l
            .append(&o, &lt, |_o: Operation, _mine: bool, _md: ()| { true })
            .is_ok());

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

        let o: [(Operation, ()); 1024] = unsafe {
            let mut a: [(Operation, ()); 1024] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, (Operation::Read, ()));
            }
            a
        };

        l.next.store(2, Ordering::Relaxed);
        l.head.store(2 * 8192, Ordering::Relaxed);
        l.tail.store(l.slog.len() - 10, Ordering::Relaxed);
        assert!(l
            .append(&o, &lt, |_o: Operation, _mine: bool, _md: ()| { true })
            .is_ok());

        assert_eq!(l.lmasks[0].get(), true);
        assert_eq!(l.tail.load(Ordering::Relaxed), l.slog.len() + 1014);
    }

    // Test that we can execute operations appended to the log.
    #[test]
    fn test_log_exec() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o = [(Operation::Read, ())];
        let mut f = |op: Operation, mine: bool, _md: ()| {
            assert_eq!(op, Operation::Read);
            assert!(mine);
            true
        };

        assert!(l
            .append(&o, &lt, |_o: Operation, _mine: bool, _md: ()| { true })
            .is_ok());
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

        let mut f = |_o: Operation, _mine: bool, _md: ()| {
            assert!(false);
            true
        };

        l.exec(&lt, &mut f);
    }

    // Test that exec() doesn't do anything if we're already up-to-date.
    #[test]
    fn test_log_exec_zero() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o = [(Operation::Read, ())];
        let mut f = |op: Operation, mine: bool, _md: ()| {
            assert_eq!(op, Operation::Read);
            assert!(mine);
            true
        };
        let mut g = |_op: Operation, _mine: bool, _md: ()| {
            assert!(false);
            true
        };

        assert!(l
            .append(&o, &lt, |_o: Operation, _mine: bool, _md: ()| { true })
            .is_ok());
        l.exec(&lt, &mut f);
        l.exec(&lt, &mut g);
    }

    // Test that multiple entries on the log can be executed correctly.
    #[test]
    fn test_log_exec_multiple() {
        let l = Log::<Operation>::default();
        let lt = l.register().unwrap();

        let o = [(Operation::Read, ()), (Operation::Write(119), ())];
        let mut s = 0;
        let mut f = |op: Operation, _mine, _md| {
            match op {
                Operation::Read => s += 121,
                Operation::Write(v) => s += v,
                Operation::Invalid => assert!(false),
            };
            true
        };

        assert!(l
            .append(&o, &lt, |_o: Operation, _mine: bool, _md: ()| { true })
            .is_ok());
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

        let o: [(Operation, ()); 1024] = unsafe {
            let mut a: [(Operation, ()); 1024] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, (Operation::Read, ()));
            }
            a
        };
        let mut f = |op: Operation, mine: bool, _md: ()| {
            assert_eq!(op, Operation::Read);
            assert!(mine);
            true
        };

        assert!(l
            .append(&o, &lt, |_o: Operation, _mine: bool, _md: ()| { true })
            .is_ok()); // Required for GC to work correctly.
        l.next.store(2, Ordering::SeqCst);
        l.head.store(2 * 8192, Ordering::SeqCst);
        l.tail.store(l.slog.len() - 10, Ordering::SeqCst);
        assert!(l
            .append(&o, &lt, |_o: Operation, _mine: bool, _md: ()| { true })
            .is_ok());

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

        let o: [(Operation, ()); 1024] = unsafe {
            let mut a: [(Operation, ()); 1024] = ::std::mem::MaybeUninit::zeroed().assume_init();
            for i in &mut a[..] {
                ::std::ptr::write(i, (Operation::Read, ()));
            }
            a
        };
        let mut f = |_op: Operation, _mine, _md| {
            assert!(false);
            true
        };

        assert!(l
            .append(&o, &lt, |_o: Operation, _mine: bool, _md: ()| { true })
            .is_ok());
        l.head.store(8192, Ordering::SeqCst);

        l.exec(&lt, &mut f);
    }

    // Tests that operations are cloned when added to the log, and that
    // they are correctly dropped once overwritten.
    #[test]
    fn test_log_change_refcount() {
        let l = Log::<Arc<Operation>>::default();
        let lt = l.register().unwrap();

        let o1 = [(Arc::new(Operation::Read), ())];
        let o2 = [(Arc::new(Operation::Read), ())];
        assert_eq!(Arc::strong_count(&o1[0].0), 1);
        assert_eq!(Arc::strong_count(&o2[0].0), 1);

        assert!(l
            .append(&o1[..], &lt, |_o: Arc<Operation>, _mine, _md| { true })
            .is_ok());
        assert_eq!(Arc::strong_count(&o1[0].0), 2);
        assert!(l
            .append(&o1[..], &lt, |_o: Arc<Operation>, _mine, _md| { true })
            .is_ok());
        assert_eq!(Arc::strong_count(&o1[0].0), 3);

        unsafe { l.reset() };

        // Over here, we overwrite entries that were written to by the two
        // previous appends. This decreases the refcount of o1 and increases
        // the refcount of o2.
        assert!(l
            .append(&o2[..], &lt, |_o: Arc<Operation>, _mine, _md| { true })
            .is_ok());
        assert_eq!(Arc::strong_count(&o1[0].0), 2);
        assert_eq!(Arc::strong_count(&o2[0].0), 2);
        assert!(l
            .append(&o2[..], &lt, |_o: Arc<Operation>, _mine, _md| { true })
            .is_ok());
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
        let size: usize = total_entries * entry_size;
        let l = Log::<Arc<Operation>>::new_with_bytes(size, ());
        let lt = l.register().unwrap();
        let o1 = [(Arc::new(Operation::Read), ())];
        let o2 = [(Arc::new(Operation::Read), ())];
        assert_eq!(Arc::strong_count(&o1[0].0), 1);
        assert_eq!(Arc::strong_count(&o2[0].0), 1);

        for i in 1..(total_entries + 1) {
            assert!(l
                .append(&o1[..], &lt, |_o: Arc<Operation>, _mine, _md| { true })
                .is_ok());
            assert_eq!(Arc::strong_count(&o1[0].0), i + 1);
        }
        assert_eq!(Arc::strong_count(&o1[0].0), total_entries + 1);

        for i in 1..(total_entries + 1) {
            assert!(l
                .append(&o2[..], &lt, |_o: Arc<Operation>, _mine, _md| { true })
                .is_ok());
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
        let l = Log::<Operation>::default();
        let one = l.register().unwrap();
        let two = l.register().unwrap();

        assert_eq!(one, LogToken(1));
        assert_eq!(two, LogToken(2));

        let o = [(Operation::Read, ())];
        let mut f = |op: Operation, mine: bool, _md: ()| {
            assert_eq!(op, Operation::Read);
            assert!(mine);
            true
        };

        assert!(l
            .append(&o, &one, |_o: Operation, _mine: bool, _md: ()| { true })
            .is_ok());
        l.exec(&one, &mut f);
        assert_eq!(l.is_replica_synced_for_reads(&one, l.get_ctail()), true);
        assert_eq!(l.is_replica_synced_for_reads(&two, l.get_ctail()), false);

        let mut f = |op: Operation, mine: bool, _md: ()| {
            assert_eq!(op, Operation::Read);
            assert!(!mine);
            true
        };
        l.exec(&two, &mut f);
        assert_eq!(l.is_replica_synced_for_reads(&two, l.get_ctail()), true);
    }
}
