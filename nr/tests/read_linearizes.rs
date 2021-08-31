// Copyright © 2019-2021 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! If many threads read from the replica, they should all see a monotonically
//! increasing value.
//!
//! By saying we ensure linearizability, it should not possible that thread A
//! reads 2 from TheCounter, sends that value to thread B, which then reads 1
//! from TheCounter.
//!
//! This loom test is supposed to verify that this can't happen.
//! See also: https://github.com/tokio-rs/loom

// Run with:
// RUSTFLAGS="--cfg loom" cargo test --test read_linearizes --release -- --nocapture

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, Ordering};
use loom::sync::Arc;
use loom::thread;

use node_replication::Dispatch;
use node_replication::Log;
use node_replication::Replica;

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
enum OpWr {
    Increment,
    Noop,
}

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
enum OpRd {
    Get,
}

// Our data-structure is a silly, replicated counter that monotonically
// increases.
#[derive(Eq, PartialEq, Clone, Copy, Debug, Default)]
struct TheCounter {
    pub counter: usize,
}

impl Dispatch for TheCounter {
    type ReadOperation = OpRd;
    type WriteOperation = OpWr;
    type Response = usize;

    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            OpRd::Get => self.counter,
        }
    }

    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
        match op {
            OpWr::Increment => {
                self.counter += 1;
                //println!("counter is {}", self.counter);
                self.counter
            }
            OpWr::Noop => 0,
        }
    }
}

// This is a simple variant of the test that ensures reads linearize in the
// default case when there is no GC.
#[test]
fn test_read_linearizes_no_gc() {
    loom::model(move || {
        let log = Arc::new(Log::<<TheCounter as Dispatch>::WriteOperation>::new(4096));
        let r1 = Arc::new(Replica::<TheCounter>::new(&log));
        let r2 = Replica::<TheCounter>::new(&log);
        let (tx, rx) = loom::sync::mpsc::channel::<usize>();

        let mut threads = Vec::new();

        {
            let r1a = r1.clone();
            let child = thread::spawn(move || {
                let idx = r1a.register().expect("Failed to register with Replica.");

                let cntr_val = r1a.execute_mut(OpWr::Increment, idx);

                assert_eq!(cntr_val, 1);
            });
            threads.push(child);
        }

        {
            let child = thread::spawn(move || {
                let idx = r1.register().expect("Failed to register with Replica.");

                let cntr_val = r1.execute(OpRd::Get, idx);
                assert!(cntr_val == 1 || cntr_val == 0);

                tx.send(cntr_val).unwrap();
            });
            threads.push(child);
        }

        {
            let child = thread::spawn(move || {
                let idx = r2.register().expect("Failed to register with Replica.");

                let observed_val = rx.recv().unwrap();
                let cntr_val = r2.execute(OpRd::Get, idx);

                assert!(
                    cntr_val >= observed_val,
                    "we read cntr_val={}, but we received observed_val={} from t2",
                    cntr_val,
                    observed_val
                );
            });
            threads.push(child);
        }

        for _i in 0..threads.len() {
            let _retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Thread didn't finish successfully.");
        }
    });
}

// Kinda the same as `test_read_linearizes`, but we make sure we have to do gc
// during `execute_mut` on the first thread.
//
// This triggered a bug in the past where reads could sneak by from another
// thread on the same replica and read a value "too early", (meaning t3 on
// another replica could do a read later in time and see an older value) for
// some thread interleavings.
//
// Unfortunately, loom can't check this model exhaustively (AFAICT due to the
// try_recv) so we have a max. duration of 10s for this test. It was enough to
// trigger the bug but not enough to prove the absence of bugs.
//
// To execute just this test, run: `RUSTFLAGS="--cfg loom" cargo test --test
// read_linearizes --release -- --nocapture test_read_linearizes_with_gc`
#[test]
fn test_read_linearizes_with_gc() {
    let _r = env_logger::try_init().ok();

    let mut b = loom::model::Builder::new();
    b.max_duration = Some(core::time::Duration::from_secs(10));

    b.check(move || {
        // Make a log with just 4 entries, on adding a second entry, we start GC
        let log = Log::<<TheCounter as Dispatch>::WriteOperation>::new(256);
        log.append(&[OpWr::Noop, OpWr::Noop], 2, |_op, _idx| {
            panic!("We're doing GC but we don't want to do it just yet...");
        });

        let log = Arc::new(log);

        let r1 = Arc::new(Replica::<TheCounter>::new(&log));
        let r2 = Replica::<TheCounter>::new(&log);
        let (tx, rx) = loom::sync::mpsc::channel::<usize>();

        let mut threads = Vec::new();

        let done = Arc::new(AtomicBool::new(false));

        {
            let done = done.clone();
            let r1a = r1.clone();
            let child = thread::spawn(move || {
                let idx = r1a.register().expect("Failed to register with Replica.");
                let cntr_val = r1a.execute_mut(OpWr::Increment, idx);
                assert_eq!(cntr_val, 1);
                done.store(true, std::sync::atomic::Ordering::SeqCst);
            });
            threads.push(child);
        }

        {
            let done = done.clone();
            let child = thread::spawn(move || {
                let idx = r1.register().expect("Failed to register with Replica.");
                let cntr_val = r1.execute(OpRd::Get, idx);
                assert!(cntr_val == 1 || cntr_val == 0);
                tx.send(cntr_val).unwrap();

                while !done.load(Ordering::SeqCst) {
                    let _cntr_val = r1.execute(OpRd::Get, idx);
                    loom::thread::yield_now();
                }
            });
            threads.push(child);
        }

        {
            let child = thread::spawn(move || {
                let idx = r2.register().expect("Failed to register with Replica.");
                loop {
                    // In this case (since we do GC here) we need to make sure
                    // our test case does not deadlock. Why? Assume t1 tries to
                    // do GC (so it waits for replica 2 to advance (by
                    // processing the entries)). Now t2 (correctly) can't read
                    // so it doesn't make progress; because t3 (on replica 2)
                    // never receives a value and won't execute the read on r2
                    // (which would let t1 resume from advance_log/GC)
                    if let Ok(observed_val) = rx.try_recv() {
                        let cntr_val = r2.execute(OpRd::Get, idx);
                        assert!(
                            cntr_val >= observed_val,
                            "we read cntr_val={}, but we received observed_val={} from t2",
                            cntr_val,
                            observed_val
                        );
                        break;
                    } else {
                        loom::thread::yield_now();
                        let _cntr_val = r2.execute(OpRd::Get, idx);
                        continue;
                    }
                }
            });
            threads.push(child);
        }

        for _i in 0..threads.len() {
            let _retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Thread didn't finish successfully.");
        }
    });
}
