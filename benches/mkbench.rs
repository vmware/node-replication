// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Helper functions to instantiate and configure benchmarks.

use criterion::{Criterion, Throughput};
use node_replication::{log::Log, replica::Replica, Dispatch};
use std::sync::Arc;

/// Creates a benchmark to evalute the overhead the log adds for a given data-structure.
///
/// Takes a generic data-structure that implements dispatch and a vector of operations
/// to execute against said data-structure.
///
/// Then configures the supplied criterion runner to do two benchmarks:
/// - Running the DS operations on a single-thread directly against the DS.
/// - Running the DS operation on a single-thread but go through a replica/log.
pub fn baseline_comparison<T: Dispatch + Default>(
    c: &mut Criterion,
    name: &str,
    ops: Vec<<T as Dispatch>::Operation>,
    log_size_bytes: usize,
) {
    let s: T = Default::default();

    // First benchmark is just a stack on a single thread:
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(ops.len() as u64));
    group.bench_function("baseline", |b| {
        b.iter(|| {
            for i in 0..ops.len() {
                s.dispatch(ops[i]);
            }
        })
    });

    // 2nd benchmark we compare the stack but now we put a log in front:
    let log = Arc::new(Log::<<T as Dispatch>::Operation>::new(log_size_bytes));
    let r = Replica::<T>::new(&log);
    let ridx = r.register().expect("Failed to register with Replica.");

    group.bench_function("log", |b| {
        b.iter(|| {
            for i in 0..ops.len() {
                r.execute(ops[i], ridx);
            }
        })
    });

    group.finish();
}

pub fn scaleout<T: Dispatch + Default>(
    c: &mut Criterion,
    name: &str,
    ops: Vec<<T as Dispatch>::Operation>,
    log_size_bytes: usize,
) {
}
