use std::cell::RefCell;
use std::time::{Duration, Instant};
use std::sync::Arc;


use rand::{thread_rng, Rng};

use criterion::{Benchmark, Criterion, ParameterizedBenchmark, Throughput};
use criterion::black_box;

use criterion::criterion_main;
use criterion::criterion_group;

use node_replication::{log::Log, replica::Replica, Dispatch};

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
enum Op {
    Push(u32),

    Pop,

    Invalid,
}

impl Default for Op {
    fn default() -> Op {
        Op::Invalid
    }
}

#[derive(Debug, Clone)]
struct Stack {
    storage: RefCell<Vec<u32>>,
}

impl Stack {
    pub fn push(&self, data: u32) {
        self.storage.borrow_mut().push(data);
    }

    pub fn pop(&self) -> Option<u32> {
        self.storage.borrow_mut().pop()
    }
}

impl Default for Stack {
    fn default() -> Stack {
        let s = Stack {
            storage: Default::default(),
        };

        for e in 0..50000 {
            s.push(e);
        }

        s
    }
}

impl Dispatch for Stack {
    type Operation = Op;
    type Response = Option<u32>;

    fn dispatch(&self, op: Self::Operation) -> Self::Response {
        match op {
            Op::Push(v) => {
                self.push(v);
                return None;
            }
            Op::Pop => return self.pop(),
            Op::Invalid => unreachable!("Op::Invalid?"),
        }
    }
}

fn st_setup(nop: usize) -> Vec<Op> {
    let mut orng = thread_rng();
    let mut arng = thread_rng();

    let mut ops = Vec::with_capacity(nop);
    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 2usize {
            0usize => ops.push(Op::Pop),
            1usize => ops.push(Op::Push(arng.gen())),
            _ => ops.push(Op::Invalid),
        }
    }

    ops
}

/// A baseline stack without a log.
/// 
/// `cargo bench --bench stack -- stack/baseline --profile-time 5`
/// `autoperf profile -o stack-baseline /root/.cargo/bin/cargo bench --bench stack -- stack/baseline --profile-time 5`
fn stack_single_threaded(c: &mut Criterion) {
    let nop = 500000;
    let ops = st_setup(nop);
    let s: Stack = Default::default();

    let mut group = c.benchmark_group("stack");
    group.throughput(Throughput::Elements(nop as u64));
    group.bench_function("baseline", move |b| {
        b.iter(|| {
            for i in 0..nop {
                s.dispatch(ops[i]);
            }
        })
    });
    group.finish();
}

/// A baseline stack with a log.
/// 
/// `cargo bench --bench stack -- stack-log/baseline --profile-time 5`
/// `autoperf profile -o stack-baseline-log /root/.cargo/bin/cargo bench --bench stack -- stack-log/baseline --profile-time 5`
fn stack_single_threaded_with_log(c: &mut Criterion) {
    let nop = 500000;
    let ops = st_setup(nop);

    let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(
        5 * 1024 * 1024 * 1024,
    ));
    
    let r  = Replica::<Stack>::new(&log);
    let ridx = r.register().expect("Failed to register with Replica.");

    let mut group = c.benchmark_group("stack-log");
    group.throughput(Throughput::Elements(nop as u64));
    group.bench_function("baseline", move |b| {
        b.iter(|| {
            for i in 0..nop {
                r.execute(ops[i], ridx);
            }
        })
    });
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = stack_single_threaded, stack_single_threaded_with_log
);
criterion_main!(benches);
