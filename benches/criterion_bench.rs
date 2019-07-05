#[macro_use]
extern crate criterion;

use criterion::black_box;
use criterion::Criterion;

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n - 1) + fibonacci(n - 2),
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum Opcode {
    Get,
    Set,
}

impl Default for Opcode {
    fn default() -> Opcode {
        Opcode::Get
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq)]
struct Params(u64);

#[derive(Default)]
struct Dispatcher;
impl node_replication::Dispatch for Dispatcher {
    fn dispatch<T, P>(&self, op: T, params: P) {}
}

fn node_replication_benchmark(c: &mut Criterion) {
    let log = node_replication::log::Log::<Opcode, Params>::new(1024);
    let repl = node_replication::replica::Replica::<Opcode, Params, Dispatcher>::new(&log);

    c.bench_function("sillytest", |b| {
        b.iter_custom(|iters| {
            let barrier = Arc::new(Barrier::new(10));
            let mut handles = Vec::new();

            // Spawn some load generator threads
            for _ in 0..9 {
                let c = barrier.clone();
                handles.push(thread::spawn(move || {
                    c.wait();
                    for i in 0..iters {
                        black_box(fibonacci(black_box(20)));
                    }
                }));
            }

            let c = barrier.clone();
            c.wait(); // Wait for other threads to finish

            // Measure on our base thread
            let start = Instant::now();
            for i in 0..iters {
                black_box(fibonacci(black_box(20)));
            }
            let elapsed = start.elapsed();

            // Wait for other threads to finish
            for handle in handles {
                handle.join().unwrap();
            }

            return elapsed;
        })
    });
}

criterion_group!(benches, node_replication_benchmark);
criterion_main!(benches);
