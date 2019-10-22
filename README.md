# node-replication

[![pipeline status](https://gitlab.com/gz1/node-replication/badges/master/pipeline.svg)](https://gitlab.com/gz1/node-replication/commits/master)

Rust implementation of Node Replication. [Black-box Concurrent Data Structures
for NUMA Architectures](https://dl.acm.org/citation.cfm?id=3037721) published
at ASPLOS 2017

This library can be used to implement a concurrent version of any single
threaded data structure. It takes in a single threaded implementation of said
data structure, and scales it out to multiple cores and NUMA nodes using an
operation log.

The code should be treated as experimental and work in progress, there may be
correctness and performance bugs.

## Compiling

The library should compile with a stable rust compiler. The code supports
`no_std` as well.

The following system libraries are required if you want to run the
examples/benchmarks:

```
$ apt-get install libhwloc-dev libfreetype6 libfreetype6-dev gnuplot numactl libfuse-dev
```

Running the tests require the use of a nightly rust compiler:
```
rustup toolchain install nightly
rustup default nightly
```

## Testing

There are a series of unit tests as part of the implementation and a few
[integration tests](./tests) that verify the correctness of the implementation
using a stack.

You can run the tests by executing: `cargo test`

## Examples

### Stack [[src](examples/stack.rs)]

A working example of a replicated stack.
To run the example execute: `cargo run --example stack`

## Benchmarks

Executing `cargo bench` will run many benchmarks (described in more detail in
the following subsections) to evaluate the performance of the library. The code
is located in the `benches` folder. We use
[criterion](https://crates.io/crates/criterion) as a harness runner for
benchmarks. After execution, the summary of the results are best viewed in a
browser by opening `target/criterion/report/index.html`.

Please ensure to always set `RUST_TEST_THREADS=1` in your environment for
benchmarking since the scale-out benchmarks will spawn multiple threads
internally that can utilize the entire machine for certain runs.

### Log append [[data-structure](benches/nop.rs)] [[runner](benches/criterion.rs)]

A benchmark that evaluates the append performance (in terms of throughput
ops/s) of the log by varying the batch size and the amount of threads
contending on the log. This gives you the maximum of operations that are
theoretically possible (if we are ignoring the overhead of synchronization
within a replica).

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench criterion -- log`

### Stack [[data-structure](benches/stack.rs)] [[runner](benches/criterion.rs)]

One benchmark that evaluates the COST (overhead of added synchronization) by
comparing a node-replicated stack against a single-threaded stack (without a
log/replica), and a benchmark that evaluates the scalability of the stack by
running with increasing amounts of threads.

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench criterion -- stack`

### Hash-map [[data-structure](benches/hashmap.rs)] [[runner](benches/criterion.rs)]

A benchmark that evaluates the COST (overhead of added synchronization) by
comparing a node-replicated hash-map against a single-threaded hash-map (without a
log/replica), and a benchmark that evaluates the scalability of the hash-map by
running with increasing amounts of threads.

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench criterion -- hashmap`

### Synthetic data-structure [[data-structure](benches/synthetic.rs)] [[runner](benches/criterion.rs)]

A benchmark to evaluates the performance of the NR library by using a
configurable cache-model and access-pattern for an abstract data-structure that
is replicated by the library.

Again the code contains two sets of benchmarks to (a) compare the overhead of
added synchronization and (b) evaluate the scalability of the synthetic
data-structure. All benchmarks report throughput in ops/s.

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench criterion -- synthetic`

Example: To run a specific benchmark of the synthetic data-structure with 8 threads
(using one replica, sequential thread mapping, and batch size 1):

`RUST_TEST_THREADS=1 cargo bench --bench criterion -- 'synthetic-scaleout/RS=System TM=Sequential BS=1/8'`

### VSpace [[src](benches/vspace.rs)]

A benchmark to evaluate the performance of the NR library for address-space
replication (using an x86-64 4-level address space layout).

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench vspace`

### MemFS [[src](benches/memfs.rs)]

A benchmark to evaluate the performance of the NR library for file-system like
operations, by using a very simple in-memory file-system ([btfs](https://crates.io/crates/btfs)).

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench memfs`

### NRbench [[src](benches/nrbench.rs)]

NRbench is a CLI tool that can quickly run a benchmark (stack or synthetic)
configured by command-line parameters.

Use `RUST_TEST_THREADS=1 cargo bench --bench nrbench -- --help` to see an
overview of supported configuration.

### Hashbench [[src](benches/hashbench.rs)]

A benchmark to compare various concurrent Hashtables (originally
from [evmap](https://github.com/jonhoo/rust-evmap)).

Use `RUST_TEST_THREADS=1 cargo bench --bench hashbench -- --help` to see an
overview of supported configuration.