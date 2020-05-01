# Benchmarks

Executing `cargo bench` will run many benchmarks (described in more detail in
the following subsections) to evaluate the performance of the library. The code
is located in the `benches` folder.

Please ensure to always set `RUST_TEST_THREADS=1` in your environment for
benchmarking since the scale-out benchmarks will spawn multiple threads
internally that can utilize the entire machine for certain runs.

Note: Running all benchmarks may take hours, depending on the system!

## Log append [[benchmark](benches/log.rs)]

A benchmark that evaluates the append performance (in terms of throughput
ops/s) of the log by varying the batch size and the amount of threads
contending on the log. This gives you the maximum of operations that are
theoretically possible (if we are ignoring the overhead of synchronization
within a replica).

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench log`

## Stack [[benchmark](benches/stack.rs)]

One benchmark that evaluates the COST (overhead of added synchronization) by
comparing a node-replicated stack against a single-threaded stack (without a
log/replica), and a benchmark that evaluates the scalability of the stack by
running with increasing amounts of threads.

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench stack`

## Hash-map [[benchmark](benches/hashmap.rs)]

A benchmark that evaluates the COST (overhead of added synchronization) by
comparing a node-replicated hash-map against a single-threaded hash-map (without a
log/replica), and a benchmark that evaluates the scalability of the hash-map by
running with increasing amounts of threads.

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench hashmap`

## Synthetic data-structure [[benchmark](benches/synthetic.rs)]

A benchmark to evaluates the performance of the NR library by using a
configurable cache-model and access-pattern for an abstract data-structure that
is replicated by the library.

Again the code contains two sets of benchmarks to (a) compare the overhead of
added synchronization and (b) evaluate the scalability of the synthetic
data-structure. All benchmarks report throughput in ops/s.

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench synthetic`

## VSpace [[benchmark](benches/vspace.rs)]

A benchmark to evaluate the performance of the NR library for address-space
replication (using an x86-64 4-level address space layout).

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench vspace`

## MemFS [[benchmark](benches/memfs.rs)]

A benchmark to evaluate the performance of the NR library for file-system like
operations, by using a very simple in-memory file-system ([btfs](https://crates.io/crates/btfs)).

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench memfs`

## Hashbench [[benchmark](benches/hashbench.rs)]

A benchmark to compare various concurrent Hashtables (originally
from [evmap](https://github.com/jonhoo/rust-evmap)).

Use `RUST_TEST_THREADS=1 cargo bench --bench hashbench -- --help` to see an
overview of supported configuration.
