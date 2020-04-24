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

## Compile the library

The library should compile with a stable rust compiler. The code supports
`no_std` as well.

```
$ cargo build
```

As a dependency in your Cargo.toml:

```bash
node-replication = "0.0.7"
```

### Compile benchmark and tests

Running the tests, examples and benchmarks requires the use of a nightly rust
compiler:

```
rustup toolchain install nightly
rustup default nightly
```

In addition the a few system libraries are required to be present on your
system.

#### Install Benchmark/Test dependencies Ubuntu

```bash
$ apt-get install libhwloc-dev gnuplot pkg-config libfuse-dev liburcu-dev liburcu6 clang r-base r-cran-plyr r-cran-ggplot2
```

If you are on Ubuntu 19.04 or older you need to get a newer version of URCU:

```bash
wget http://mirrors.kernel.org/ubuntu/pool/main/libu/liburcu/liburcu6_0.11.1-2_amd64.deb
wget http://mirrors.kernel.org/ubuntu/pool/main/libu/liburcu/liburcu-dev_0.11.1-2_amd64.deb
dpkg -i liburcu-dev_0.11.1-2_amd64.deb liburcu6_0.11.1-2_amd64.deb
```

#### Install Benchmark/Test dependencies on MacOS

```bash
$ brew install gnuplot hwloc
```

hashbench compares against URCU (user-space RCU). For MacOS the easiest
way is to install it from the sources:

```bash
$ git clone git://git.liburcu.org/userspace-rcu.git
$ userspace-rcu
$ git checkout v.0.11.0
$ ./bootstrap
$ ./configure --build=x86_64-apple-darwin11
$ make
$ make install
```

The memfs benchmarks depends on btfs which uses FUSE. The easiest way to
install FUSE for MacOS is through downloading the packages on
[osxfuse](https://osxfuse.github.io/).

## Testing

There are a series of unit tests as part of the implementation and a few
[integration tests](./tests) that verify the correctness of the implementation
using a stack.

You can run the tests by executing: `cargo test`

## Examples

### Stack [[src](examples/stack.rs)]

A working example of a replicated stack. To run the example execute: `cargo run
--example stack`

## Benchmarks

Executing `cargo bench` will run many benchmarks (described in more detail in
the following subsections) to evaluate the performance of the library. The code
is located in the `benches` folder.

Please ensure to always set `RUST_TEST_THREADS=1` in your environment for
benchmarking since the scale-out benchmarks will spawn multiple threads
internally that can utilize the entire machine for certain runs.

Note: Running all benchmarks may take hours, depending on the system!

### Log append [[benchmark](benches/log.rs)]

A benchmark that evaluates the append performance (in terms of throughput
ops/s) of the log by varying the batch size and the amount of threads
contending on the log. This gives you the maximum of operations that are
theoretically possible (if we are ignoring the overhead of synchronization
within a replica).

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench log`

### Stack [[benchmark](benches/stack.rs)]

One benchmark that evaluates the COST (overhead of added synchronization) by
comparing a node-replicated stack against a single-threaded stack (without a
log/replica), and a benchmark that evaluates the scalability of the stack by
running with increasing amounts of threads.

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench stack`

### Hash-map [[benchmark](benches/hashmap.rs)]

A benchmark that evaluates the COST (overhead of added synchronization) by
comparing a node-replicated hash-map against a single-threaded hash-map (without a
log/replica), and a benchmark that evaluates the scalability of the hash-map by
running with increasing amounts of threads.

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench hashmap`

### Synthetic data-structure [[benchmark](benches/synthetic.rs)]

A benchmark to evaluates the performance of the NR library by using a
configurable cache-model and access-pattern for an abstract data-structure that
is replicated by the library.

Again the code contains two sets of benchmarks to (a) compare the overhead of
added synchronization and (b) evaluate the scalability of the synthetic
data-structure. All benchmarks report throughput in ops/s.

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench synthetic`

### VSpace [[benchmark](benches/vspace.rs)]

A benchmark to evaluate the performance of the NR library for address-space
replication (using an x86-64 4-level address space layout).

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench vspace`

### MemFS [[benchmark](benches/memfs.rs)]

A benchmark to evaluate the performance of the NR library for file-system like
operations, by using a very simple in-memory file-system ([btfs](https://crates.io/crates/btfs)).

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench memfs`

### Hashbench [[benchmark](benches/hashbench.rs)]

A benchmark to compare various concurrent Hashtables (originally
from [evmap](https://github.com/jonhoo/rust-evmap)).

Use `RUST_TEST_THREADS=1 cargo bench --bench hashbench -- --help` to see an
overview of supported configuration.
