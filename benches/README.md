# Benchmarks

This folder contains a few different benchmarks to evaluate the performance of
the library. They are described in more detail in the following subsections.

Please ensure to always set `RUST_TEST_THREADS=1` in your environment for
benchmarking since the scale-out benchmarks will spawn multiple threads
internally that can utilize the entire machine for certain runs.

*Be aware*: Running all benchmarks can take hours, depending on the system and
will require lots of RAM (> 20 GiB). You can pass `--features smokebench` to run
for a shorter duration and with a smaller working set.

## Install Benchmark/Test dependencies on Ubuntu

```bash
apt-get install libhwloc-dev gnuplot pkg-config libfuse-dev liburcu-dev liburcu6 clang r-base r-cran-plyr r-cran-ggplot2
```

If you are on Ubuntu 19.04 or older you need to get a newer version of URCU:

```bash
wget http://mirrors.kernel.org/ubuntu/pool/main/libu/liburcu/liburcu6_0.11.1-2_amd64.deb
wget http://mirrors.kernel.org/ubuntu/pool/main/libu/liburcu/liburcu-dev_0.11.1-2_amd64.deb
dpkg -i liburcu-dev_0.11.1-2_amd64.deb liburcu6_0.11.1-2_amd64.deb
```

## Install Benchmark/Test dependencies on MacOS

```bash
brew install gnuplot hwloc
```

hashbench compares against URCU (user-space RCU). For MacOS the easiest
way is to install it from the sources:

```bash
git clone git://git.liburcu.org/userspace-rcu.git
userspace-rcu
git checkout v.0.11.0
./bootstrap
./configure --build=x86_64-apple-darwin11
make
make install
```

The `memfs` benchmarks depends on btfs which uses FUSE. The easiest way to
install FUSE for MacOS is through downloading the packages on
[osxfuse](https://osxfuse.github.io/).

## Log append [[benchmark](log.rs)]

A benchmark that evaluates the append performance (in terms of throughput ops/s)
of the log by varying the batch size and the amount of threads contending on the
log. This gives you the maximum of operations that are theoretically possible
(if we are ignoring the overhead of synchronization within a replica).

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench log`

## Stack [[benchmark](stack.rs)]

One benchmark that evaluates the COST (overhead of added synchronization) by
comparing a node-replicated stack against a single-threaded stack (without a
log/replica), and a benchmark that evaluates the scalability of the stack by
running with increasing amounts of threads.

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench stack`

## Hash-map [[benchmark](hashmap.rs)]

A benchmark that evaluates the COST (overhead of added synchronization) by
comparing a node-replicated hash-map against a single-threaded hash-map (without
a log/replica), and a benchmark that evaluates the scalability of the hash-map
by running with increasing amounts of threads.

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench hashmap`

## Synthetic data-structure [[benchmark](synthetic.rs)]

A benchmark to evaluates the performance of the NR library by using a
configurable cache-model and access-pattern for an abstract data-structure that
is replicated by the library.

Again the code contains two sets of benchmarks to (a) compare the overhead of
added synchronization and (b) evaluate the scalability of the synthetic
data-structure. All benchmarks report throughput in ops/s.

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench synthetic`

## VSpace [[benchmark](vspace.rs)]

A benchmark to evaluate the performance of the NR library for address-space
replication (using an x86-64 4-level address space layout).

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench vspace`

## MemFS [[benchmark](memfs.rs)]

A benchmark to evaluate the performance of the NR library for file-system like
operations, by using a very simple in-memory file-system
([btfs](https://crates.io/crates/btfs)).

To run these benchmarks execute:
`RUST_TEST_THREADS=1 cargo bench --bench memfs`

## Hashbench [[benchmark](hashbench.rs)]

A benchmark to compare various concurrent Hashtables (originally
from [evmap](https://github.com/jonhoo/rust-evmap)).

Use `RUST_TEST_THREADS=1 cargo bench --bench hashbench -- --help` to see an
overview of supported configuration or check the `hashbench_run.sh` script.

## RWLock bench [[benchmark](rwlockbench.rs)]

A benchmark to measure the performance of the readers-writer lock
implementation.

Use `RUST_TEST_THREADS=1 cargo bench --bench rwlockbench -- --help` to see an
overview of supported configuration or check the `rwlockbench_run.sh` script.