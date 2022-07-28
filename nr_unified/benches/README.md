# Benchmarks

This folder contains a few different benchmarks to evaluate the performance of
both [nr](../src/nr) and [cnr](../src/cnr). They are described in more detail in
the following subsections.

## Benchmark feature flags

- `smokebench`:  You can pass `--features smokebench` to run for a shorter
  duration and with a smaller working set.

- `cmp`: This will do comparisons against other, related implementations

- `exhaustive`: This will run very exhaustive parameter sweeps. *Be aware*:
  Running in this mode can take hours, depending on the system and will require
  lots of RAM (> 20 GiB).

## Install Benchmark/Test dependencies on Ubuntu

```bash
apt-get install libhwloc-dev gnuplot pkg-config libfuse-dev liburcu-dev liburcu6 clang r-base r-cran-plyr r-cran-ggplot2
```

If you are on Ubuntu 19.04 or older you need to get a newer version of URCU:

```bash
wget http://mirrors.kernel.org/ubuntu/pool/main/libu/liburcu/liburcu6_0.11.1-2_amd64.deb
wget http://mirrors.kernel.org/ubuntu/pool/main/libu/liburcu/liburcu-dev_0.11.1-2_amd64.deb
sudo dpkg -i liburcu-dev_0.11.1-2_amd64.deb liburcu6_0.11.1-2_amd64.deb
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

## NR benchmarks

### Log append [[benchmark](log.rs)]

A benchmark that evaluates the append performance (in terms of throughput ops/s)
of the log by varying the batch size and the amount of threads contending on the
log. This gives you an upper bound on possible throughput (every thread
represents a replica that just inserts) since it ignores the overhead of
synchronization within a replica.

To run these benchmarks execute:
`cargo bench --bench log`

### Stack [[benchmark](stack.rs)]

One benchmark that evaluates the COST (overhead of added synchronization) by
comparing a node-replicated stack against a single-threaded stack (without a
log/replica), and a benchmark that evaluates the scalability of the stack by
running with increasing amounts of threads.

A stack data-structure normally has a low cache footprint cache-miss rate. So
this is a scenario where CNR/NR replication often does not help much.

To run these benchmarks execute:
`cargo bench --bench stack`

### Hash-map [[benchmark](hashmap/main.rs)]

A benchmark that evaluates the COST (overhead of added synchronization) by
comparing a node-replicated hash-map against a single-threaded hash-map (without
a log/replica), and a benchmark that evaluates the scalability of the hash-map
by running with increasing amounts of threads.

Large hash-tables have bad cache hit rates and many random DRAM accesses, here
NR/CNR replication works well.

To run these benchmarks execute:
`cargo bench --bench hashmap`

### Synthetic data-structure [[benchmark](synthetic.rs)]

A benchmark to evaluates the performance of the NR library by using a
configurable cache-model and access-pattern for an abstract data-structure that
is replicated by the library.

Again the code contains two sets of benchmarks to (a) compare the overhead of
added synchronization and (b) evaluate the scalability of the synthetic
data-structure. All benchmarks report throughput in ops/s.

To run these benchmarks execute:
`cargo bench --bench synthetic`

### VSpace [[benchmark](vspace.rs)]

A benchmark to evaluate the performance of the NR library for address-space
replication (using an x86-64 4-level address space layout, radix-tree). This is
a benchmark that was externalized from the
[nrkernel](https://github.com/vmware-labs/node-replicated-kernel).

To run these benchmarks execute:
`cargo bench --bench vspace`

### Hashbench [[benchmark](hashbench)]

A benchmark to compare various concurrent Hashtables (originally taken from
[evmap](https://github.com/jonhoo/rust-evmap) and extended with NR).

- Use `cargo bench --bench hashbench -- --help` for an overview of supported
  command line arguments
- Or, invoke the `hashbench_run.sh` script

### RWLock bench [[benchmark](rwlockbench)]

A benchmark to measure the performance of the readers-writer lock implementation
used by NR.

- Use `cargo bench --bench rwlockbench -- --help` for an overview of supported
  command line arguments
- Or, invoke the `rwlockbench_run.sh` script

## CNR benchmarks

### Lockfree [[benchmark](lockfree)]

Compares CNR against lock-free and partitioned data-structrues.

To run these benchmarks execute:
`cargo bench --bench lockfree`

### NrFS [[benchmark](nrfs.rs)]

A benchmark program to prototype the in-memory FS implementation of the
[nrkernel](https://github.com/vmware-labs/node-replicated-kernel) in user-space.

To run these benchmarks execute:
`cargo bench --bench nrfs`

### chashbench [[benchmark](chashbench.rs)]

Similar to hashbench for NR, but runs CNR code with chashmap.

- `cargo bench --bench chashbench`
