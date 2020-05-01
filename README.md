# node-replication

[![pipeline status](https://gitlab.com/gz1/node-replication/badges/master/pipeline.svg)](https://gitlab.com/gz1/node-replication/commits/master)

This is a rust implementation of Node Replication. [Black-box Concurrent Data
Structures for NUMA Architectures](https://dl.acm.org/citation.cfm?id=3037721).

This library can be used to implement a concurrent version of *any* single
threaded data structure: It takes in a single threaded implementation of said
data structure, and scales it out to multiple cores and NUMA nodes by combining
two techniques: operation logging and flat combining.

The code should currently be treated as an early release and is still work in
progress, there may be correctness and performance bugs. In it's current form,
the library is only known to work on x86 platforms (other platforms will require
some changes and are untested).

## Compile the library

The library should compile with a stable rust compiler. The code supports
`no_std` as well.

```bash
cargo build
```

As a dependency in your Cargo.toml:

```toml
node-replication = "0.0.7"
```

### Compile benchmark and tests

Running the tests, examples and benchmarks requires the use of a nightly rust
compiler:

```bash
rustup toolchain install nightly
rustup default nightly
```

In addition the a few system libraries are required to be present on your
system.

#### Install Benchmark/Test dependencies Ubuntu

```bash
apt-get install libhwloc-dev gnuplot pkg-config libfuse-dev liburcu-dev liburcu6 clang r-base r-cran-plyr r-cran-ggplot2
```

If you are on Ubuntu 19.04 or older you need to get a newer version of URCU:

```bash
wget http://mirrors.kernel.org/ubuntu/pool/main/libu/liburcu/liburcu6_0.11.1-2_amd64.deb
wget http://mirrors.kernel.org/ubuntu/pool/main/libu/liburcu/liburcu-dev_0.11.1-2_amd64.deb
dpkg -i liburcu-dev_0.11.1-2_amd64.deb liburcu6_0.11.1-2_amd64.deb
```

#### Install Benchmark/Test dependencies on MacOS

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

The benchmarks are explained with more detail in the [[benches](benches/README.md)] folder.
