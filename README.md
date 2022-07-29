# node-replication

This library can be used to implement a concurrent version of single threaded
data structures: It takes in a single threaded implementation of said data
structure, and scales it out to multiple cores and NUMA nodes by combining three
techniques: readers-writer locks, operation logging and flat combining.

## Implementations

Node replication converts a data structure to its NUMA-aware concurrent version.
The crate contains two variants of node-replication: NR for transforming
sequential data-structures and CNR for already concurrent or partitioned data
structures.

* [`nr`](node-replication/src/nr) converts a sequental data structure to its
  NUMA-aware concurrent version.
* [`cnr`](node-replication/src/cnr) converts a concurrent (or partitioned) data
  structure to its NUMA-aware concurrent version.

## Background

- The implementation of NR and a detailed evaluation can be found in this paper:
  [Black-box Concurrent Data Structures for NUMA
  Architectures](https://dl.acm.org/citation.cfm?id=3037721) which explains the
  NR library in more details.
- The implementation of CNR and a detailed evaluation can be found in this
  paper: [NrOS: Effective Replication and Sharing in an Operating
  System](https://www.usenix.org/conference/osdi21/presentation/bhardwaj). The
  [NrOS
  documentation](https://nrkernel.systems/book/architecture/NodeReplication.html)
  also has a brief overview of NR and CNR as these are the main concurrency
  mechanisms in the nrkernel.
- The NR code in this repo was [formally verified by porting it to
  Dafny](https://github.com/vmware-labs/verified-betrfs/tree/concurrency-experiments/concurrency/node-replication),
  including a proof that it ensures linearizability.

## How does it work (NR)

To replicate a single-threaded data structure, one needs to implement `Dispatch`
(from node-replication). As an example, we implement `Dispatch` for the
single-threaded HashMap from `std`.

```rust
#![feature(generic_associated_types)]
use std::collections::HashMap;
use node_replication::nr::Dispatch;

/// The node-replicated hashmap uses a std hashmap internally.
#[derive(Default)]
struct NrHashMap {
    storage: HashMap<u64, u64>,
}

/// We support mutable put operation on the hashmap.
#[derive(Clone, Debug, PartialEq)]
enum Modify {
    Put(u64, u64),
}

/// We support an immutable read operation to lookup a key from the hashmap.
#[derive(Clone, Debug, PartialEq)]
enum Access {
    Get(u64),
}

/// The Dispatch traits executes `ReadOperation` (our Access enum)
/// and `WriteOperation` (our `Modify` enum) against the replicated
/// data-structure.
impl Dispatch for NrHashMap {
    type ReadOperation<'rop> = Access;
    type WriteOperation = Modify;
    type Response = Option<u64>;

    /// The `dispatch` function applies the immutable operations.
    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
        match op {
            Access::Get(key) => self.storage.get(&key).map(|v| *v),
        }
    }

    /// The `dispatch_mut` function applies the mutable operations.
    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
        match op {
            Modify::Put(key, value) => self.storage.insert(key, value),
        }
    }
}
```

The full example (using `HashMap` as the underlying data-structure) can be found
[here](node-replication/examples/hashmap.rs).
To run, execute: `cargo run --example hashmap`

## How does it perform (NR)

The library often makes your single-threaded implementation work better than, or
competitive with fine-grained locking or lock free implementations of the same
data-structure.

It works especially well if

- Your data-structure exceeds the L3 cache-size of your system (you may not see
  any gain from replication if your data can always remain in the cache).
- All your threads need to issue mixed, mutable and immutable operations (if
  not alternative techniques like RCU may work better).
- You have enough DRAM to take advantage of the replication (i.e., it's
  typically best to use one replica per-NUMA node which means your original
  memory foot-print is multiplied with the amount of NUMA nodes in the system).

As an example, the following benchmark uses Rust' the hash-table with the
`Dispatch` implementation from above (`nr`), and compares it against concurrent
hash table implementations from [crates.io](https://crates.io) (chashmap,
dashmap, flurry), a HashMap protected by an `RwLock` (`std`), and
[urcu](https://liburcu.org/).

<table>
  <tr>
    <td valign="top"><a href="./graphs/skylake4x-throughput-vs-cores.png?raw=true">
    <img src="./graphs/skylake4x-throughput-vs-cores.png?raw=true" alt="Throughput of node-replicated HT" />
</a></td>
    <td valign="top"><a href="./graphs/skylake4x-throughput-vs-cores.png?raw=true">
    <img src="./graphs/skylake4x-throughput-vs-wr.png?raw=true" alt="Different write ratios with 196 threads" /></td>
  </tr>
</table>

The figures show a benchmark using hash tables pre-filled with 67M entires (8
byte keys and values) and uses a uniform key distribution for operations. On the
left graphs, different write ratios (0%, 10% and 80%) are shown. On the right
graph, we vary the write ratio (x-axis) with 192 threads. The system has 4 NUMA
nodes, so it uses 4 replicas (at `x=96`, a replica gets added every 24 cores).
After `x=96`, the remaining hyper-threads are used.

## Compile the library

The works with `no_std` and currently requires a nightly rust compiler.

```bash
cargo build
```

As it as a dependency in your `Cargo.toml`:

```toml
node-replication = "*"
```

The code should currently be treated as an early release and is still work in
progress.

## Testing

There are a range of unit tests as part of the implementation and a few
[integration tests](node-replication/tests) that check various aspects of the
implementations.

You can run the tests by executing: `cargo test`

## Benchmarks

The benchmarks (and how to execute them) are explained in more detail in the
[benches](node-replication/benches/README.md) folder.

## Supported Platforms

The code should be treated as an early release and is still work in progress. In
its current form, the library is only known to work on x86-64 platforms (other
platforms will require some changes and are untested).

## Contributing

The node-replication project team welcomes contributions from the community. If
you wish to contribute code and you have not signed our contributor license
agreement (CLA), our bot will update the issue when you open a Pull Request. For
any questions about the CLA process, please refer to our
[FAQ](https://cla.vmware.com/faq).