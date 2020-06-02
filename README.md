# node-replication

Node Replication library based on [Black-box Concurrent Data Structures for NUMA
Architectures](https://dl.acm.org/citation.cfm?id=3037721).

This library can be used to implement a concurrent version of any single
threaded data structure: It takes in a single threaded implementation of said
data structure, and scales it out to multiple cores and NUMA nodes by combining
three techniques: readers-writer locks, operation logging and flat combining.

## How does it work

To replicate a single-threaded data structure, one needs to implement `Dispatch`
(from node-replication). As an example, we implement `Dispatch` for the
single-threaded HashMap from `std`.

```rust
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
    type ReadOperation = Access;
    type WriteOperation = Modify;
    type Response = Option<u64>;

    /// The `dispatch` function applies the immutable operations.
    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
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
[here](examples/hashmap.rs). To run, execute: `cargo run --example hashmap`

## How does it perform

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
    <td valign="top"><a href="/benches/graphs/skylake4x-throughput-vs-cores.png?raw=true">
    <img src="/benches/graphs/skylake4x-throughput-vs-cores.png?raw=true" alt="Throughput of node-replicated HT" />
</a></td>
    <td valign="top"><a href="/benches/graphs/skylake4x-throughput-vs-cores.png?raw=true">
    <img src="/benches/graphs/skylake4x-throughput-vs-wr.png?raw=true" alt="Different write ratios with 196 threads" /></td>
  </tr>
</table>

The figures show a benchmark using hash tables pre-filled with 67M entires (8
byte keys and values) and uses a uniform key distribution for operations. On the
left graphs, different write ratios (0%, 10% and 80%) are shown. On the right
graph, we vary the write ratio (x-axis) with 192 threads. The system has 4 NUMA
nodes, so it uses 4 replicas (at `x=96`, a replica gets added every 24 cores).
After `x=96`, the remaining hyper-threads are used.

## Compile the library

The library currently requires a nightly rust compiler (due to the use of
`atomic_min_max`, `new_uninit`, and `get_mut_unchecked`, `negative_impls` API).
The library works with `no_std`.

```bash
rustup toolchain install nightly
rustup default nightly
cargo build
```

As a dependency in your `Cargo.toml`:

```toml
node-replication = "*"
```

The code should currently be treated as an early release and is still work in
progress. In its current form, the library is only known to work on x86
platforms (other platforms will require some changes and are untested).

## Testing

There are a series of unit tests as part of the implementation and a few
[integration tests](./tests) that check various aspects of the implementation
using a stack.

You can run the tests by executing: `cargo test`

## Benchmarks

The benchmarks (and how to execute them) are explained in more detail in the
[benches](benches/README.md) folder.
