# node-replication

Node Replication library based on [Black-box Concurrent Data Structures for NUMA
Architectures](https://dl.acm.org/citation.cfm?id=3037721).

This library can be used to implement a concurrent version of any single
threaded data structure: It takes in a single threaded implementation of said
data structure, and scales it out to multiple cores and NUMA nodes by combining
two techniques: operation logging and flat combining.

## How does it work

To replicate a single-threaded data structure, one needs to implement `Dispatch`
(from node-replication). The full example (using `Vec` as the underlying
data-structure) can be found here: [[src](examples/stack.rs)]. To run, execute:
`cargo run --example stack`

```rust
/// The actual stack, it uses a single-threaded Vec.
struct Stack {
    storage: Vec<u32>,
}

/// We support mutable push and pop operations on the stack.
enum Modify {
    Push(u32),
    Pop,
}

/// We support an immutable read operation to peek the stack.
enum Access {
    Peek,
}

/// The Dispatch traits executes `ReadOperation` (our Access enum)
/// and `WriteOperation` (our `Modify` enum) against the replicated
/// data-structure.
impl Dispatch for Stack {
    type ReadOperation = Access;
    type WriteOperation = Modify;
    type Response = u32;
    type ResponseError = ();

    /// The `dispatch` function applies the immutable operations.
    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            Access::Peek => self.storage.last().cloned().ok_or(()),
        }
    }

    /// The `dispatch_mut` function applies the mutable operations.
    fn dispatch_mut(
        &mut self,
        op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        match op {
            Modify::Push(v) => {
                self.storage.push(v);
                return Ok(v);
            }
            Modify::Pop => return self.storage.pop().ok_or(()),
        }
    }
}
```

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

As an example, the following benchmark uses Rust's single-threaded hash table
from `std` with node-replication and compares it against concurrent hash table
implementations from crates.io.

![Throughput of node-replication](/benches/graphs/skylake4x-throughput-vs-cores.png?raw=true "Throughput with increasing cores")
![Write ratios with max-cores](/benches/graphs/skylake4x-throughput-vs-wr.png?raw=true "Different write ratios with 196 threads")

The figures show a benchmark using hash tables pre-filled with 67M entires (8
byte keys and values) and uses a uniform key distribution for operations. On the
first graph, the write ratios 0%, 10% and 80% are shown, and the second graph
shows different write ratios (x-axis) with 196 threads. The system has 4 NUMA
nodes, so it uses 4 replicas (at `x=96`, a replica gets added every 24 cores).
After `x=96` hyper-threads are used.

## Compile the library

The library currently requires a nightly rust compiler (due to the use of
`atomic_min_max`, `new_uninit`, and `get_mut_unchecked` API). The library works
with `no_std`.

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
[integration tests](./tests) that verify the correctness of the implementation
using a stack.

You can run the tests by executing: `cargo test`

## Benchmarks

The benchmarks (and how to execute them) are explained in more detail in the
[[benches](benches/README.md)] folder.
