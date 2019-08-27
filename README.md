# node-replication
Rust implementation of Node Replication. [Black-box Concurrent Data Structures for NUMA Architectures](https://dl.acm.org/citation.cfm?id=3037721) published at ASPLOS 2017

This library can be used to implement a concurrent version of any single threaded data structure. It takes in a single threaded implementation of said data structure, and
scales it out to multiple cores and NUMA nodes using an operation log.

The code should be treated as experimental and work in progress, there may be correctness and performance bugs.

## Testing
Unit tests can be run by executing the command `cargo test --lib`

## Benchmarks
`cargo bench` will run several benchmarks evaluating the performance of the log. The code is located in the `benches` folder.
The benchmark results are located in `target/criterion/report/index.html`.

## Examples

### Stack
A working example of a replicated stack can be found under `examples/stack/`

To run the stack example with 1-2 threads, each performing 800'000 push and pop operations invoke:
`cargo run --release --example stack -- -t1,2 --nop 800000 -l 2`

For a detailled explanation of the program parameters use `-h`.
