# node-replication
Rust implementation of Node Replication. [Black-box Concurrent Data Structures for NUMA Architectures](https://dl.acm.org/citation.cfm?id=3037721) published at ASPLOS 2017

This library can be used to implement a concurrent version of any single threaded data structure. It takes in a single threaded implementation of said data structure, and
scales it out to multiple cores and NUMA nodes using an operation log.

# Testing
Unit tests can be run by executing the command `cargo test --lib`

# Example
A working example of a stack can be found under `examples/stack.rs`

To run the example with one and two threads each you can pass (for a detailled explanation of the parameters use `-h`):
`RUST_LOG="stack=info" cargo run --release --example stack -- -r socket -m sequential -t1,2 --nop 800000 -l 2`