# node-replication

Node Replication library based on [Black-box Concurrent Data Structures for NUMA
Architectures](https://dl.acm.org/citation.cfm?id=3037721).

This library can be used to implement a concurrent version of *any* single
threaded data structure: It takes in a single threaded implementation of said
data structure, and scales it out to multiple cores and NUMA nodes by combining
two techniques: operation logging and flat combining.

The code should currently be treated as an early release and is still work in
progress. In it's current form, the library is only known to work on x86
platforms (other platforms will require some changes and are untested).

## Compile the library

The library currently requires a nightly rust compiler (due to the use of
`atomic_min_max`, `new_uninit`, and `get_mut_unchecked` APIs). The library works
with `no_std`.

```bash
cargo build
```

As a dependency in your `Cargo.toml`:

```toml
node-replication = "*"
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

## Testing

There are a series of unit tests as part of the implementation and a few
[integration tests](./tests) that verify the correctness of the implementation
using a stack.

You can run the tests by executing: `cargo test`

## Benchmarks

The benchmarks and how to execute them are explained with more detail in the
[[benches](benches/README.md)] folder.

## Examples

### Stack [[src](examples/stack.rs)]

A working example of a replicated stack. To run the example execute: `cargo run
--example stack`
