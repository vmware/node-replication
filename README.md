# node-replication

Node Replication library based on [Black-box Concurrent Data Structures for NUMA
Architectures](https://dl.acm.org/citation.cfm?id=3037721).

This library can be used to implement an optimized concurrent version of a data structure.
It takes the given data structure, and scales it out to multiple cores and NUMA nodes by combining
three techniques: readers-writer locks, operation logging and flat combining.

## Crates

Node Replication library converts a data structure to its NUMA-aware concurrent version.
The library achieves this differently for sequential and concurrent data structures. This
repository contains two crates: one for sequential and one for concurrent data structures.

* [`node-replication`](nr) converts a sequental data structure to its NUMA-aware concurrent version.
* [`cnr`](cnr) converts a concurrent data structure to its NUMA-aware concurrent version.

## Supported Platforms
Both the crates currently requires a nightly rust compiler (due to the use of
`new_uninit`, and `get_mut_unchecked`, `negative_impls` API) and works
with `no_std`.

```bash
rustup toolchain install nightly
rustup default nightly
```

The code should currently be treated as an early release and is still work in
progress. In its current form, the library is only known to work on x86
platforms (other platforms will require some changes and are untested).

## Benchmarks
The benchmarks (and how to execute them) are explained in more detail in the
[benches](./benches/README.md) folder.

## Contributing

The node-replication project team welcomes contributions from the community. If
you wish to contribute code and you have not signed our contributor license
agreement (CLA), our bot will update the issue when you open a Pull Request. For
any questions about the CLA process, please refer to our
[FAQ](https://cla.vmware.com/faq).