# node-replication

Node Replication library based on [Black-box Concurrent Data Structures for NUMA
Architectures](https://dl.acm.org/citation.cfm?id=3037721).

This library can be used to implement an optimized, concurrent version of a data
structure. It takes the given data structure, and scales it out to multiple
cores and NUMA nodes by combining three techniques: readers-writer locks,
operation logging and flat combining.

## Crates

Node replication converts a data structure to its NUMA-aware concurrent version.
This repository contains two crates: one for transforming sequential
data-structures and one for already concurrent or partitioned data structures.

* [`node-replication`](nr) converts a sequental data structure to its NUMA-aware
  concurrent version.
* [`cnr`](cnr) converts a concurrent (or partitioned) data structure to its
  NUMA-aware concurrent version.

## Supported Platforms

The code should be treated as an early release and is still work in progress. In
its current form, the library is only known to work on x86-64 platforms (other
platforms will require some changes and are untested).

## Benchmarks

Benchmarks (and how to execute them) are explained in more detail in the
[benches](./benches/README.md) folder.

## Contributing

The node-replication project team welcomes contributions from the community. If
you wish to contribute code and you have not signed our contributor license
agreement (CLA), our bot will update the issue when you open a Pull Request. For
any questions about the CLA process, please refer to our
[FAQ](https://cla.vmware.com/faq).