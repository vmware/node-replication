#!/bin/bash
set -ex

cd bench_utils
cargo fmt -- --check
#cargo clippy -- -D warnings
cd ..

cd node-replication
cargo fmt -- --check
cargo clippy -- -D warnings

cargo build
cargo test --release
cargo doc

cargo run --example nr_async_hashmap
cargo run --example nr_btreeset
cargo run --example nr_hashmap
cargo run --example nr_stack
cargo run --example cnr_hashmap
cargo run --example cnr_stack


cargo bench --bench log --features smokebench
cargo bench --bench hashmap --features smokebench
cargo bench --bench synthetic --features smokebench
cargo bench --bench stack --features smokebench
cargo bench --bench vspace --features smokebench
cargo bench --bench lockfree --features smokebench
# Currently fails: cargo bench --bench nrfs  --features smokebench

cargo bench --bench hashbench -- --readers 1 --writers 1
cargo bench --bench rwlockbench -- --readers 1 --writers 1
cargo bench --bench chashbench -- --readers 1 --writers 1



