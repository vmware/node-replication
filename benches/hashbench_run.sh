#!/bin/bash
set -ex

cargo build --release
rm results.log || true
for w in 0 1 2 4 8 14 16 24; do
  for d in uniform skewed; do
    for r in 0 1 2 4 8 14 16 24; do
      if [[ $w > 0 ]] || [[ $r > 0 ]] ; then
        RUST_TEST_THREADS=1 cargo bench --bench hashbench -- -r $r -w $w -d $d -c | tee -a results.log;
      fi
    done;
  done;
done

R -q --no-readline --no-restore --no-save < hashbench_plot.r