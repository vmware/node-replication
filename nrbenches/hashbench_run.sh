#!/bin/bash
set -ex

cargo build --release
rm results.log || true
for w in 1 4 8 16; do
  for d in uniform skewed; do
    for r in 1 4 8 16; do
      if [[ $w > 0 ]] || [[ $r > 0 ]] ; then
        RUST_TEST_THREADS=1 cargo bench --bench hashbench --features="nr" -- -r $r -w $w -d $d | tee -a results.log;
      fi
    done;
  done;
done

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
R -q --no-readline --no-restore --no-save < $SCRIPT_DIR/hashbench_plot.r