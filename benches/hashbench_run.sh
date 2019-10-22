#!/bin/bash
set -ex

cargo build --release
rm results.log || true
for w in 0 1 8 16 24; do
  for d in uniform skewed; do
    for r in 0 1 8 16 24; do
      if [[ $w > 0 ]] || [[ $r > 0 ]] ; then
        RUST_TEST_THREADS=1 cargo bench --bench hashbench -- -r $r -w $w -d $d -c | tee -a results.log;
      fi
    done;
  done;
done

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
R -q --no-readline --no-restore --no-save < $SCRIPT_DIR/hashbench_plot.r