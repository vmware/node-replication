#!/bin/bash
set -ex

cargo build --release
rm rwlockbench_results.log || true
for w in 1 4 8; do
  for r in 1 4 8; do
    if [[ $w > 0 ]] || [[ $r > 0 ]] ; then
      RUST_TEST_THREADS=1 cargo bench --bench rwlockbench --features="nr" -- -r $r -w $w | tee -a rwlockbench_results.log;
    fi
  done;
done

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
R -q --no-readline --no-restore --no-save < $SCRIPT_DIR/rwlockbench_plot.r