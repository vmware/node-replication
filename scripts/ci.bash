#!/bin/bash
#
# Usage: $ CI_MACHINE_TYPE='skylake2x' bash scripts/ci.bash
#
set -ex

# Disable all the Linux policies that are up to no good
sudo sh -c "echo 0 > /proc/sys/kernel/numa_balancing"
sudo sh -c "echo 0 > /sys/kernel/mm/ksm/run"
sudo sh -c "echo 0 > /sys/kernel/mm/ksm/merge_across_nodes"
sudo sh -c "echo never > /sys/kernel/mm/transparent_hugepage/enabled"

cd benches
RUST_TEST_THREADS=1 timeout 1h cargo bench --bench log --features="nr"
RUST_TEST_THREADS=1 timeout 1h cargo bench --bench synthetic --features="nr"
RUST_TEST_THREADS=1 timeout 1h cargo bench --bench stack --features="nr"
RUST_TEST_THREADS=1 timeout 20h cargo bench --bench hashmap --features="nr"
RUST_TEST_THREADS=1 timeout 1h cargo bench --bench vspace --features="nr"
RUST_TEST_THREADS=1 timeout 1h cargo bench --bench nrfs --features="c_nr"
RUST_TEST_THREADS=1 timeout 20h cargo bench --bench lockfree --features="c_nr"

timeout 1.5h bash hashbench_run.sh
timeout 1.5h bash rwlockbench_run.sh

# Move results to root repo.
cd ..
mv benches/*.csv .
mv benches/*.log .
mv benches/*.png .

# Check that we can checkout gh-pages early:
rm -rf gh-pages
git clone --depth 1 -b master git@github.com:gz/nr-benchmarks.git gh-pages
pip3 install -r gh-pages/requirements.txt

# Copy scalebench
python3 gh-pages/scale_bench_plot.py scaleout_benchmarks.csv

# Get revision
export GIT_REV_CURRENT=`git rev-parse --short HEAD`
export CSV_LINE="`date +%Y-%m-%d`",${GIT_REV_CURRENT},"","index.html","index.html","index.html"
echo $CSV_LINE >> gh-pages/_data/${CI_MACHINE_TYPE}.csv

SCALEBENCH_DEPLOY="gh-pages/scalebench/${CI_MACHINE_TYPE}/${GIT_REV_CURRENT}"
rm -rf ${SCALEBENCH_DEPLOY}
mkdir -p ${SCALEBENCH_DEPLOY}
mv baseline_comparison.csv ${SCALEBENCH_DEPLOY}
mv scaleout_benchmarks.csv ${SCALEBENCH_DEPLOY}
mv scaleout_benchmarks_cnr.csv ${SCALEBENCH_DEPLOY}
mv per_thread_times.* ${SCALEBENCH_DEPLOY}
mv throughput-*-*.* ${SCALEBENCH_DEPLOY}
gzip ${SCALEBENCH_DEPLOY}/baseline_comparison.csv
gzip ${SCALEBENCH_DEPLOY}/scaleout_benchmarks.csv
gzip ${SCALEBENCH_DEPLOY}/scaleout_benchmarks_cnr.csv

# Copy hashbench results
HASHBENCH_DEPLOY="gh-pages/hashbench/${CI_MACHINE_TYPE}/${GIT_REV_CURRENT}"
rm -rf ${HASHBENCH_DEPLOY}
mkdir -p ${HASHBENCH_DEPLOY}
mv results.log write-throughput.png read-throughput.png ${HASHBENCH_DEPLOY}

# Copy rwlockbench results
RWLOCKBENCH_DEPLOY="gh-pages/rwlockbench/${CI_MACHINE_TYPE}/${GIT_REV_CURRENT}"
rm -rf ${RWLOCKBENCH_DEPLOY}
mkdir -p ${RWLOCKBENCH_DEPLOY}
mv rwlockbench_results.log rwlock-write-throughput.png rwlock-read-throughput.png ${RWLOCKBENCH_DEPLOY}

# Setup html layouts
cp gh-pages/scalebench/index.markdown ${SCALEBENCH_DEPLOY}
cp gh-pages/hashbench/index.markdown ${HASHBENCH_DEPLOY}
cp gh-pages/rwlockbench/index.markdown ${RWLOCKBENCH_DEPLOY}

# Update CI time plots
cd gh-pages
python3 history_plots.py

# Push to gh-pages
git add .
git commit -a -m "Added benchmark results for $GIT_REV_CURRENT."
git push origin master
cd ..

rm -rf gh-pages
