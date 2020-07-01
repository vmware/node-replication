// Copyright © 2017-2019 Jon Gjengset <jon@thesquareplanet.com>.
// Copyright © VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Integration of the rust-evmap benchmarks (https://github.com/jonhoo/rust-evmap/)
//! for various hash-maps; added a node-replicated and urcu hash-table for comparison.
use chashmap::CHashMap as HashMap;
use clap::{crate_version, value_t, App, Arg};
use rand::distributions::Distribution;
use rand::RngCore;

use std::sync;
use std::thread;
use std::time;

use node_replication::{Dispatch, Log, Replica, ReplicaToken};

mod utils;

fn main() {
    let args = std::env::args().filter(|e| e != "--bench");
    let matches = App::new("Concurrent HashMap Benchmarker")
        .version(crate_version!())
        .author("Jon Gjengset <jon@thesquareplanet.com>, Gerd Zellweger <mail@gerdzellweger.com>")
        .about(
            "Benchmark multiple implementations of concurrent HashMaps with varying read/write load",
        )
        .arg(
            Arg::with_name("readers")
                .short("r")
                .long("readers")
                .help("Set the number of readers")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("writers")
                .short("w")
                .long("writers")
                .required(true)
                .help("Set the number of writers")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("distribution")
                .short("d")
                .long("dist")
                .possible_values(&["uniform", "skewed"])
                .default_value("uniform")
                .help("Set the distribution for reads and writes")
                .takes_value(true),
        )
        .get_matches_from(args);

    let readers = value_t!(matches, "readers", usize).unwrap_or_else(|e| e.exit());
    let writers = value_t!(matches, "writers", usize).unwrap_or_else(|e| e.exit());
    let dist = matches.value_of("distribution").unwrap_or("uniform");
    let dur = time::Duration::from_secs(5);
    let dur_in_ns = dur.as_secs() * 1_000_000_000_u64 + dur.subsec_nanos() as u64;
    let dur_in_s = dur_in_ns as f64 / 1_000_000_000_f64;
    let span = 1_000_000;

    let stat = |var: &str, op, results: Vec<(_, usize)>| {
        for (i, res) in results.into_iter().enumerate() {
            println!(
                "{:2} {:2} {:10} {:10} {:8.0} ops/s {} {}",
                readers,
                writers,
                dist,
                var,
                res.1 as f64 / dur_in_s as f64,
                op,
                i
            )
        }
    };

    let mut join = Vec::with_capacity(readers + writers);
    let versions: Vec<&str> = vec!["nr"];

    // then, benchmark sync::Arc<ReplicaAndToken>
    if versions.contains(&"nr") {
        const LOG_SIZE_BYTES: usize = 1024 * 1024 * 2;
        let log1 = sync::Arc::new(Log::<<NrHashMap as Dispatch>::WriteOperation>::new(
            LOG_SIZE_BYTES,
        ));
        let log2 = sync::Arc::new(Log::<<NrHashMap as Dispatch>::WriteOperation>::new(
            LOG_SIZE_BYTES,
        ));
        let replica = Replica::<NrHashMap>::new(vec![log1, log2]);

        let start = time::Instant::now();
        let end = start + dur;
        join.extend((0..readers).into_iter().map(|_| {
            let replica = replica.clone();
            let dist = dist.to_owned();
            thread::spawn(move || {
                let replica = ReplicaAndToken::new(replica);
                drive(replica, end, dist, false, span)
            })
        }));
        join.extend((0..writers).into_iter().map(|_| {
            let replica = replica.clone();
            let dist = dist.to_owned();

            thread::spawn(move || {
                let replica = ReplicaAndToken::new(replica);
                drive(replica, end, dist, true, span)
            })
        }));
        let (wres, rres): (Vec<_>, _) = join
            .drain(..)
            .map(|jh| jh.join().unwrap())
            .partition(|&(write, _)| write);
        stat("nr", "write", wres);
        stat("nr", "read", rres);
    }
}

trait Backend {
    fn b_get(&self, key: u64) -> u64;
    fn b_put(&self, key: u64, value: u64);
}

fn drive<B: Backend>(
    mut backend: B,
    end: time::Instant,
    dist: String,
    write: bool,
    span: usize,
) -> (bool, usize) {
    use rand::Rng;

    let mut ops = 0;
    let skewed = dist == "skewed";
    let mut t_rng = rand::thread_rng();
    let zipf = zipf::ZipfDistribution::new(span, 1.03).unwrap();
    while time::Instant::now() < end {
        // generate both so that overhead is always the same
        let id_uniform: u64 = t_rng.gen_range(0, span as u64);
        let id_skewed = zipf.sample(&mut t_rng) as u64;
        let id = if skewed { id_skewed } else { id_uniform };
        if write {
            backend.b_put(id, t_rng.next_u64());
        } else {
            backend.b_get(id);
        }
        ops += 1;
    }

    (write, ops)
}

/// Operations we can perform on the stack.
#[derive(Hash, Debug, Eq, PartialEq, Clone, Copy)]
pub enum OpWr {
    /// Add an item to the hash-map.
    Put(u64, u64),
}

#[derive(Hash, Debug, Eq, PartialEq, Clone, Copy)]
pub enum OpRd {
    /// Get item from the hash-map.
    Get(u64),
}

#[derive(Debug, Clone)]
pub struct NrHashMap {
    storage: HashMap<u64, u64>,
}

impl NrHashMap {
    pub fn put(&self, key: u64, val: u64) {
        self.storage.insert(key, val);
    }

    pub fn get(&self, key: u64) -> u64 {
        *self.storage.get(&key).unwrap()
    }
}

impl Default for NrHashMap {
    /// Return a dummy hash-map with initial capacity of 50k elements.
    fn default() -> NrHashMap {
        let mut storage = HashMap::with_capacity(5_000_000);
        for i in 0..5_000_000 {
            storage.insert(i as u64, (i + 1) as u64);
        }
        NrHashMap { storage }
    }
}

impl Dispatch for NrHashMap {
    type ReadOperation = OpRd;
    type WriteOperation = OpWr;
    type Response = Result<u64, ()>;

    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            OpRd::Get(key) => return Ok(self.get(key)),
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(&self, op: Self::WriteOperation) -> Self::Response {
        match op {
            OpWr::Put(key, val) => {
                self.put(key, val);
                Ok(0)
            }
        }
    }
}

struct ReplicaAndToken<'a> {
    replica: sync::Arc<Replica<'a, NrHashMap>>,
    token: ReplicaToken,
}

impl<'a> ReplicaAndToken<'a> {
    fn new(replica: sync::Arc<Replica<'a, NrHashMap>>) -> ReplicaAndToken<'a> {
        let token = replica.register().unwrap();
        ReplicaAndToken { replica, token }
    }
}

impl<'a> Backend for ReplicaAndToken<'a> {
    fn b_get(&self, key: u64) -> u64 {
        match self.replica.execute(OpRd::Get(key), self.token) {
            Ok(res) => return res,
            Err(_) => unreachable!(),
        }
    }

    fn b_put(&self, key: u64, value: u64) {
        self.replica
            .execute_mut(OpWr::Put(key, value), self.token)
            .unwrap();
    }
}
