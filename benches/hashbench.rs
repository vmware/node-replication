// Copyright © 2017-2019 Jon Gjengset <jon@thesquareplanet.com>.
// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Integration of the rust-evmap benchmarks (https://github.com/jonhoo/rust-evmap/)
//! for various hash-maps; added a node-replicated hash-table for comparison.

extern crate chashmap;
#[macro_use]
extern crate clap;
extern crate evmap;
extern crate node_replication;
extern crate parking_lot;
extern crate rand;
extern crate zipf;

use chashmap::CHashMap;
use clap::{App, Arg};
use rand::distributions::Distribution;
use rand::RngCore;
use std::collections::HashMap;

use std::sync;
use std::thread;
use std::time;

use node_replication::log::Log;
use node_replication::replica::Replica;
use node_replication::Dispatch;

mod utils;

fn main() {
    let args = std::env::args().filter(|e| e != "--bench");
    let matches = App::new("Concurrent HashMap Benchmarker")
        .version(crate_version!())
        .author("Jon Gjengset <jon@thesquareplanet.com>")
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
            Arg::with_name("compare")
                .short("c")
                .help("Also benchmark Arc<RwLock<HashMap>> and CHashMap"),
        )
        .arg(
            Arg::with_name("eventual")
                .short("e")
                .takes_value(true)
                .default_value("1")
                .value_name("N")
                .help("Refresh evmap every N writes"),
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

    let refresh = value_t!(matches, "eventual", usize).unwrap_or_else(|e| e.exit());
    let readers = value_t!(matches, "readers", usize).unwrap_or_else(|e| e.exit());
    let writers = value_t!(matches, "writers", usize).unwrap_or_else(|e| e.exit());
    let dist = matches.value_of("distribution").unwrap_or("uniform");
    let dur = time::Duration::from_secs(5);
    let dur_in_ns = dur.as_secs() * 1_000_000_000_u64 + dur.subsec_nanos() as u64;
    let dur_in_s = dur_in_ns as f64 / 1_000_000_000_f64;
    let span = 10_000;

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
    // first, benchmark Arc<RwLock<HashMap>>
    if matches.is_present("compare") {
        let map: HashMap<u64, u64> = HashMap::with_capacity(5_000_000);
        let map = sync::Arc::new(parking_lot::RwLock::new(map));
        let start = time::Instant::now();
        let end = start + dur;
        join.extend((0..readers).into_iter().map(|_| {
            let map = map.clone();
            let dist = dist.to_owned();
            thread::spawn(move || drive(map, end, dist, false, span))
        }));
        join.extend((0..writers).into_iter().map(|_| {
            let map = map.clone();
            let dist = dist.to_owned();
            thread::spawn(move || drive(map, end, dist, true, span))
        }));
        let (wres, rres): (Vec<_>, _) = join
            .drain(..)
            .map(|jh| jh.join().unwrap())
            .partition(|&(write, _)| write);
        stat("std", "write", wres);
        stat("std", "read", rres);
    }

    // then, benchmark Arc<CHashMap>
    if matches.is_present("compare") {
        let map: CHashMap<u64, u64> = CHashMap::with_capacity(5_000_000);
        let map = sync::Arc::new(map);
        let start = time::Instant::now();
        let end = start + dur;
        join.extend((0..readers).into_iter().map(|_| {
            let map = map.clone();
            let dist = dist.to_owned();
            thread::spawn(move || drive(map, end, dist, false, span))
        }));
        join.extend((0..writers).into_iter().map(|_| {
            let map = map.clone();
            let dist = dist.to_owned();
            thread::spawn(move || drive(map, end, dist, true, span))
        }));
        let (wres, rres): (Vec<_>, _) = join
            .drain(..)
            .map(|jh| jh.join().unwrap())
            .partition(|&(write, _)| write);
        stat("chashmap", "write", wres);
        stat("chashmap", "read", rres);
    }

    // then, benchmark sync::Arc<ReplicaAndToken>
    if matches.is_present("compare") {
        const LOG_SIZE_BYTES: usize = 1024 * 1024 * 2;
        let log = sync::Arc::new(Log::<<NrHashMap as Dispatch>::Operation>::new(
            LOG_SIZE_BYTES,
        ));
        let replica = sync::Arc::new(Replica::<NrHashMap>::new(&log));

        let start = time::Instant::now();
        let end = start + dur;
        join.extend((0..readers).into_iter().map(|_| {
            let replica = ReplicaAndToken::new(replica.clone());
            let dist = dist.to_owned();
            thread::spawn(move || drive(replica, end, dist, false, span))
        }));
        join.extend((0..writers).into_iter().map(|_| {
            let replica = ReplicaAndToken::new(replica.clone());
            let dist = dist.to_owned();

            thread::spawn(move || drive(replica, end, dist, true, span))
        }));
        let (wres, rres): (Vec<_>, _) = join
            .drain(..)
            .map(|jh| jh.join().unwrap())
            .partition(|&(write, _)| write);
        stat("nr", "write", wres);
        stat("nr", "read", rres);
    }

    // finally, benchmark evmap
    {
        let (r, w) = evmap::Options::default()
            .with_capacity(5_000_000)
            .construct();
        let w = sync::Arc::new(parking_lot::Mutex::new((w, 0, refresh)));
        let start = time::Instant::now();
        let end = start + dur;
        join.extend((0..readers).into_iter().map(|_| {
            let map = EvHandle::Read(r.clone());
            let dist = dist.to_owned();
            thread::spawn(move || drive(map, end, dist, false, span))
        }));
        join.extend((0..writers).into_iter().map(|_| {
            let map = EvHandle::Write(w.clone());
            let dist = dist.to_owned();
            thread::spawn(move || drive(map, end, dist, true, span))
        }));
        let (wres, rres): (Vec<_>, _) = join
            .drain(..)
            .map(|jh| jh.join().unwrap())
            .partition(|&(write, _)| write);

        let n = if refresh == 1 {
            "evmap".to_owned()
        } else {
            format!("evmap-refresh{}", refresh)
        };
        stat(&n, "write", wres);
        stat(&n, "read", rres);
    }
}

trait Backend {
    fn b_get(&mut self, key: u64) -> u64;
    fn b_put(&mut self, key: u64, value: u64);
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

impl Backend for sync::Arc<CHashMap<u64, u64>> {
    fn b_get(&mut self, key: u64) -> u64 {
        self.get(&key).map(|v| *v).unwrap_or(0)
    }

    fn b_put(&mut self, key: u64, value: u64) {
        self.insert(key, value);
    }
}

impl Backend for sync::Arc<parking_lot::RwLock<HashMap<u64, u64>>> {
    fn b_get(&mut self, key: u64) -> u64 {
        self.read().get(&key).map(|&v| v).unwrap_or(0)
    }

    fn b_put(&mut self, key: u64, value: u64) {
        self.write().insert(key, value);
    }
}

enum EvHandle {
    Read(evmap::ReadHandle<u64, u64>),
    Write(sync::Arc<parking_lot::Mutex<(evmap::WriteHandle<u64, u64>, usize, usize)>>),
}

impl Backend for EvHandle {
    fn b_get(&mut self, key: u64) -> u64 {
        if let EvHandle::Read(ref r) = *self {
            r.get_and(&key, |v| v[0]).unwrap_or(0)
        } else {
            unreachable!();
        }
    }

    fn b_put(&mut self, key: u64, value: u64) {
        if let EvHandle::Write(ref w) = *self {
            let mut w = w.lock();
            w.0.update(key, value);
            w.1 += 1;
            if w.1 == w.2 {
                w.1 = 0;
                w.0.refresh();
            }
        } else {
            unreachable!();
        }
    }
}

/// Operations we can perform on the stack.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Op {
    /// Add an item to the hash-map.
    Put(u64, u64),
    /// Get item from the hash-map.
    Get(u64),
    /// Invalid operation
    Invalid,
}

impl Default for Op {
    fn default() -> Op {
        Op::Invalid
    }
}

#[derive(Debug, Clone)]
pub struct NrHashMap {
    storage: HashMap<u64, u64>,
}

impl NrHashMap {
    pub fn put(&mut self, key: u64, val: u64) {
        self.storage.insert(key, val);
    }

    pub fn get(&mut self, key: u64) -> u64 {
        *self.storage.get(&key).unwrap_or(&0)
    }
}

impl Default for NrHashMap {
    /// Return a dummy hash-map with initial capacity of 50k elements.
    fn default() -> NrHashMap {
        NrHashMap {
            storage: HashMap::with_capacity(5_000_000),
        }
    }
}

impl Dispatch for NrHashMap {
    type Operation = Op;
    type Response = u64;

    /// Implements how we execute operation from the log against our local stack
    fn dispatch(&mut self, op: Self::Operation) -> Self::Response {
        match op {
            Op::Put(key, val) => {
                self.put(key, val);
                0
            }
            Op::Get(key) => return self.get(key),
            Op::Invalid => unreachable!("Op::Invalid?"),
        }
    }
}

struct ReplicaAndToken<'a> {
    replica: sync::Arc<Replica<'a, NrHashMap>>,
    token: usize,
    responses: Vec<u64>,
}

impl<'a> ReplicaAndToken<'a> {
    fn new(replica: sync::Arc<Replica<'a, NrHashMap>>) -> ReplicaAndToken<'a> {
        let token = replica.register().unwrap();
        ReplicaAndToken {
            replica,
            token,
            responses: Vec::with_capacity(1),
        }
    }
}

impl<'a> Backend for ReplicaAndToken<'a> {
    fn b_get(&mut self, key: u64) -> u64 {
        self.replica.execute(Op::Get(key), self.token);
        let mut i = 1;
        while self.replica.get_responses(self.token, &mut self.responses) == 0 {
            if i % (1024 * 1024 * 2) == 0 {
                println!(
                    "{:?} Waiting too long for get_responses",
                    std::thread::current().id()
                );
            }
            i += 1;
        }
        let r = self.responses[0];
        self.responses.clear();
        r
    }

    fn b_put(&mut self, key: u64, value: u64) {
        self.replica.execute(Op::Put(key, value), self.token);
        let mut i = 1;
        while self.replica.get_responses(self.token, &mut self.responses) == 0 {
            if i % (1024 * 1024 * 2) == 0 {
                println!(
                    "{:?} Waiting too long for get_responses",
                    std::thread::current().id()
                );
            }
            i += 1;
        }
        self.responses.clear();
    }
}
