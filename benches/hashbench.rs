// Copyright © 2017-2019 Jon Gjengset <jon@thesquareplanet.com>.
// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Integration of the rust-evmap benchmarks (https://github.com/jonhoo/rust-evmap/)
//! for various hash-maps; added a node-replicated and urcu hash-table for comparison.

use chashmap::CHashMap;
use clap::{crate_version, value_t, App, Arg};
use rand::distributions::Distribution;
use rand::RngCore;
use std::collections::HashMap;

use std::ffi::c_void;
use std::mem;
use std::ptr;
use std::sync;
use std::thread;
use std::time;

use node_replication::log::Log;
use node_replication::replica::Replica;
use node_replication::Dispatch;

use urcu_sys;

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
            Arg::with_name("compare")
                .short("c")
                .multiple(true)
                .takes_value(true)
                .possible_values(&["std", "chashmap", "urcu", "nr", "evmap"])
                .help("What HashMap versions to benchmark."),
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

    let versions: Vec<&str> = match matches.values_of("compare") {
        Some(iter) => iter.collect(),
        None => vec!["std", "chashmap", "urcu", "nr", "evmap"],
    };

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
    if versions.contains(&"std") {
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
    if versions.contains(&"chashmap") {
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

    // then, benchmark urcu
    if versions.contains(&"urcu") {
        let map: RcuHashMap = RcuHashMap::new();
        let map = sync::Arc::new(map);
        let start = time::Instant::now();
        let end = start + dur;
        join.extend((0..readers).into_iter().map(|_| {
            let map = map.clone();
            let dist = dist.to_owned();
            thread::spawn(move || unsafe {
                urcu_sys::rcu_register_thread();
                let r = drive(map, end, dist, false, span);
                urcu_sys::rcu_unregister_thread();
                r
            })
        }));
        join.extend((0..writers).into_iter().map(|_| {
            let map = map.clone();
            let dist = dist.to_owned();
            thread::spawn(move || unsafe {
                urcu_sys::rcu_register_thread();
                let r = drive(map, end, dist, true, span);
                urcu_sys::rcu_unregister_thread();
                r
            })
        }));
        let (wres, rres): (Vec<_>, _) = join
            .drain(..)
            .map(|jh| jh.join().unwrap())
            .partition(|&(write, _)| write);
        stat("urcu", "write", wres);
        stat("urcu", "read", rres);
    }

    // then, benchmark sync::Arc<ReplicaAndToken>
    if versions.contains(&"nr") {
        const LOG_SIZE_BYTES: usize = 1024 * 1024 * 2;
        let log = sync::Arc::new(Log::<<NrHashMap as Dispatch>::WriteOperation>::new(
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
    if versions.contains(&"evmap") {
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
pub enum OpWr {
    /// Add an item to the hash-map.
    Put(u64, u64),
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum OpRd {
    /// Get item from the hash-map.
    Get(u64),
}

#[derive(Debug, Clone)]
pub struct NrHashMap {
    storage: HashMap<u64, u64>,
}

impl NrHashMap {
    pub fn put(&mut self, key: u64, val: u64) {
        self.storage.insert(key, val);
    }

    pub fn get(&self, key: u64) -> u64 {
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
    type ReadOperation = OpRd;
    type WriteOperation = OpWr;
    type Response = u64;
    type ResponseError = ();

    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpRd::Get(key) => return Ok(self.get(key)),
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(
        &mut self,
        op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
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
    token: usize,
    responses: Vec<Result<u64, ()>>,
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
        self.replica.execute_ro(OpRd::Get(key), self.token);
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
        match r {
            Ok(res) => return res,
            Err(_) => unreachable!(),
        }
    }

    fn b_put(&mut self, key: u64, value: u64) {
        self.replica.execute(OpWr::Put(key, value), self.token);
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

struct RcuHashMap {
    test_ht: *mut urcu_sys::cds_lfht,
}

unsafe impl Sync for RcuHashMap {}
unsafe impl Send for RcuHashMap {}

impl RcuHashMap {
    fn new() -> RcuHashMap {
        unsafe {
            // Not quite using 5M entries since cds_lfht needs power-of-twos
            let test_ht: *mut urcu_sys::cds_lfht = urcu_sys::cds_lfht_new(
                4194304, // initial hash-buckes  2^22
                4194304, // minimal hash-buckets 2^22
                8388608, // maximum hash-buckets 2^23
                urcu_sys::CDS_LFHT_AUTO_RESIZE as i32,
                ptr::null_mut(),
            );
            assert_ne!(test_ht, ptr::null_mut());
            RcuHashMap { test_ht }
        }
    }
}

#[repr(C)]
struct lfht_test_node {
    node: urcu_sys::cds_lfht_node,
    key: u64,
    data: u64,
    /* cache-cold for iteration */
    head: urcu_sys::rcu_head,
}

unsafe fn to_test_node(node: *mut urcu_sys::cds_lfht_node) -> *mut lfht_test_node {
    mem::transmute(node)
}

unsafe extern "C" fn test_match(node: *mut urcu_sys::cds_lfht_node, key: *const c_void) -> i32 {
    let my_key = key as u64;
    let test_node: *mut lfht_test_node = to_test_node(node);
    (my_key == (*test_node).key) as i32
}

impl Backend for sync::Arc<RcuHashMap> {
    fn b_get(&mut self, key: u64) -> u64 {
        unsafe {
            urcu_sys::rcu_read_lock();

            let mut iter: urcu_sys::cds_lfht_iter = mem::MaybeUninit::zeroed().assume_init();
            urcu_sys::cds_lfht_lookup(
                self.test_ht,
                key,
                Some(test_match),
                key as *const c_void,
                &mut iter as *mut urcu_sys::cds_lfht_iter,
            );
            let found_node: *mut urcu_sys::cds_lfht_node =
                urcu_sys::cds_lfht_iter_get_node(&mut iter);

            let value = if found_node != ptr::null_mut() {
                (*to_test_node(found_node)).data
            } else {
                0
            };

            urcu_sys::rcu_read_unlock();
            value
        }
    }

    fn b_put(&mut self, key: u64, value: u64) {
        unsafe {
            urcu_sys::rcu_read_lock();
            let layout = std::alloc::Layout::new::<lfht_test_node>();
            let new_node: *mut lfht_test_node =
                std::alloc::alloc_zeroed(layout) as *mut lfht_test_node;
            (*new_node).key = key;
            (*new_node).data = value;

            let old_node: *mut urcu_sys::cds_lfht_node = urcu_sys::cds_lfht_add_replace(
                self.test_ht,
                key,
                Some(test_match),
                key as *const c_void,
                &mut (*new_node).node as *mut urcu_sys::cds_lfht_node,
            );

            urcu_sys::rcu_read_unlock();

            if old_node != ptr::null_mut() {
                // 1 - wait till readers are done
                urcu_sys::synchronize_rcu();
                // 2 - free the node
                std::alloc::dealloc(old_node as *mut u8, layout);
            }
        }
    }
}
