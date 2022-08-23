// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! An experiment on using SIMD based replication (instead of a log and threads).
#![feature(
    generic_associated_types,
    const_option,
    stdsimd,
    portable_simd,
    slice_pattern
)]

use core::slice::SlicePattern;
use std::arch::x86_64::*;
use std::simd::Simd;
use std::thread;
use std::time::Instant;

use memmap::MmapMut;
use memmap::MmapOptions;

use bench_utils::topology;

fn make_regions(region_count: usize, region_size: usize) -> Vec<MmapMut> {
    let mut regions = Vec::with_capacity(region_count);
    for r in 0..region_count {
        // Pin thread to force the allocations below (`operations` etc.)
        // with the correct NUMA affinity
        let core = *topology::MACHINE_TOPOLOGY
            .cpus_on_node(r as u64)
            .get(0)
            .expect("No core on NUMA node found");
        bench_utils::pin_thread(core.cpu);
        let mut region = MmapOptions::new().len(region_size).map_anon().unwrap();
        region.as_mut().fill(0x0);
        regions.push(region);
    }

    reset_regions(&mut regions);
    regions
}

fn reset_regions(regions: &mut Vec<MmapMut>) {
    for region in regions {
        region.as_mut().fill(0x0);
    }
}

fn assert_regions_content(len: usize, regions: &mut Vec<MmapMut>) {
    for region in regions {
        region.as_slice()[0..len].iter().for_each(|p| {
            assert_eq!(*p, 0xff);
        });
    }
}

fn threadsline(regions: &mut Vec<MmapMut>, region_size: usize, input: &Vec<u8>) {
    assert!(input.len() <= region_size);

    thread::scope(|s| {
        let mut threads = Vec::with_capacity(regions.len());

        for (r, region) in regions.into_iter().enumerate() {
            threads.push(s.spawn(move || {
                let core = *topology::MACHINE_TOPOLOGY
                    .cpus_on_node(r as u64)
                    .get(r)
                    .expect("No core on NUMA node found");
                bench_utils::pin_thread(core.cpu);

                let ptr = region.as_mut();
                ptr[0..input.len()].copy_from_slice(input.as_slice());
            }));
        }

        for thread in threads {
            thread.join().expect("Can't join thread?");
        }
    });
}

fn baseline(regions: &mut Vec<MmapMut>, region_size: usize, input: &Vec<u8>) {
    assert!(input.len() <= region_size);

    for region in regions {
        let ptr = region.as_mut();
        ptr[0..input.len()].copy_from_slice(input.as_slice());
    }
}

fn simdline(regions: &mut Vec<MmapMut>, _region_size: usize, input: &Vec<u8>) {
    assert!(input.len() % 8 == 0);

    let cnt = input.len() / 8;
    let region_base_addrs: Vec<i64> = regions.iter().map(|r| r.as_ref().as_ptr() as i64).collect();
    assert!(regions.len().is_power_of_two());
    assert!(regions.len() <= u8::MAX.into());
    fn setbits(x: u8) -> u8 {
        u8::MAX >> (8 - x)
    }
    let k: __mmask8 = setbits(regions.len() as u8);

    for idx in 0..cnt {
        let val = unsafe { *(input[idx * 8..(idx + 1) * 8].as_ptr() as *mut u64) };
        assert_eq!(val, 0xffff_ffff_ffff_ffffu64);

        let mut vindex_slice = [0; 8];
        assert!(region_base_addrs.len() <= vindex_slice.len());
        for (rbaidx, rba) in region_base_addrs.iter().enumerate() {
            vindex_slice[rbaidx] = rba + (idx * 8) as i64;
        }
        let base_address: *mut u8 = std::ptr::null_mut();
        let vindex = Simd::<i64, 8>::from_array(vindex_slice);
        let a: __m512d = Simd::<f64, 8>::from_array([
            f64::from_bits(val),
            f64::from_bits(val),
            f64::from_bits(val),
            f64::from_bits(val),
            f64::from_bits(val),
            f64::from_bits(val),
            f64::from_bits(val),
            f64::from_bits(val),
        ])
        .into();
        //println!(
        //    "base_address={:#x} k={:#x} vindex={:#x} a={:?}",
        //    base_address as i64, k, vindex, a
        //);
        assert!(regions.len() <= 8);

        const SCALE: i32 = 1;
        unsafe { _mm512_mask_i64scatter_pd(base_address, k, vindex.into(), a, SCALE) };

        //for region in regions.iter() {
        //    assert_eq!(
        //        u64::from_le_bytes([
        //            region[0], region[1], region[2], region[3], region[4], region[5], region[6],
        //            region[7]
        //        ]),
        //        val
        //    );
        //}
    }
}

fn main() {
    let region_size: usize = 1024 * 1024 * 512;
    let regions_cnt = 4;
    const BUF_SIZE: usize = 1024 * 1024 * 512;

    let mut regions = make_regions(regions_cnt, region_size);

    let mut to_write: Vec<u8> = Vec::with_capacity(BUF_SIZE);
    for _ in 0..BUF_SIZE {
        to_write.push(0xff);
    }
    assert_eq!(to_write.len(), BUF_SIZE);

    reset_regions(&mut regions);
    let now = Instant::now();
    baseline(&mut regions, region_size, &to_write);
    let elapsed = now.elapsed();
    assert_regions_content(to_write.len(), &mut regions);
    println!("baseline,{:?}", elapsed);

    reset_regions(&mut regions);
    let now = Instant::now();
    threadsline(&mut regions, region_size, &to_write);
    let elapsed = now.elapsed();
    assert_regions_content(to_write.len(), &mut regions);
    println!("threadsline,{:?}", elapsed);

    reset_regions(&mut regions);
    let now = Instant::now();
    simdline(&mut regions, region_size, &to_write);
    let elapsed = now.elapsed();
    assert_regions_content(to_write.len(), &mut regions);
    println!("simdline,{:?}", elapsed);
}
