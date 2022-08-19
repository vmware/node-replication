// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! An experiment on using SIMD based replication (instead of a log and threads).
#![feature(generic_associated_types, const_option, stdsimd, portable_simd)]

use std::arch::x86_64::*;
use std::simd::Simd;
use std::thread;

use memmap::MmapMut;
use memmap::MmapOptions;

fn make_regions(region_count: usize, region_size: usize) -> Vec<MmapMut> {
    let mut regions = Vec::with_capacity(region_count);
    for _r in 0..region_count {
        regions.push(MmapOptions::new().len(region_size).map_anon().unwrap());
    }

    regions
}

fn threadsline(regions: &mut Vec<MmapMut>, region_size: usize, input: &Vec<u8>) {
    assert!(input.len() <= region_size);

    thread::scope(|s| {
        let mut threads = Vec::with_capacity(regions.len());
        
        for region in regions {
            threads.push(s.spawn(move || {
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

fn simdline(regions: &mut Vec<MmapMut>, _region_size: usize, _input: &Vec<u8>) {
    unsafe {
        let region_base_addrs = regions.iter().map(|r| r.as_ref().as_ptr() as i64);

        let mut vindex_slice = [0; 8];
        assert!(region_base_addrs.len() <= vindex_slice.len());
        for (idx, rba) in region_base_addrs.enumerate() {
            vindex_slice[idx] = rba;
        }

        let base_address: *mut u8 = std::ptr::null_mut();

        assert!(regions.len().is_power_of_two());
        assert!(regions.len() <= u8::MAX.into());
        fn setbits(x: u8) -> u8 {
            u8::MAX >> (8 - x)
        }
        let k: __mmask8 = setbits(regions.len() as u8);

        let vindex = Simd::<i64, 8>::from_array(vindex_slice);
        let a: __m512d = Simd::<f64, 8>::from_array([
            f64::from_bits(0xffff_ffff_ffff_ffffu64),
            f64::from_bits(0xffff_ffff_ffff_ffffu64),
            f64::from_bits(0xffff_ffff_ffff_ffffu64),
            f64::from_bits(0xffff_ffff_ffff_ffffu64),
            f64::from_bits(0xffff_ffff_ffff_ffffu64),
            f64::from_bits(0xffff_ffff_ffff_ffffu64),
            f64::from_bits(0xffff_ffff_ffff_ffffu64),
            f64::from_bits(0xffff_ffff_ffff_ffffu64),
        ])
        .into();
        const SCALE: i32 = 1;

        assert!(regions.len() <= 8);
        _mm512_mask_i64scatter_pd(base_address, k, vindex.into(), a, SCALE);

        for region in regions.iter() {
            assert_eq!(
                u64::from_le_bytes([region[0], region[1], region[2], region[3], region[4], region[5], region[6], region[7]]),
                0xffff_ffff_ffff_ffffu64
            );
        }
    }
}

fn main() {
    let region_size: usize = 1024 * 1024 * 4;
    let regions_cnt = 4;
    let mut regions = make_regions(regions_cnt, region_size);

    let mut to_write: Vec<u8> = Vec::with_capacity(4096);
    to_write.extend_from_slice(&[0xffu8; 4096]);
    assert_eq!(to_write.len(), 4096);

    baseline(&mut regions, region_size, &to_write);
    threadsline(&mut regions, region_size, &to_write);
    simdline(&mut regions, region_size, &to_write);
}
