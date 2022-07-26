// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![no_std]
#![cfg_attr(
    feature = "unstable",
    feature(new_uninit, get_mut_unchecked, negative_impls)
)]
#![feature(
    allocator_api,
    box_syntax,
    generic_associated_types,
    nonnull_slice_from_raw_parts,
    doc_auto_cfg,
    core_intrinsics
)]
#[cfg(test)]
extern crate std;

extern crate alloc;
extern crate core;

extern crate crossbeam_utils;

#[macro_use]
extern crate logging;

pub(crate) mod context;
pub(crate) mod log;
pub(crate) mod replica;

pub mod cnr;
pub mod nr;

#[cfg(doctest)]
mod test_readme {
    macro_rules! external_doc_test {
        ($x:expr) => {
            #[doc = $x]
            extern "C" {}
        };
    }

    external_doc_test!(include_str!("../README.md"));
}
