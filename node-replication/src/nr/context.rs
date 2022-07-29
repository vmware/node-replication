// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! NR specific Context.

pub use crate::context::MAX_PENDING_OPS;

/// The NR per-thread context.
///
/// It stores every outstanding request (`T`) and response (`R`) pair.
pub(crate) type Context<T, R> = crate::context::Context<T, R, ()>;
