// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Common replica definitions, implementation for NR/CNR etc.

use static_assertions::const_assert;

/// The maximum number of threads that can be registered with a replica.
///
/// If more than this number of threads try to register, the
/// [`crate::nr::Replica::register()`] or [`crate::cnr::Replica::register()`]
/// function will start to return None.
#[cfg(not(loom))]
pub const MAX_THREADS_PER_REPLICA: usize = 32;
#[cfg(loom)]
pub const MAX_THREADS_PER_REPLICA: usize = 2;
// MAX_THREADS_PER_REPLICA must be a power of two
const_assert!(MAX_THREADS_PER_REPLICA.is_power_of_two());

/// Unique identifier for the given replicas.
///
/// It's unique within a NR/CNR instance. It makes sense to be e.g., the same as
/// the NUMA node that this replica corresponds to.
pub type ReplicaId = usize;

/// A (monotoically increasing) number that uniquely identifies a thread that's
/// registered with the replica.
///
/// `ThreadIdx` will start at 1 because they're used in the
/// [`crate::nr::replica::CombinerLock`] to indicate which thread holds the
/// lock, and 0 means no one holds the lock.
///
/// # See also
/// - [`crate::nr::Replica::register`]
/// - [`crate::cnr::Replica::register`]
pub type ThreadIdx = usize;

/// A token handed out to threads that replicas with replicas.
///
/// It is a bug to supply this token to another replica object than the one that
/// issued it. This would ideally be a runtime check which leads to a panic in
/// the future.
///
/// # Implementation detail on types
/// Ideally this would be an affine type (not Clone/Copy) for max. type safety
/// and returned again by [`crate::nr::Replica::execute`] and
/// [`crate::nr::Replica::execute_mut`]. However it feels like this would hurt
/// API ergonomics a lot.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct ReplicaToken(pub(crate) ThreadIdx);

/// Make it harder to accidentially use the same ReplicaToken on multiple
/// threads.
///
/// That would be a disaster.
#[cfg(not(feature = "async"))]
impl !Send for ReplicaToken {}

impl ReplicaToken {
    /// Creates a new ReplicaToken.
    ///
    /// # Safety
    /// This should only ever be used for the benchmark harness to create additional fake
    /// replica implementations.
    ///
    /// If `pub(test)` is ever supported, we should do that instead.
    #[doc(hidden)]
    pub unsafe fn new(tid: ThreadIdx) -> Self {
        ReplicaToken(tid)
    }

    /// Get the (replica specific) thread identifier for this particular token.
    ///
    /// This method always returns something >= 1.
    pub fn tid(&self) -> ThreadIdx {
        self.0
    }
}
