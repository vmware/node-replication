use static_assertions::const_assert;

/// The maximum number of operations that can be batched inside this context.
#[cfg(not(loom))]
pub const MAX_PENDING_OPS: usize = 32;
#[cfg(loom)]
pub(crate) const MAX_PENDING_OPS: usize = 1;
// This constant must be a power of two for `index()` to work.
const_assert!(MAX_PENDING_OPS.is_power_of_two());
