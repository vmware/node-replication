use static_assertions::const_assert;

/// The maximum number of threads that can be registered with a replica.
///
/// If more than this number of threads try to register, the
/// [`Replica::register()`] function will start to return None.
#[cfg(not(loom))]
pub const MAX_THREADS_PER_REPLICA: usize = 256;
#[cfg(loom)]
pub const MAX_THREADS_PER_REPLICA: usize = 2;

// MAX_THREADS_PER_REPLICA must be a power of two
const_assert!(MAX_THREADS_PER_REPLICA.is_power_of_two());
