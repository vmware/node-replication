// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// Copyright © 2022 Tokio Contributors. All Rights Reserved.
// SPDX-License-Identifier: MIT

//! Like a Box<T>, but reusable.
//!
//! Code is borrowed from Tokio's reusable box. See also:
//! <https://github.com/tokio-rs/tokio/blob/master/tokio-util/src/sync/reusable_box.rs>

use alloc::alloc::Layout;
use alloc::boxed::Box;
use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::ptr::{self, NonNull};
use core::task::{Context, Poll};

/// A reusable `Pin<Box<dyn Future<Output = T> + Send>>`.
///
/// This type lets you replace the future stored in the box without
/// reallocating when the size and alignment permits this.
pub struct ReusableBoxFuture<'a, T> {
    boxed: NonNull<dyn Future<Output = T> + Send + 'a>,
}

impl<'a, T> ReusableBoxFuture<'a, T> {
    /// Create a new `ReusableBoxFuture<T>` containing the provided future.
    pub fn new<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'a,
    {
        let boxed: Box<dyn Future<Output = T> + Send> = Box::new(future);

        let boxed = Box::into_raw(boxed);

        // SAFETY: Box::into_raw does not return null pointers.
        let boxed = unsafe { NonNull::new_unchecked(boxed) };

        Self { boxed }
    }

    /// Replace the future currently stored in this box.
    ///
    /// This reallocates if and only if the layout of the provided future is
    /// different from the layout of the currently stored future.
    pub fn set<F>(&mut self, future: F)
    where
        F: Future<Output = T> + Send + 'a,
    {
        if let Err(future) = self.try_set(future) {
            *self = Self::new(future);
        }
    }

    /// Replace the future currently stored in this box.
    ///
    /// This function never reallocates, but returns an error if the provided
    /// future has a different size or alignment from the currently stored
    /// future.
    pub fn try_set<F>(&mut self, future: F) -> Result<(), F>
    where
        F: Future<Output = T> + Send + 'a,
    {
        // SAFETY: The pointer is not dangling.
        let self_layout = {
            let dyn_future: &(dyn Future<Output = T> + Send) = unsafe { self.boxed.as_ref() };
            Layout::for_value(dyn_future)
        };

        if Layout::new::<F>() == self_layout {
            // SAFETY: We just checked that the layout of F is correct.
            unsafe {
                self.set_same_layout(future);
            }

            Ok(())
        } else {
            Err(future)
        }
    }

    /// Set the current future.
    ///
    /// # Safety
    ///
    /// This function requires that the layout of the provided future is the
    /// same as `self.layout`.
    unsafe fn set_same_layout<F>(&mut self, future: F)
    where
        F: Future<Output = T> + Send + 'a,
    {
        // Drop the existing future, catching any panics.
        ptr::drop_in_place(self.boxed.as_ptr());

        // Overwrite the future behind the pointer. This is safe because the
        // allocation was allocated with the same size and alignment as the type F.
        let self_ptr: *mut F = self.boxed.as_ptr() as *mut F;
        ptr::write(self_ptr, future);

        // Update the vtable of self.boxed. The pointer is not null because we
        // just got it from self.boxed, which is not null.
        self.boxed = NonNull::new_unchecked(self_ptr);
    }

    /// Get a pinned reference to the underlying future.
    pub fn get_pin(&mut self) -> Pin<&mut (dyn Future<Output = T> + Send)> {
        // SAFETY: The user of this box cannot move the box, and we do not move it
        // either.
        unsafe { Pin::new_unchecked(self.boxed.as_mut()) }
    }

    /// Poll the future stored inside this box.
    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<T> {
        self.get_pin().poll(cx)
    }
}

impl<'a, T> Future for ReusableBoxFuture<'a, T> {
    type Output = T;

    /// Poll the future stored inside this box.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        Pin::into_inner(self).get_pin().poll(cx)
    }
}

// The future stored inside ReusableBoxFuture<T> must be Send.
unsafe impl<'a, T> Send for ReusableBoxFuture<'a, T> {}

// The only method called on self.boxed is poll, which takes &mut self, so this
// struct being Sync does not permit any invalid access to the Future, even if
// the future is not Sync.
unsafe impl<'a, T> Sync for ReusableBoxFuture<'a, T> {}

// Just like a Pin<Box<dyn Future>> is always Unpin, so is this type.
impl<'a, T> Unpin for ReusableBoxFuture<'a, T> {}

impl<'a, T> Drop for ReusableBoxFuture<'a, T> {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.boxed.as_ptr()));
        }
    }
}

impl<'a, T> fmt::Debug for ReusableBoxFuture<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReusableBoxFuture").finish()
    }
}
