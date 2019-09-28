// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

// KCB is the local kernel control that stores all core local state.

use core::cell::{Ref, RefCell, RefMut, UnsafeCell};
use core::pin::Pin;
use core::ptr;
use std::boxed::Box;
use std::thread_local;

use super::kpi;

use super::process::Process;
use super::vspace::VSpace;

use super::memory::buddy::BuddyFrameAllocator;

#[thread_local]
static mut KCB: RefCell<Kcb> = RefCell::new(Kcb::new(&[0; 1], BuddyFrameAllocator::new()));

/// Try to retrieve the KCB.
pub fn try_get_kcb() -> Option<&'static mut Kcb> {
    unsafe { Some(KCB.get_mut()) }
}

/// Retrieve the KCB.
///
/// # Panic
/// This will fail in case the KCB is not yet set (i.e., early on during
/// initialization).
pub fn get_kcb() -> &'static mut Kcb {
    unsafe {
        let kcb = KCB.get_mut();
        if kcb.current_process().is_none() {
            kcb.swap_current_process(Box::new(Process::new_map().unwrap()));
        }
        kcb
    }
}

/// The Kernel Control Block for a given core. It contains all core-local state of the kernel.
pub struct Kcb {
    /// Pointer to the save area of the core,
    /// this is referenced on trap/syscall entries to save the CPU state into it.
    ///
    /// State from the save_area may be copied into current_process` save area
    /// to handle upcalls (in the general state it is stored/resumed from here).
    pub save_area: Option<Pin<Box<kpi::arch::SaveArea>>>,

    /// A handle to the currently active (scheduled) process.
    current_process: RefCell<Option<Box<Process>>>,

    /// Arguments passed to the kernel by the bootloader.
    //kernel_args: RefCell<&'static KernelArgs<[Module; 2]>>,

    /// A pointer to the memory location of the kernel ELF binary.
    kernel_binary: RefCell<&'static [u8]>,

    /// The initial VSpace as constructed by the bootloader.
    //init_vspace: RefCell<VSpace>,

    /// A handle to the physical memory manager.
    pmanager: RefCell<BuddyFrameAllocator>,
}

impl Kcb {
    pub const fn new(
        //kernel_args: &'static KernelArgs<[Module; 2]>,
        kernel_binary: &'static [u8],
        //init_vspace: VSpace,
        pmanager: BuddyFrameAllocator,
        //apic: XAPIC,
    ) -> Kcb {
        Kcb {
            save_area: None,
            current_process: RefCell::new(None),
            //kernel_args: RefCell::new(kernel_args),
            kernel_binary: RefCell::new(kernel_binary),
            //init_vspace: RefCell::new(init_vspace),
            pmanager: RefCell::new(pmanager),
            //apic: RefCell::new(apic),
            //interrupt_stack: None,
            //syscall_stack: None,
        }
    }

    pub fn set_syscall_stack(&mut self, mut stack: Pin<Box<[u8; 64 * 0x1000]>>) {}

    pub fn set_save_area(&mut self, save_area: Pin<Box<kpi::arch::SaveArea>>) {
        self.save_area = Some(save_area);
    }

    pub fn get_save_area_ptr(&self) -> *const kpi::arch::SaveArea {
        // TODO: this probably doesn't need an unsafe, but I couldn't figure
        // out how to get that pointer out of the Option<Pin<Box>>>
        unsafe {
            core::mem::transmute::<_, *const kpi::arch::SaveArea>(
                &*(*self.save_area.as_ref().unwrap()),
            )
        }
    }

    /// Swaps out current process with a new process. Returns the old process.
    pub fn swap_current_process(&self, new_current_process: Box<Process>) -> Option<Box<Process>> {
        let p = self.current_process.replace(Some(new_current_process));

        // TODO: need static assert and offsetof!
        debug_assert_eq!(
            (&self.save_area as *const _ as usize) - (self as *const _ as usize),
            8,
            "The current process entry should be at offset 8 (for assembly)"
        );

        p
    }

    pub fn current_process(&self) -> RefMut<Option<Box<Process>>> {
        self.current_process.borrow_mut()
    }

    pub fn pmanager(&self) -> RefMut<BuddyFrameAllocator> {
        self.pmanager.borrow_mut()
    }

    /*pub fn apic(&self) -> RefMut<XAPIC> {
        self.apic.borrow_mut()
    }*/

    /*pub fn init_vspace(&self) -> RefMut<VSpace> {
        self.init_vspace.borrow_mut()
    }*/

    pub fn kernel_binary(&self) -> Ref<&'static [u8]> {
        self.kernel_binary.borrow()
    }

    /*pub fn kernel_args(&self) -> Ref<&'static KernelArgs<[Module; 2]>> {
        self.kernel_args.borrow()
    }*/
}
