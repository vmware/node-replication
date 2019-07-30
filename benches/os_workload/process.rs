use core::fmt;
use core::mem::transmute;
use core::ops::{Deref, DerefMut};
use core::ptr;

use x86::bits64::paging;
use x86::bits64::paging::*;
use x86::bits64::rflags;

use super::memory::{kernel_vaddr_to_paddr, paddr_to_kernel_vaddr, PAddr, VAddr};
use super::memory::{BespinPageTableProvider, PageTableProvider};

use super::kpi;
use super::vspace::*;
use super::KError;

pub struct UserPtr<T> {
    value: *mut T,
}

impl<T> UserPtr<T> {
    pub fn new(pointer: *mut T) -> UserPtr<T> {
        UserPtr { value: pointer }
    }

    pub fn vaddr(&self) -> VAddr {
        VAddr::from(self.value as u64)
    }
}

impl<T> Deref for UserPtr<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.value }
    }
}

impl<T> DerefMut for UserPtr<T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.value }
    }
}

impl<T> Drop for UserPtr<T> {
    fn drop(&mut self) {}
}

pub struct UserValue<T> {
    value: T,
}

impl<T> UserValue<T> {
    pub fn new(pointer: T) -> UserValue<T> {
        UserValue { value: pointer }
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        unsafe { core::mem::transmute(&self.value) }
    }
}

impl<T> Deref for UserValue<T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe {
            //rflags::stac();
            &self.value
        }
    }
}

impl<T> DerefMut for UserValue<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            //rflags::stac();
            &mut self.value
        }
    }
}

impl<T> Drop for UserValue<T> {
    fn drop(&mut self) {
        //unsafe { rflags::clac() };
    }
}

/// A ResumeHandle that can either be an upcall or a context restore.
///
/// # TODO
/// This two should ideally be separate with a common resume trait once impl Trait
/// is flexible enough.
/// The interface is not really safe at the moment (we use it in very restricted ways
/// i.e., get the handle and immediatle resume but we can def. make this more safe
/// to use...)
pub struct ResumeHandle {
    is_upcall: bool,
    pub save_area: *const kpi::arch::SaveArea,

    entry_point: VAddr,
    stack_top: VAddr,
    cpu_ctl: u64,
    vector: u64,
    exception: u64,
}

impl ResumeHandle {
    pub fn new_restore(save_area: *const kpi::arch::SaveArea) -> ResumeHandle {
        ResumeHandle {
            is_upcall: false,
            save_area: save_area,
            entry_point: VAddr::zero(),
            stack_top: VAddr::zero(),
            cpu_ctl: 0,
            vector: 0,
            exception: 0,
        }
    }

    pub fn new_upcall(
        entry_point: VAddr,
        stack_top: VAddr,
        cpu_ctl: u64,
        vector: u64,
        exception: u64,
    ) -> ResumeHandle {
        ResumeHandle {
            is_upcall: true,
            save_area: ptr::null(),
            entry_point: entry_point,
            stack_top: stack_top,
            cpu_ctl: cpu_ctl,
            vector: vector,
            exception: exception,
        }
    }

    pub unsafe fn resume(self) -> ! {
        if self.is_upcall {
            self.upcall()
        } else {
            self.restore()
        }
    }

    unsafe fn restore(self) -> ! {
        unreachable!("We should not come here!");
    }

    unsafe fn upcall(self) -> ! {
        unreachable!("We should not come here!");
    }
}

/// A process representation.
#[repr(C, packed)]
pub struct Process {
    /// CPU context save area (must be first, see exec.S).
    pub save_area: kpi::x86_64::SaveArea,
    /// ELF File mappings that were installed into the address space.
    pub mapping: Vec<(VAddr, usize, u64, MapAction)>,
    /// Process ID.
    pub pid: u64,
    /// The address space of the process.
    pub vspace: VSpace,
    /// Offset where ELF is located.
    pub offset: VAddr,
    /// The entry point of the ELF file.
    pub entry_point: VAddr,

    // TODO: The stuff that comes next is actually per core
    // so we should take it out of here and move it into
    // a separate dispatcher object:
    /// Initial allocated stack (base address).
    pub stack_base: VAddr,
    /// Initial allocated stack (top address).
    pub stack_top: VAddr,
    /// Initial allocated stack size.
    pub stack_size: usize,

    /// Upcall allocated stack (base address).
    pub upcall_stack_base: VAddr,
    /// Upcall allocated stack (top address).
    pub upcall_stack_top: VAddr,
    /// Upcall allocated stack size.
    pub upcall_stack_size: usize,

    /// Virtual CPU control used by the user-space upcall mechanism.
    pub vcpu_ctl: Option<UserPtr<kpi::arch::VirtualCpu>>,
}

impl Drop for Process {
    fn drop(&mut self) {
        //panic!("Shouldn't drop");
    }
}

impl Process {
    /// Create a process from a Module (i.e., a struct passed by UEFI)
    pub fn new_map() -> Result<Process, KError> {
        let mut p = Process::new(0);

        // Allocate a stack
        p.vspace.map(
            p.stack_base,
            p.stack_size,
            MapAction::ReadWriteExecuteUser,
            BASE_PAGE_SIZE as u64,
        )?;

        // Allocate a upcall stack
        p.vspace.map(
            p.upcall_stack_base,
            p.upcall_stack_size,
            MapAction::ReadWriteExecuteUser,
            BASE_PAGE_SIZE as u64,
        )?;

        // Install the kernel mappings
        super::kcb::try_get_kcb().map(|kcb| {
            let kernel_pml_entry = kcb.init_vspace().pml4[128];
            info!("KERNEL MAPPINGS {:?}", kernel_pml_entry);
            p.vspace.pml4[128] = kernel_pml_entry;
        });

        Ok(p)
    }

    /// Create a new `empty` process.
    fn new<'b>(pid: u64) -> Process {
        let stack_base = VAddr::from(0xadf000_0000usize);
        let stack_size = 128 * BASE_PAGE_SIZE;
        let stack_top = stack_base + stack_size - 8usize; // -8 due to x86 stack alignemnt requirements

        let upcall_stack_base = VAddr::from(0xad2000_0000usize);
        let upcall_stack_size = 128 * BASE_PAGE_SIZE;
        let upcall_stack_top = upcall_stack_base + stack_size - 8usize; // -8 due to x86 stack alignemnt requirements

        unsafe {
            Process {
                offset: VAddr::from(0usize),
                mapping: Vec::with_capacity(64),
                pid: pid,
                vspace: VSpace::new(),
                save_area: Default::default(),
                entry_point: VAddr::from(0usize),
                stack_base: stack_base,
                stack_top: stack_top,
                stack_size: stack_size,
                upcall_stack_base: upcall_stack_base,
                upcall_stack_size: upcall_stack_size,
                upcall_stack_top: upcall_stack_top,
                vcpu_ctl: None,
            }
        }
    }

    /// Start the process (run it for the first time).
    pub fn start(&mut self) -> ResumeHandle {
        ResumeHandle::new_upcall(self.offset + self.entry_point, self.stack_top, 0, 0, 0)
    }

    pub fn resume(&self) -> ResumeHandle {
        ResumeHandle::new_restore(&self.save_area as *const kpi::arch::SaveArea)
    }

    pub fn upcall(&mut self, vector: u64, exception: u64) -> ResumeHandle {
        let (entry_point, cpu_ctl) = self
            .vcpu_ctl
            .as_mut()
            .map_or((VAddr::zero(), 0), |mut ctl| {
                (ctl.resume_with_upcall, ctl.vaddr().into())
            });

        info!("cpu_ctl is : {:#x}", cpu_ctl);
        info!("upcall for {:?}", self);
        ResumeHandle::new_upcall(
            entry_point,
            self.upcall_stack_top,
            cpu_ctl,
            vector,
            exception,
        )
    }

    fn maybe_switch_vspace(&self) {
        unreachable!()
    }
}

impl fmt::Debug for Process {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Process {}:\nSaveArea: {:?}", self.pid, self.save_area)
    }
}
