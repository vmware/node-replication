// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![allow(safe_packed_borrows)]

pub mod kcb;
pub mod kpi;
mod memory;
mod process;
mod vspace;

use vspace::ResourceType;

pub use kpi::*;
use process::{UserPtr, UserValue};

use custom_error::custom_error;
use x86::current::paging::{PAddr, VAddr, BASE_PAGE_SIZE};

pub trait PowersOf2 {
    fn log2(self) -> u8;
}

impl PowersOf2 for usize {
    #[cfg(target_pointer_width = "64")]
    fn log2(self) -> u8 {
        63 - self.leading_zeros() as u8
    }

    #[cfg(target_pointer_width = "32")]
    fn log2(self) -> u8 {
        31 - self.leading_zeros() as u8
    }
}

custom_error! {
    #[derive(Copy, Clone)]
    pub KError
    ProcessNotSet = "CURRENT_PROCESS is not set",
    NotSupported = "Request is not yet supported",
    InvalidSyscallArgument1{a: u64} = "Invalid 1st syscall argument supplied: {}",
    InvalidVSpaceOperation{a: u64} = "Invalid VSpace Operation (2nd syscall argument) supplied: {}",
    InvalidProcessOperation{a: u64} = "Invalid Process Operation (2nd syscall argument) supplied: {}",
    ProcessCreate{desc: u64}  = "Unable to create process: {desc}",
    VSpace{source: vspace::VSpaceError} = "VSpace operation covers existing mapping",
}

impl Default for KError {
    fn default() -> KError {
        KError::NotSupported
    }
}

impl Into<SystemCallError> for KError {
    /// Translate KErrors to SystemCallErrors.
    ///
    /// The idea is to reduce a big set of events into a smaller set of less precise errors.
    /// We can log the the precise errors before we return in the kernel since the conversion
    /// happens at the end of the system call.
    fn into(self) -> SystemCallError {
        match self {
            KError::VSpace { source: s } => s.into(),
            KError::InvalidSyscallArgument1 { a: _ } => SystemCallError::NotSupported,
            KError::InvalidVSpaceOperation { a: _ } => SystemCallError::NotSupported,
            KError::InvalidProcessOperation { a: _ } => SystemCallError::NotSupported,
            KError::ProcessCreate { desc: _ } => SystemCallError::InternalError,
            _ => SystemCallError::InternalError,
        }
    }
}

/// System call handler for printing
fn process_print(buf: UserValue<&str>) -> Result<(u64, u64), KError> {
    let buffer: &str = *buf;
    info!("{}", buffer);
    Ok((0, 0))
}

/// System call handler for process exit
fn process_exit(code: u64) -> Result<(u64, u64), KError> {
    info!("Process got exit, we are done for now...");
    //super::debug::shutdown(crate::ExitReason::Ok);
    Ok((0, 0))
}

fn handle_process(arg1: u64, arg2: u64, arg3: u64) -> Result<(u64, u64), KError> {
    let op = ProcessOperation::from(arg1);
    debug!("{:?} {:#x} {:#x}", op, arg2, arg3);

    match op {
        ProcessOperation::Log => {
            let buffer: *const u8 = arg2 as *const u8;
            let len: usize = arg3 as usize;

            let user_str = unsafe {
                let slice = core::slice::from_raw_parts(buffer, len);
                core::str::from_utf8_unchecked(slice)
            };

            process_print(UserValue::new(user_str))
        }
        ProcessOperation::InstallVCpuArea => unsafe {
            let kcb = kcb::get_kcb();
            let mut plock = kcb.current_process();

            plock.as_mut().map_or(Err(KError::ProcessNotSet), |p| {
                let cpu_ctl_addr = VAddr::from(arg2);
                p.vspace.map(
                    cpu_ctl_addr,
                    BASE_PAGE_SIZE,
                    vspace::MapAction::ReadWriteUser,
                    0x1000,
                )?;

                //x86::tlb::flush_all();

                p.vcpu_ctl = Some(UserPtr::new(
                    cpu_ctl_addr.as_u64() as *mut kpi::x86_64::VirtualCpu
                ));

                warn!("installed vcpu area {:p}", cpu_ctl_addr,);

                Ok((cpu_ctl_addr.as_u64(), 0))
            })
        },
        ProcessOperation::AllocateVector => {
            // TODO: missing proper IRQ resource allocation...
            let vector = arg2;
            let core = arg3;
            //super::irq::ioapic_establish_route(vector, core);
            Ok((vector, core))
        }
        ProcessOperation::Exit => {
            let exit_code = arg2;
            process_exit(arg1)
        }
        _ => Err(KError::InvalidProcessOperation { a: arg1 }),
    }
}

/// System call handler for vspace operations
fn handle_vspace(
    arg1: u64,
    arg2: u64,
    arg3: u64,
    arg4: u64,
    arg5: u64,
) -> Result<(u64, u64), KError> {
    let op = VSpaceOperation::from(arg1);
    let base = VAddr::from(arg2);
    let bound = arg3;
    debug!("{:?} {:#x} {:#x}", op, base, bound);

    let kcb = kcb::get_kcb();
    let mut plock = kcb.current_process();

    match op {
        VSpaceOperation::Map => unsafe {
            plock.as_mut().map_or(Err(KError::ProcessNotSet), |p| {
                let (paddr, size) = (*p).vspace.map_new(
                    base,
                    bound as usize,
                    vspace::MapAction::ReadWriteUser,
                    x86::bits64::paging::PAddr(arg4),
                )?;

                //tlb::flush_all();
                Ok((paddr.as_u64(), size as u64))
            })
        },
        VSpaceOperation::MapDevice => unsafe {
            plock.as_mut().map_or(Err(KError::ProcessNotSet), |p| {
                let paddr = PAddr::from(base.as_u64());
                p.vspace.map_generic(
                    base,
                    (paddr, bound as usize),
                    vspace::MapAction::ReadWriteUser,
                )?;

                //tlb::flush_all();
                Ok((paddr.as_u64(), bound))
            })
        },
        VSpaceOperation::Unmap => {
            error!("Can't do VSpaceOperation unmap yet.");
            Err(KError::NotSupported)
        }
        VSpaceOperation::Identify => unsafe {
            trace!("Identify base {:#x}.", base);
            plock.as_mut().map_or(Err(KError::ProcessNotSet), |p| {
                let paddr = p.vspace.resolve_addr(base);

                Ok((paddr.map(|pnum| pnum.as_u64()).unwrap_or(0x0), 0x0))
            })
        },
        VSpaceOperation::Unknown => {
            error!("Got an invalid VSpaceOperation code.");
            Err(KError::InvalidVSpaceOperation { a: arg1 })
        }
    }
}

pub fn get_paddr(size: u64) -> u64 {
    let kcb = kcb::get_kcb();
    let mut plock = kcb.current_process();

    plock
        .as_mut()
        .map_or(Err(KError::ProcessNotSet), |p| {
            Ok((*p).vspace.allocate_pages_aligned(
                size as usize / BASE_PAGE_SIZE,
                ResourceType::Memory,
                0x1000,
            ))
        })
        .unwrap()
        .as_u64()
}

#[inline(never)]
#[no_mangle]
pub fn syscall_handle(
    function: u64,
    arg1: u64,
    arg2: u64,
    arg3: u64,
    arg4: u64,
    arg5: u64,
) -> Result<(u64, u64), KError> {
    //println!("syscall_handle {} {} {} {} {} {}", function, arg1, arg2, arg3, arg4, arg5);
    let status: Result<(u64, u64), KError> = match SystemCall::new(function) {
        SystemCall::Process => handle_process(arg1, arg2, arg3),
        SystemCall::VSpace => handle_vspace(arg1, arg2, arg3, arg4, arg5),
        _ => Err(KError::InvalidSyscallArgument1 { a: function }),
    };

    status
}
