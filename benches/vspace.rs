// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Evaluates a virtual address space implementation using node-replication.
#![feature(test)]

extern crate alloc;

use std::fmt;
use std::mem::transmute;
use std::pin::Pin;
use std::sync::Arc;

use log::{debug, trace};
use rand::{thread_rng, Rng};
use x86::bits64::paging::*;

use node_replication::replica::Replica;
use node_replication::Dispatch;

mod mkbench;
mod utils;
use utils::benchmark::*;
use utils::Operation;

extern crate jemallocator;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn kernel_vaddr_to_paddr(v: VAddr) -> PAddr {
    let vaddr_val: usize = v.into();
    PAddr::from(vaddr_val as u64 - 0x0)
}

fn paddr_to_kernel_vaddr(p: PAddr) -> VAddr {
    let paddr_val: u64 = p.into();
    VAddr::from((paddr_val + 0x0) as usize)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct VSpaceError {
    at: u64,
}

/// Type of resource we're trying to allocate
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum ResourceType {
    /// ELF Binary data
    Binary,
    /// Physical memory
    Memory,
    /// Page-table meta-data
    PageTable,
}

/// Mapping rights to give to address translation.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[allow(unused)]
pub enum MapAction {
    /// Don't map
    None,
    /// Map region read-only.
    ReadUser,
    /// Map region read-only for kernel.
    ReadKernel,
    /// Map region read-write.
    ReadWriteUser,
    /// Map region read-write for kernel.
    ReadWriteKernel,
    /// Map region read-executable.
    ReadExecuteUser,
    /// Map region read-executable for kernel.
    ReadExecuteKernel,
    /// Map region read-write-executable.
    ReadWriteExecuteUser,
    /// Map region read-write-executable for kernel.
    ReadWriteExecuteKernel,
}

impl MapAction {
    /// Transform MapAction into rights for 1 GiB page.
    fn to_pdpt_rights(&self) -> PDPTFlags {
        use MapAction::*;
        match self {
            None => PDPTFlags::empty(),
            ReadUser => PDPTFlags::XD,
            ReadKernel => PDPTFlags::US | PDPTFlags::XD,
            ReadWriteUser => PDPTFlags::RW | PDPTFlags::XD,
            ReadWriteKernel => PDPTFlags::RW | PDPTFlags::US | PDPTFlags::XD,
            ReadExecuteUser => PDPTFlags::empty(),
            ReadExecuteKernel => PDPTFlags::US,
            ReadWriteExecuteUser => PDPTFlags::RW,
            ReadWriteExecuteKernel => PDPTFlags::RW | PDPTFlags::US,
        }
    }

    /// Transform MapAction into rights for 2 MiB page.
    fn to_pd_rights(&self) -> PDFlags {
        use MapAction::*;
        match self {
            None => PDFlags::empty(),
            ReadUser => PDFlags::XD,
            ReadKernel => PDFlags::US | PDFlags::XD,
            ReadWriteUser => PDFlags::RW | PDFlags::XD,
            ReadWriteKernel => PDFlags::RW | PDFlags::US | PDFlags::XD,
            ReadExecuteUser => PDFlags::empty(),
            ReadExecuteKernel => PDFlags::US,
            ReadWriteExecuteUser => PDFlags::RW,
            ReadWriteExecuteKernel => PDFlags::RW | PDFlags::US,
        }
    }

    /// Transform MapAction into rights for 4KiB page.
    fn to_pt_rights(&self) -> PTFlags {
        use MapAction::*;
        match self {
            None => PTFlags::empty(),
            ReadUser => PTFlags::XD,
            ReadKernel => PTFlags::US | PTFlags::XD,
            ReadWriteUser => PTFlags::RW | PTFlags::XD,
            ReadWriteKernel => PTFlags::RW | PTFlags::US | PTFlags::XD,
            ReadExecuteUser => PTFlags::empty(),
            ReadExecuteKernel => PTFlags::US,
            ReadWriteExecuteUser => PTFlags::RW,
            ReadWriteExecuteKernel => PTFlags::RW | PTFlags::US,
        }
    }
}

impl fmt::Display for MapAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use MapAction::*;
        match self {
            None => write!(f, " ---"),
            ReadUser => write!(f, "uR--"),
            ReadKernel => write!(f, "kR--"),
            ReadWriteUser => write!(f, "uRW-"),
            ReadWriteKernel => write!(f, "kRW-"),
            ReadExecuteUser => write!(f, "uR-X"),
            ReadExecuteKernel => write!(f, "kR-X"),
            ReadWriteExecuteUser => write!(f, "uRWX"),
            ReadWriteExecuteKernel => write!(f, "kRWX"),
        }
    }
}

pub struct VSpace {
    pub pml4: Pin<Box<PML4>>,
    allocs: Vec<(*mut u8, usize)>,
}

unsafe impl Sync for VSpace {}
unsafe impl Send for VSpace {}

impl Drop for VSpace {
    fn drop(&mut self) {
        unsafe {
            self.allocs.reverse();
            for (base, size) in self.allocs.iter() {
                //println!("-- dealloc {:p} {:#x}", base, size);
                alloc::alloc::dealloc(
                    *base,
                    core::alloc::Layout::from_size_align_unchecked(*size, 4096),
                );
            }
        }
    }
}

impl Default for VSpace {
    fn default() -> VSpace {
        VSpace {
            pml4: Box::pin(
                [PML4Entry::new(PAddr::from(0x0u64), PML4Flags::empty()); PAGE_SIZE_ENTRIES],
            ),
            allocs: Vec::with_capacity(1024),
        }
    }
}

impl VSpace {
    fn map_generic(
        &mut self,
        vbase: VAddr,
        pregion: (PAddr, usize),
        rights: MapAction,
    ) -> Result<(), VSpaceError> {
        let (pbase, psize) = pregion;
        assert_eq!(pbase % BASE_PAGE_SIZE, 0);
        assert_eq!(psize % BASE_PAGE_SIZE, 0);
        assert_eq!(vbase % BASE_PAGE_SIZE, 0);
        assert_ne!(rights, MapAction::None);

        debug!(
            "map_generic {:#x} -- {:#x} -> {:#x} -- {:#x} {}",
            vbase,
            vbase + psize,
            pbase,
            pbase + psize,
            rights
        );

        let pml4_idx = pml4_index(vbase);
        if !self.pml4[pml4_idx].is_present() {
            trace!("New PDPDT for {:?} @ PML4[{}]", vbase, pml4_idx);
            self.pml4[pml4_idx] = self.new_pdpt();
        }
        assert!(
            self.pml4[pml4_idx].is_present(),
            "The PML4 slot we need was not allocated?"
        );

        let pdpt = self.get_pdpt(self.pml4[pml4_idx]);
        let mut pdpt_idx = pdpt_index(vbase);
        // TODO: if we support None mappings, this is if not good enough:
        if !pdpt[pdpt_idx].is_present() {
            // The virtual address corresponding to our position within the page-table
            let vaddr_pos: usize = PML4_SLOT_SIZE * pml4_idx + HUGE_PAGE_SIZE * pdpt_idx;

            // In case we can map something at a 1 GiB granularity and
            // we still have at least 1 GiB to map, create huge-page mappings
            if vbase.as_usize() == vaddr_pos
                && (pbase % HUGE_PAGE_SIZE == 0)
                && psize >= HUGE_PAGE_SIZE
            {
                // To track how much space we've covered
                let mut mapped = 0;

                // Add entries to PDPT as long as we're within this allocated PDPT table
                // and have 1 GiB chunks to map:
                while mapped < psize && ((psize - mapped) >= HUGE_PAGE_SIZE) && pdpt_idx < 512 {
                    assert!(!pdpt[pdpt_idx].is_present());
                    pdpt[pdpt_idx] = PDPTEntry::new(
                        pbase + mapped,
                        PDPTFlags::P | PDPTFlags::PS | rights.to_pdpt_rights(),
                    );
                    trace!(
                        "Mapped 1GiB range {:#x} -- {:#x} -> {:#x} -- {:#x}",
                        vbase + mapped,
                        (vbase + mapped) + HUGE_PAGE_SIZE,
                        pbase + mapped,
                        (vbase + mapped) + HUGE_PAGE_SIZE
                    );

                    pdpt_idx += 1;
                    mapped += HUGE_PAGE_SIZE;
                }

                if mapped < psize {
                    trace!(
                        "map_generic recurse from 1 GiB map to finish {:#x} -- {:#x} -> {:#x} -- {:#x}",
                        vbase + mapped,
                        vbase + (psize - mapped),
                        (pbase + mapped),
                        pbase + (psize - mapped),
                    );
                    return self.map_generic(
                        vbase + mapped,
                        ((pbase + mapped), psize - mapped),
                        rights,
                    );
                } else {
                    // Everything fit in 1 GiB ranges,
                    // We're done with mappings
                    return Ok(());
                }
            } else {
                trace!(
                    "Mapping 0x{:x} -- 0x{:x} is smaller than 1 GiB, going deeper.",
                    vbase,
                    vbase + psize
                );
                pdpt[pdpt_idx] = self.new_pd();
            }
        }
        assert!(
            pdpt[pdpt_idx].is_present(),
            "The PDPT entry we're relying on is not allocated?"
        );
        if pdpt[pdpt_idx].is_page() {
            // "An existing mapping already covers the 1 GiB range we're trying to map in?
            return Err(VSpaceError { at: vbase.as_u64() });
        }

        let pd = self.get_pd(pdpt[pdpt_idx]);
        let mut pd_idx = pd_index(vbase);
        if !pd[pd_idx].is_present() {
            let vaddr_pos: usize =
                PML4_SLOT_SIZE * pml4_idx + HUGE_PAGE_SIZE * pdpt_idx + LARGE_PAGE_SIZE * pd_idx;

            // In case we can map something at a 2 MiB granularity and
            // we still have at least 2 MiB to map create large-page mappings
            if vbase.as_usize() == vaddr_pos
                && (pbase % LARGE_PAGE_SIZE == 0)
                && psize >= LARGE_PAGE_SIZE
            {
                let mut mapped = 0;
                // Add entries as long as we are within this allocated PDPT table
                // and have at least 2 MiB things to map
                while mapped < psize && ((psize - mapped) >= LARGE_PAGE_SIZE) && pd_idx < 512 {
                    if pd[pd_idx].is_present() {
                        trace!("Already mapped pd at {:#x}", pbase + mapped);
                        return Err(VSpaceError { at: vbase.as_u64() });
                    }

                    pd[pd_idx] = PDEntry::new(
                        pbase + mapped,
                        PDFlags::P | PDFlags::PS | rights.to_pd_rights(),
                    );
                    trace!(
                        "Mapped 2 MiB region {:#x} -- {:#x} -> {:#x} -- {:#x}",
                        vbase + mapped,
                        (vbase + mapped) + LARGE_PAGE_SIZE,
                        pbase + mapped,
                        (pbase + mapped) + LARGE_PAGE_SIZE
                    );

                    pd_idx += 1;
                    mapped += LARGE_PAGE_SIZE;
                }

                if mapped < psize {
                    trace!(
                        "map_generic recurse from 2 MiB map to finish {:#x} -- {:#x} -> {:#x} -- {:#x}",
                        vbase + mapped,
                        vbase + (psize - mapped),
                        (pbase + mapped),
                        pbase + (psize - mapped),
                    );
                    return self.map_generic(
                        vbase + mapped,
                        ((pbase + mapped), psize - mapped),
                        rights,
                    );
                } else {
                    // Everything fit in 2 MiB ranges,
                    // We're done with mappings
                    return Ok(());
                }
            } else {
                trace!(
                    "Mapping 0x{:x} -- 0x{:x} is smaller than 2 MiB, going deeper.",
                    vbase,
                    vbase + psize
                );
                pd[pd_idx] = self.new_pt();
            }
        }
        assert!(
            pd[pd_idx].is_present(),
            "The PD entry we're relying on is not allocated?"
        );
        if pd[pd_idx].is_page() {
            // An existing mapping already covers the 2 MiB range we're trying to map in?
            return Err(VSpaceError { at: vbase.as_u64() });
        }

        let pt = self.get_pt(pd[pd_idx]);
        let mut pt_idx = pt_index(vbase);
        let mut mapped: usize = 0;
        while mapped < psize && pt_idx < 512 {
            if !pt[pt_idx].is_present() {
                pt[pt_idx] = PTEntry::new(pbase + mapped, PTFlags::P | rights.to_pt_rights());
            } else {
                return Err(VSpaceError { at: vbase.as_u64() });
            }

            mapped += BASE_PAGE_SIZE;
            pt_idx += 1;
        }

        // Need go to different PD/PDPT/PML4 slot
        if mapped < psize {
            trace!(
                "map_generic recurse from 4 KiB map to finish {:#x} -- {:#x} -> {:#x} -- {:#x}",
                vbase + mapped,
                vbase + (psize - mapped),
                (pbase + mapped),
                pbase + (psize - mapped),
            );
            return self.map_generic(vbase + mapped, ((pbase + mapped), psize - mapped), rights);
        } else {
            // else we're done here, return
            Ok(())
        }
    }

    /// A simple wrapper function for allocating just one page.
    fn allocate_one_page(&mut self) -> PAddr {
        self.allocate_pages(1, ResourceType::PageTable)
    }

    fn allocate_pages(&mut self, how_many: usize, _typ: ResourceType) -> PAddr {
        let new_region: *mut u8 = unsafe {
            alloc::alloc::alloc(core::alloc::Layout::from_size_align_unchecked(
                how_many * BASE_PAGE_SIZE,
                4096,
            ))
        };
        assert!(!new_region.is_null());
        for i in 0..how_many * BASE_PAGE_SIZE {
            unsafe {
                *new_region.offset(i as isize) = 0u8;
            }
        }
        self.allocs.push((new_region, how_many * BASE_PAGE_SIZE));

        kernel_vaddr_to_paddr(VAddr::from(new_region as usize))
    }

    fn new_pt(&mut self) -> PDEntry {
        let paddr: PAddr = self.allocate_one_page();
        return PDEntry::new(paddr, PDFlags::P | PDFlags::RW | PDFlags::US);
    }

    fn new_pd(&mut self) -> PDPTEntry {
        let paddr: PAddr = self.allocate_one_page();
        return PDPTEntry::new(paddr, PDPTFlags::P | PDPTFlags::RW | PDPTFlags::US);
    }

    fn new_pdpt(&mut self) -> PML4Entry {
        let paddr: PAddr = self.allocate_one_page();
        return PML4Entry::new(paddr, PML4Flags::P | PML4Flags::RW | PML4Flags::US);
    }

    /// Resolve a PDEntry to a page table.
    fn get_pt<'b>(&self, entry: PDEntry) -> &'b mut PT {
        unsafe { transmute::<VAddr, &mut PT>(paddr_to_kernel_vaddr(entry.address())) }
    }

    /// Resolve a PDPTEntry to a page directory.
    fn get_pd<'b>(&self, entry: PDPTEntry) -> &'b mut PD {
        unsafe { transmute::<VAddr, &mut PD>(paddr_to_kernel_vaddr(entry.address())) }
    }

    /// Resolve a PML4Entry to a PDPT.
    fn get_pdpt<'b>(&self, entry: PML4Entry) -> &'b mut PDPT {
        unsafe { transmute::<VAddr, &mut PDPT>(paddr_to_kernel_vaddr(entry.address())) }
    }

    fn resolve_addr(&self, addr: VAddr) -> Option<PAddr> {
        let pml4_idx = pml4_index(addr);
        if self.pml4[pml4_idx].is_present() {
            let pdpt_idx = pdpt_index(addr);
            let pdpt = self.get_pdpt(self.pml4[pml4_idx]);
            if pdpt[pdpt_idx].is_present() {
                if pdpt[pdpt_idx].is_page() {
                    // Page is a 1 GiB mapping, we have to return here
                    let page_offset = addr.huge_page_offset();
                    return Some(pdpt[pdpt_idx].address() + page_offset);
                } else {
                    let pd_idx = pd_index(addr);
                    let pd = self.get_pd(pdpt[pdpt_idx]);
                    if pd[pd_idx].is_present() {
                        if pd[pd_idx].is_page() {
                            // Encountered a 2 MiB mapping, we have to return here
                            let page_offset = addr.large_page_offset();
                            return Some(pd[pd_idx].address() + page_offset);
                        } else {
                            let pt_idx = pt_index(addr);
                            let pt = self.get_pt(pd[pd_idx]);
                            if pt[pt_idx].is_present() {
                                let page_offset = addr.base_page_offset();
                                return Some(pt[pt_idx].address() + page_offset);
                            }
                        }
                    }
                }
            }
        }
        None
    }

    fn map_new(
        &mut self,
        base: VAddr,
        size: usize,
        rights: MapAction,
        paddr: PAddr,
    ) -> Result<(PAddr, usize), VSpaceError> {
        assert_eq!(base % BASE_PAGE_SIZE, 0, "base is not page-aligned");
        assert_eq!(size % BASE_PAGE_SIZE, 0, "size is not page-aligned");
        self.map_generic(base, (paddr, size), rights)?;
        Ok((paddr, size))
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum OpcodeWr {
    Map(VAddr, usize, MapAction, PAddr),
    MapDevice(VAddr, PAddr, usize),
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum OpcodeRd {
    Identify(u64),
}

#[derive(Default)]
struct VSpaceDispatcher {
    vspace: VSpace,
}

impl Dispatch for VSpaceDispatcher {
    type ReadOperation = OpcodeRd;
    type WriteOperation = OpcodeWr;
    type Response = (u64, u64);
    type ResponseError = VSpaceError;

    fn dispatch(&self, op: Self::ReadOperation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpcodeRd::Identify(base) => {
                let paddr = self.vspace.resolve_addr(VAddr::from(base));
                Ok((paddr.map(|pnum| pnum.as_u64()).unwrap_or(0x0), 0x0))
            }
        }
    }

    fn dispatch_mut(
        &mut self,
        op: Self::WriteOperation,
    ) -> Result<Self::Response, Self::ResponseError> {
        match op {
            OpcodeWr::Map(vbase, length, rights, pbase) => {
                let (retaddr, len) = self.vspace.map_new(vbase, length, rights, pbase)?;
                Ok((retaddr.as_u64(), len as u64))
            }
            OpcodeWr::MapDevice(base, paddr, bound) => {
                self.vspace
                    .map_generic(base, (paddr, bound as usize), MapAction::ReadWriteUser)?;
                Ok((0, 0))
            }
        }
    }
}

fn generate_operations(nop: usize) -> Vec<Operation<OpcodeRd, OpcodeWr>> {
    let mut ops = Vec::with_capacity(nop);
    let mut rng = thread_rng();

    const PAGE_RANGE_MASK: u64 = !0xffff_0000_0000_0fff;
    const MAP_SIZE_MASK: u64 = !0xffff_ffff_f000_0fff;
    for _i in 0..nop {
        match rng.gen::<usize>() % 3 {
            0 => ops.push(Operation::ReadOperation(OpcodeRd::Identify(
                rng.gen::<u64>(),
            ))),
            1 => ops.push(Operation::WriteOperation(OpcodeWr::Map(
                VAddr::from(rng.gen::<u64>() & PAGE_RANGE_MASK),
                rng.gen::<usize>() & MAP_SIZE_MASK as usize,
                MapAction::ReadWriteUser,
                PAddr::from(rng.gen::<u64>() & PAGE_RANGE_MASK),
            ))),
            2 => ops.push(Operation::WriteOperation(OpcodeWr::MapDevice(
                VAddr::from(rng.gen::<u64>() & PAGE_RANGE_MASK),
                PAddr::from(rng.gen::<u64>() & PAGE_RANGE_MASK),
                rng.gen::<usize>() & MAP_SIZE_MASK as usize,
            ))),
            _ => unreachable!(),
        }
    }
    ops
}

fn vspace_single_threaded(c: &mut TestHarness) {
    const NOP: usize = 3000;
    const LOG_SIZE_BYTES: usize = 16 * 1024 * 1024;
    mkbench::baseline_comparison::<Replica<VSpaceDispatcher>, VSpaceDispatcher>(
        c,
        "vspace",
        generate_operations(NOP),
        LOG_SIZE_BYTES,
    );
}

fn vspace_scale_out(c: &mut TestHarness) {
    const NOP: usize = 3000;
    let ops = generate_operations(NOP);

    mkbench::ScaleBenchBuilder::<Replica<VSpaceDispatcher>, VSpaceDispatcher>::new(ops)
        .machine_defaults()
        .configure(
            c,
            "vspace-scaleout",
            |_cid, rid, _log, replica: &Arc<Replica<VSpaceDispatcher>>, op, _batch_size| match op {
                Operation::ReadOperation(o) => {
                    let _r = replica.execute_ro(*o, rid);
                }
                Operation::WriteOperation(o) => {
                    let _r = replica.execute(*o, rid);
                }
            },
        );
}

fn main() {
    let _r = env_logger::try_init();
    let mut harness = Default::default();

    vspace_single_threaded(&mut harness);
    vspace_scale_out(&mut harness);
}
