use core::alloc::Layout;
use core::mem::transmute;

use core::fmt;

use x86::bits64::paging;

pub mod buddy;

pub use self::buddy::BuddyFrameAllocator as PhysicalMemoryAllocator;

use slabmalloc::{ObjectPage, PageProvider};
pub use x86::current::paging::{PAddr, VAddr, BASE_PAGE_SIZE};

/// Start of the kernel address space.
pub const KERNEL_BASE: u64 = 0x0;

/// Translate a kernel 'virtual' address to the physical address of the memory.
pub fn kernel_vaddr_to_paddr(v: VAddr) -> PAddr {
    let vaddr_val: usize = v.into();
    PAddr::from(vaddr_val as u64 - KERNEL_BASE)
}

/// Translate a physical memory address into a kernel addressable location.
pub fn paddr_to_kernel_vaddr(p: PAddr) -> VAddr {
    let paddr_val: u64 = p.into();
    VAddr::from((paddr_val + KERNEL_BASE) as usize)
}

pub trait PhysicalAllocator {
    fn init(&mut self) {}

    unsafe fn add_memory(&mut self, _region: Frame) -> bool {
        false
    }

    unsafe fn allocate(&mut self, _layout: Layout) -> Option<Frame> {
        None
    }

    unsafe fn deallocate(&mut self, _frame: Frame, _layout: Layout) {}

    fn print_info(&self) {}
}

/// Physical region of memory.
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct Frame {
    pub base: PAddr,
    pub size: usize,
}

impl Frame {
    pub const fn new(base: PAddr, size: usize) -> Frame {
        Frame {
            base: base,
            size: size,
        }
    }

    #[allow(unused)]
    const fn empty() -> Frame {
        Frame {
            base: PAddr::zero(),
            size: 0,
        }
    }

    unsafe fn as_mut_slice<T>(&mut self) -> Option<&mut [T]> {
        if self.size % core::mem::size_of::<T>() == 0 {
            Some(core::slice::from_raw_parts_mut(
                self.kernel_vaddr().as_mut_ptr::<T>(),
                self.size / core::mem::size_of::<T>(),
            ))
        } else {
            None
        }
    }

    #[allow(unused)]
    unsafe fn as_slice<T>(&self) -> Option<&[T]> {
        if self.size % core::mem::size_of::<T>() == 0 {
            Some(core::slice::from_raw_parts(
                self.kernel_vaddr().as_mut_ptr::<T>(),
                self.size / core::mem::size_of::<T>(),
            ))
        } else {
            None
        }
    }

    unsafe fn fill<T: Copy>(&mut self, pattern: T) -> bool {
        self.as_mut_slice::<T>().map_or(false, |obj| {
            for i in 0..obj.len() {
                obj[i] = pattern;
            }
            true
        })
    }

    /// Size of the region (in 4K pages).
    pub fn base_pages(&self) -> usize {
        self.size / BASE_PAGE_SIZE
    }

    /// Size of the region (in bytes).
    pub fn size(&self) -> usize {
        self.size
    }

    pub unsafe fn zero(&mut self) {
        self.fill(0);
    }

    /// The kernel virtual address for this region.
    pub fn kernel_vaddr(&self) -> VAddr {
        paddr_to_kernel_vaddr(self.base)
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Frame {{ 0x{:x} -- 0x{:x} (size = {}, pages = {} }}",
            self.base,
            self.base + self.size,
            self.size,
            self.base_pages()
        )
    }
}

pub trait PageTableProvider<'a> {
    fn allocate_pml4<'b>(&mut self) -> Option<&'b mut paging::PML4>;
    fn new_pdpt(&mut self) -> Option<paging::PML4Entry>;
    fn new_pd(&mut self) -> Option<paging::PDPTEntry>;
    fn new_pt(&mut self) -> Option<paging::PDEntry>;
    fn new_page(&mut self) -> Option<paging::PTEntry>;
}

#[allow(dead_code)]
pub struct BespinPageTableProvider;

impl BespinPageTableProvider {
    #[allow(dead_code)]
    pub const fn new() -> BespinPageTableProvider {
        BespinPageTableProvider
    }
}

impl<'a> PageTableProvider<'a> for BespinPageTableProvider {
    /// Allocate a PML4 table.
    fn allocate_pml4<'b>(&mut self) -> Option<&'b mut paging::PML4> {
        let kcb = super::kcb::get_kcb();
        let mut fmanager = kcb.pmanager();
        unsafe {
            let f = fmanager.allocate(
                Layout::new::<paging::Page>()
                    .align_to(BASE_PAGE_SIZE)
                    .unwrap(),
            );
            f.map(|frame| {
                let pml4: &'b mut [paging::PML4Entry; 512] =
                    transmute(paddr_to_kernel_vaddr(frame.base));
                pml4
            })
        }
    }

    /// Allocate a new page directory and return a PML4 entry for it.
    fn new_pdpt(&mut self) -> Option<paging::PML4Entry> {
        let kcb = super::kcb::get_kcb();
        let mut fmanager = kcb.pmanager();

        unsafe {
            fmanager
                .allocate(
                    Layout::new::<paging::Page>()
                        .align_to(BASE_PAGE_SIZE)
                        .unwrap(),
                )
                .map(|frame| {
                    paging::PML4Entry::new(
                        frame.base,
                        paging::PML4Flags::P | paging::PML4Flags::RW | paging::PML4Flags::US,
                    )
                })
        }
    }

    /// Allocate a new page directory and return a pdpt entry for it.
    fn new_pd(&mut self) -> Option<paging::PDPTEntry> {
        let kcb = super::kcb::get_kcb();
        let mut fmanager = kcb.pmanager();

        unsafe {
            fmanager
                .allocate(
                    Layout::new::<paging::Page>()
                        .align_to(BASE_PAGE_SIZE)
                        .unwrap(),
                )
                .map(|frame| {
                    paging::PDPTEntry::new(
                        frame.base,
                        paging::PDPTFlags::P | paging::PDPTFlags::RW | paging::PDPTFlags::US,
                    )
                })
        }
    }

    /// Allocate a new page-directory and return a page directory entry for it.
    fn new_pt(&mut self) -> Option<paging::PDEntry> {
        let kcb = super::kcb::get_kcb();
        let mut fmanager = kcb.pmanager();

        unsafe {
            fmanager
                .allocate(
                    Layout::new::<paging::Page>()
                        .align_to(BASE_PAGE_SIZE)
                        .unwrap(),
                )
                .map(|frame| {
                    paging::PDEntry::new(
                        frame.base,
                        paging::PDFlags::P | paging::PDFlags::RW | paging::PDFlags::US,
                    )
                })
        }
    }

    /// Allocate a new (4KiB) page and map it.
    fn new_page(&mut self) -> Option<paging::PTEntry> {
        let kcb = super::kcb::get_kcb();
        let mut fmanager = kcb.pmanager();

        unsafe {
            fmanager
                .allocate(
                    Layout::new::<paging::Page>()
                        .align_to(BASE_PAGE_SIZE)
                        .unwrap(),
                )
                .map(|frame| {
                    paging::PTEntry::new(
                        frame.base,
                        paging::PTFlags::P | paging::PTFlags::RW | paging::PTFlags::US,
                    )
                })
        }
    }
}

pub struct BespinSlabsProvider;

unsafe impl Send for BespinSlabsProvider {}
unsafe impl Sync for BespinSlabsProvider {}

impl BespinSlabsProvider {
    pub const fn new() -> BespinSlabsProvider {
        BespinSlabsProvider
    }
}

impl<'a> PageProvider<'a> for BespinSlabsProvider {
    fn allocate_page(&mut self) -> Option<&'a mut ObjectPage<'a>> {
        let kcb = super::kcb::get_kcb();
        let mut fmanager = kcb.pmanager();

        let mut f = unsafe {
            fmanager.allocate(
                Layout::new::<paging::Page>()
                    .align_to(BASE_PAGE_SIZE)
                    .unwrap(),
            )
        };
        f.map(|mut frame| unsafe {
            frame.zero();
            debug!("slabmalloc allocate frame.base = {:x}", frame.base);
            let sp: &'a mut ObjectPage = transmute(paddr_to_kernel_vaddr(frame.base));
            sp
        })
    }

    fn release_page(&mut self, _p: &'a mut ObjectPage<'a>) {
        debug!("TODO!");
    }
}
