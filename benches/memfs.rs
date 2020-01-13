// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use std::ffi::OsStr;

use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};

mod mkbench;
mod utils;

use criterion::{criterion_group, criterion_main, Criterion};
use log::warn;
use node_replication::Dispatch;

use btfs::{Error, FileAttr, FileType, InodeId, MemFilesystem, SetAttrRequest};

/// All FS operations we can perform through the log.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Operation {
    GetAttr {
        ino: u64,
    },
    SetAttr {
        ino: u64,
        new_attrs: SetAttrRequest,
    },
    ReadDir {
        ino: u64,
        fh: u64,
        offset: i64,
    },
    Lookup {
        parent: u64,
        name: &'static OsStr,
    },
    RmDir {
        parent: u64,
        name: &'static OsStr,
    },
    MkDir {
        parent: u64,
        name: &'static OsStr,
        mode: u32,
    },
    Open {
        ino: u64,
        flags: u32,
    },
    Unlink {
        parent: u64,
        name: &'static OsStr,
    },
    Create {
        parent: u64,
        name: &'static OsStr,
        mode: u32,
        flags: u32,
    },
    Write {
        ino: u64,
        fh: u64,
        offset: i64,
        data: &'static [u8],
        flags: u32,
    },
    Read {
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
    },
    Rename {
        parent: u64,
        name: &'static OsStr,
        newparent: u64,
        newname: &'static OsStr,
    },
}

/// Potential responses from the file-system
#[derive(Copy, Clone)]
pub enum Response {
    Attr(FileAttr),
    Directory,
    Entry,
    Empty,
    Open,
    Create,
    Written(u64),
    Data(&'static [u8]),
}

impl Default for Response {
    fn default() -> Response {
        Response::Empty
    }
}

/// Potential errors from the file-system
#[derive(Copy, Clone)]
pub enum ResponseError {
    Err(Error),
}

impl Default for ResponseError {
    fn default() -> ResponseError {
        ResponseError::Err(Error::NoEntry)
    }
}

struct NrMemFilesystem(MemFilesystem);

impl Default for NrMemFilesystem {
    fn default() -> NrMemFilesystem {
        let mut memfs = MemFilesystem::new();

        fn setup_initial_structure(memfs: &mut MemFilesystem) -> Result<(), Error> {
            let ino = 1; // TODO: hard-coded root inode, get through a lookup()
            let ino = memfs.mkdir(ino, &OsStr::new("tmp"), 0)?.ino;
            let ino = memfs.mkdir(ino, &OsStr::new("largefile1"), 0)?.ino;

            let ino = memfs.create(ino, &OsStr::new("00000001"), 0, 0)?.ino;
            memfs.write(ino, 0, 0, &[1; 4096], 0)?;
            assert_eq!(ino, 5, "Adjust `generate_fs_operations` accordingly!");
            Ok(())
        }

        setup_initial_structure(&mut memfs).expect("Can't initialize FS");

        NrMemFilesystem(memfs)
    }
}

impl NrMemFilesystem {
    pub fn getattr(&mut self, ino: u64) -> Result<&FileAttr, Error> {
        self.0.getattr(ino)
    }

    pub fn setattr(&mut self, ino: u64, new_attrs: SetAttrRequest) -> Result<&FileAttr, Error> {
        self.0.setattr(ino, new_attrs)
    }

    pub fn readdir(&mut self, ino: u64, fh: u64) -> Result<Vec<(u64, FileType, String)>, Error> {
        self.0.readdir(ino, fh)
    }

    pub fn lookup(&mut self, parent: u64, name: &OsStr) -> Result<&FileAttr, Error> {
        self.0.lookup(parent, name)
    }

    pub fn rmdir(&mut self, parent: u64, name: &OsStr) -> Result<(), Error> {
        self.0.rmdir(parent, name)
    }

    pub fn mkdir(&mut self, parent: u64, name: &OsStr, mode: u32) -> Result<&FileAttr, Error> {
        self.0.mkdir(parent, name, mode)
    }

    pub fn unlink(&mut self, parent: u64, name: &OsStr) -> Result<(), Error> {
        self.0.unlink(parent, name)
    }

    pub fn create(
        &mut self,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<&FileAttr, Error> {
        self.0.create(parent, name, mode, flags)
    }

    pub fn write(
        &mut self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        flags: u32,
    ) -> Result<u64, Error> {
        self.0.write(ino, fh, offset, data, flags)
    }

    pub fn read(&mut self, ino: u64, fh: u64, offset: i64, size: u32) -> Result<&[u8], Error> {
        self.0.read(ino, fh, offset, size)
    }

    pub fn rename(
        &mut self,
        parent: u64,
        current_name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> Result<(), Error> {
        self.0.rename(parent, current_name, new_parent, new_name)
    }
}

impl Dispatch for NrMemFilesystem {
    type Operation = Operation;
    type Response = Response;
    type ResponseError = ResponseError;

    /// Implements how we execute operation from the log against our local stack
    fn dispatch(&mut self, op: Self::Operation) -> Result<Self::Response, Self::ResponseError> {
        match op {
            Operation::GetAttr { ino } => match self.getattr(ino) {
                Ok(attr) => Ok(Response::Attr(*attr)),
                Err(e) => Err(ResponseError::Err(e)),
            },
            Operation::SetAttr { ino, new_attrs } => match self.setattr(ino, new_attrs) {
                Ok(fattr) => Ok(Response::Attr(*fattr)),
                Err(e) => Err(ResponseError::Err(e)),
            },
            Operation::ReadDir { ino, fh, offset } => {
                match self.readdir(ino, fh) {
                    Ok(entries) => {
                        // Offset of 0 means no offset.
                        // Non-zero offset means the passed offset has already been seen,
                        // and we should start after it.
                        let to_skip = if offset == 0 { 0 } else { offset + 1 } as usize;
                        let _entries: Vec<(InodeId, FileType, String)> =
                            entries.into_iter().skip(to_skip).collect();
                        Ok(Response::Directory)
                    }
                    Err(e) => Err(ResponseError::Err(e)),
                }
            }
            Operation::Lookup { parent, name } => match self.lookup(parent, name) {
                Ok(attr) => Ok(Response::Attr(*attr)),
                Err(e) => Err(ResponseError::Err(e)),
            },
            Operation::RmDir { parent, name } => match self.rmdir(parent, name) {
                Ok(()) => Ok(Response::Empty),
                Err(e) => Err(ResponseError::Err(e)),
            },
            Operation::MkDir { parent, name, mode } => match self.mkdir(parent, name, mode) {
                Ok(attr) => Ok(Response::Attr(*attr)),
                Err(e) => Err(ResponseError::Err(e)),
            },
            Operation::Open { ino, flags } => {
                warn!("Don't do `open` for now... {} {}", ino, flags);
                Ok(Response::Empty)
            }
            Operation::Unlink { parent, name } => match self.unlink(parent, name) {
                Ok(_attr) => Ok(Response::Empty),
                Err(e) => Err(ResponseError::Err(e)),
            },
            Operation::Create {
                parent,
                name,
                mode,
                flags,
            } => match self.create(parent, name, mode, flags) {
                Ok(_attr) => Ok(Response::Empty),
                Err(e) => Err(ResponseError::Err(e)),
            },
            Operation::Write {
                ino,
                fh,
                offset,
                data,
                flags,
            } => match self.write(ino, fh, offset, data, flags) {
                Ok(written) => Ok(Response::Written(written)),
                Err(e) => Err(ResponseError::Err(e)),
            },
            Operation::Read {
                ino,
                fh,
                offset,
                size,
            } => match self.read(ino, fh, offset, size) {
                Ok(slice) => {
                    // TODO: We make a heap allocation for the data, thn leak it below
                    // since we currently don't have a good way to return non copy things to clients:
                    let bytes_as_vec = slice.to_vec();

                    // create an alias to bytes as vec so we can leak:
                    let slice = unsafe {
                        std::slice::from_raw_parts(bytes_as_vec.as_ptr(), bytes_as_vec.len())
                    };
                    let response = Response::Data(slice);

                    std::mem::forget(bytes_as_vec); // XXX leaking here
                    Ok(response)
                }
                Err(e) => Err(ResponseError::Err(e)),
            },
            Operation::Rename {
                parent,
                name,
                newparent,
                newname,
            } => match self.rename(parent, name, newparent, newname) {
                Ok(()) => Ok(Response::Empty),
                Err(e) => Err(ResponseError::Err(e)),
            },
        }
    }
}

fn generate_fs_operations(nop: usize, write_ratio: usize) -> Vec<Operation> {
    let mut ops = Vec::with_capacity(nop);
    let mut rng = rand::thread_rng();

    for idx in 0..nop {
        if idx % 100 < write_ratio {
            ops.push(Operation::Write {
                ino: 5, // XXX: hard-coded ino of file `00000001`
                fh: 0,
                offset: rng.gen_range(0, 4096 - 256),
                data: &[3; 128],
                flags: 0,
            })
        } else {
            let offset = rng.gen_range(0, 4096 - 256);
            let size = rng.gen_range(0, 128);

            ops.push(Operation::Read {
                ino: 5, // XXX: hard-coded ino of file `00000001`
                fh: 0,
                offset: offset,
                size: size,
            })
        }
    }

    ops.shuffle(&mut thread_rng());
    ops
}

fn memfs_single_threaded(c: &mut Criterion) {
    let _r = env_logger::try_init();

    const LOG_SIZE_BYTES: usize = 16 * 1024 * 1024;
    const NOP: usize = 100;
    const WRITE_RATIO: usize = 10; //% out of 100

    let ops = generate_fs_operations(NOP, WRITE_RATIO);
    mkbench::baseline_comparison::<NrMemFilesystem>(c, "memfs", ops, LOG_SIZE_BYTES);
}

/// Compare scale-out behaviour of memfs.
fn memfs_scale_out(c: &mut Criterion) {
    let _r = env_logger::try_init();

    const NOP: usize = 100;
    const WRITE_RATIO: usize = 10; //% out of 100

    let ops = generate_fs_operations(NOP, WRITE_RATIO);

    mkbench::ScaleBenchBuilder::<NrMemFilesystem>::new(ops)
        .machine_defaults()
        // The only benchmark that actually seems to slightly
        // regress with 2 MiB logsize, set to 16 MiB
        .log_size(16 * 1024 * 1024)
        .configure(
            c,
            "memfs-scaleout",
            |_cid, rid, _log, replica, ops, _batch_size| {
                let mut o = vec![];
                for op in ops {
                    replica.execute(*op, rid);
                    let mut i = 1;
                    while replica.get_responses(rid, &mut o) == 0 {
                        if i % mkbench::WARN_THRESHOLD == 0 {
                            log::warn!(
                                "{:?} Waiting too long for get_responses",
                                std::thread::current().id()
                            );
                        }
                        i += 1;
                    }
                    o.clear();
                }
            },
        );
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = memfs_single_threaded, memfs_scale_out
);

criterion_main!(benches);
