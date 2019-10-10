// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![allow(unused_imports, unused)]
#![feature(result_map_or_else)]

use std::ffi::OsStr;

mod mkbench;
mod utils;

use criterion::{criterion_group, criterion_main, Criterion};
use log::warn;
use node_replication::Dispatch;

use btfs::{Error, FileAttr, FileType, InodeId, MemFilesystem, SetAttrRequest};

/// All FS operations we can perform through the log.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
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
    Invalid,
}

/// Default operations, don't do anything
impl Default for Operation {
    fn default() -> Operation {
        Operation::Invalid
    }
}

/// Potential returns from the file-system
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
    // XXX: this is a bit of a mess atm. once we return a Result<> as part of the log
    // drop this and the associated glue-code
    Err(Error),
    Invalid,
}

impl Default for Response {
    fn default() -> Response {
        Response::Invalid
    }
}

struct NrMemFilesystem(MemFilesystem);

impl Default for NrMemFilesystem {
    fn default() -> NrMemFilesystem {
        let memfs = MemFilesystem::new();
        // TODO Add an initial directory tree here:

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

    /// Implements how we execute operation from the log against our local stack
    fn dispatch(&mut self, op: Self::Operation) -> Self::Response {
        match op {
            Operation::GetAttr { ino } => match self.getattr(ino) {
                Ok(attr) => Response::Attr(*attr),
                Err(e) => Response::Err(e),
            },
            Operation::SetAttr { ino, new_attrs } => match self.setattr(ino, new_attrs) {
                Ok(fattr) => Response::Attr(*fattr),
                Err(e) => Response::Err(e),
            },
            Operation::ReadDir { ino, fh, offset } => {
                match self.readdir(ino, fh) {
                    Ok(entries) => {
                        // Offset of 0 means no offset.
                        // Non-zero offset means the passed offset has already been seen,
                        // and we should start after it.
                        let to_skip = if offset == 0 { 0 } else { offset + 1 } as usize;
                        let entries: Vec<(InodeId, FileType, String)> =
                            entries.into_iter().skip(to_skip).collect();
                        Response::Directory
                    }
                    Err(e) => Response::Err(e),
                }
            }
            Operation::Lookup { parent, name } => match self.lookup(parent, name) {
                Ok(attr) => Response::Attr(*attr),
                Err(e) => Response::Err(e),
            },
            Operation::RmDir { parent, name } => match self.rmdir(parent, name) {
                Ok(()) => Response::Empty,
                Err(e) => Response::Err(e),
            },
            Operation::MkDir { parent, name, mode } => match self.mkdir(parent, name, mode) {
                Ok(attr) => Response::Attr(*attr),
                Err(e) => Response::Err(e),
            },
            Operation::Open { ino, flags } => {
                warn!("Don't do `open` for now...");
                Response::Empty
            }
            Operation::Unlink { parent, name } => match self.unlink(parent, name) {
                Ok(attr) => Response::Empty,
                Err(e) => Response::Err(e),
            },
            Operation::Create {
                parent,
                name,
                mode,
                flags,
            } => match self.create(parent, name, mode, flags) {
                Ok(attr) => Response::Empty,
                Err(e) => Response::Err(e),
            },
            Operation::Write {
                ino,
                fh,
                offset,
                data,
                flags,
            } => match self.write(ino, fh, offset, data, flags) {
                Ok(written) => Response::Written(written),
                Err(e) => Response::Err(e),
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
                    response
                }
                Err(e) => Response::Err(e),
            },
            Operation::Rename {
                parent,
                name,
                newparent,
                newname,
            } => match self.rename(parent, name, newparent, newname) {
                Ok(()) => Response::Empty,
                Err(e) => Response::Err(e),
            },
            Operation::Invalid => unreachable!("Got invalid OP"),
        }
    }
}

fn memfs_single_threaded(c: &mut Criterion) {
    // Use a 10 GiB log size
    const LOG_SIZE_BYTES: usize = 10 * 1024 * 1024 * 1024;
    let ops = vec![];
    mkbench::baseline_comparison::<NrMemFilesystem>(c, "memfs", ops, LOG_SIZE_BYTES);
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = memfs_single_threaded
);

criterion_main!(benches);
