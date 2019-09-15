pub extern crate fs_core;

use std::{
    collections::HashMap,
    convert::TryInto,
    ffi::{OsStr, OsString},
    io::{self, prelude::*, SeekFrom},
    ops::{Add, Div, Mul, Rem},
    sync::Mutex,
};

use uuid::Uuid;

use fs_core::{Filesystem as _, time::Timespec};

pub mod block_group;
pub mod inode;
pub mod superblock;

pub use inode::Inode;
pub use superblock::Superblock;

pub use fs_core::{read_u8, read_u16, read_u32, read_u64, read_uuid, write_u8, write_u16, write_u32, write_u64, write_uuid};

fn read_block_to<D: fs_core::Device>(
    filesystem: &Filesystem<D>,
    block_address: u32,
    buffer: &mut [u8],
) -> io::Result<()> {
    debug_assert!(block_group::block_exists(block_address, filesystem)?);
    read_block_to_raw(filesystem, block_address, buffer)
}
fn read_block_to_raw<D: fs_core::Device>(
    filesystem: &Filesystem<D>,
    block_address: u32,
    buffer: &mut [u8],
) -> io::Result<()> {
    filesystem.device.lock().unwrap().seek(SeekFrom::Start(
        block_address as u64 * filesystem.superblock.block_size,
    ))?;
    filesystem.device.lock().unwrap().read_exact(buffer)?;
    Ok(())
}
fn read_block<D: fs_core::Device>(
    filesystem: &Filesystem<D>,
    block_address: u32,
) -> io::Result<Box<[u8]>> {
    let mut vector = vec![0; filesystem.superblock.block_size.try_into().unwrap()];
    read_block_to(filesystem, block_address, &mut vector)?;
    Ok(vector.into_boxed_slice())
}
fn write_block_raw<D: fs_core::DeviceMut>(filesystem: &Filesystem<D>, block_address: u32, buffer: &[u8]) -> io::Result<()> {
    filesystem.device.lock().unwrap().seek(SeekFrom::Start(block_address as u64 * filesystem.superblock.block_size))?;
    filesystem.device.lock().unwrap().write_all(buffer)?;
    Ok(())
}
fn write_block<D: fs_core::DeviceMut>(filesystem: &Filesystem<D>, block_address: u32, buffer: &[u8]) -> io::Result<()> {
    debug_assert!(block_group::block_exists(block_address, filesystem)?);
    write_block_raw(filesystem, block_address, buffer)
}
fn div_round_up<T>(numer: T, denom: T) -> T
where
    T: Add<Output = T> + Copy + Div<Output = T> + Rem<Output = T> + From<u8> + PartialEq,
{
    if numer % denom != T::from(0u8) {
        numer / denom + T::from(1u8)
    } else {
        numer / denom
    }
}
pub fn round_up<T>(number: T, to: T) -> T
where
    T: Add<Output = T> + Copy + Div<Output = T> + Mul<Output = T> + Rem<Output = T> + From<u8> + PartialEq,
{
    div_round_up(number, to) * number
}

fn os_string_from_bytes(bytes: &[u8]) -> OsString {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        OsString::from_vec(Vec::from(bytes))
    }
    #[cfg(windows)]
    {
        String::from_utf8_lossy(Vec::from(bytes)).into()
    }
}
fn os_str_to_bytes(string: &OsStr) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        string.as_bytes().into()
    }
    #[cfg(windows)]
    {
        string.to_string_lossy().as_bytes().into()
    }
}

pub struct Filesystem<D: fs_core::Device> {
    pub superblock: Superblock,
    pub device: Mutex<D>,
    pub fhs: HashMap<u64, FileHandle>,
    last_fh: u64,
}

impl fs_core::Inode for Inode {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FileHandle {
    fh: u64,
    inode_addr: u32,
    inode: Inode,
}

impl<D: fs_core::Device> fs_core::Filesystem<D> for Filesystem<D> {
    type InodeAddr = u32;
    type InodeStruct = Inode;
    type FileHandle = FileHandle;
    type DirHandle = FileHandle;

    fn mount(mut device: D) -> Self {
        Self {
            superblock: Superblock::parse(&mut device).unwrap(),
            device: Mutex::new(device),
            fhs: HashMap::new(),
            last_fh: 0,
        }
    }
    fn load_inode(&mut self, addr: Self::InodeAddr) -> Self::InodeStruct {
        Inode::load(self, addr).unwrap()
    }
    fn open_file(&mut self, addr: Self::InodeAddr) -> FileHandle {
        let fh = FileHandle {
            inode_addr: addr,
            fh: self.last_fh,
            inode: Inode::load(self, addr).unwrap(),
        };
        self.fhs.insert(self.last_fh, fh);
        self.last_fh += 1;

        fh
    }
    fn read(&mut self, fh: &FileHandle, offset: u64, buffer: &mut [u8]) -> usize {
        fh.inode.read(self, offset, buffer).unwrap()
    }
    fn close_file(&mut self, fh: Self::FileHandle) {
        // FIXME: Flush before closing.
        self.fhs.remove(&fh.fh);
    }
    fn getattrs(&mut self, inode_addr: u32) -> fs_core::Attributes<u32> {
        use inode::InodeType;

        let inode = Inode::load(self, inode_addr).unwrap();
        fs_core::Attributes {
            access_time: Timespec {
                sec: inode.last_access_time.into(),
                nsec: 0,
            },
            change_time: Timespec {
                sec: inode.last_modification_time.into(),
                nsec: 0,
            },
            creation_time: Timespec {
                sec: inode.creation_time.into(),
                nsec: 0,
            },
            modification_time: Timespec {
                sec: inode.last_modification_time.into(),
                nsec: 0,
            },
            filetype: inode.ty.into(),
            block_count: div_round_up(inode.size(&self.superblock), self.superblock.block_size),
            flags: inode.flags,
            group_id: inode.gid.into(),
            hardlink_count: inode.hard_link_count.into(),
            inode: inode_addr.into(),
            permissions: inode.permissions,
            rdev: 0,
            size: inode.size(&self.superblock),
            user_id: inode.uid.into(),
        }
    }
    fn open_directory(&mut self, inode: u32) -> FileHandle {
        self.open_file(inode)
    }
    fn read_directory(&mut self, handle: &FileHandle, offset: i64) -> fs_core::DirectoryEntry {
        let (offset, entry) = handle.inode.dir_entries(self).unwrap().enumerate().skip(offset as usize).next().unwrap();

        fs_core::DirectoryEntry {
            filetype: entry.ty(self).into(),
            name: entry.name,
            inode: entry.inode.into(),
            offset: offset as u64,
        }
    }
    fn lookup_direntry(&mut self, parent: u32, name: &OsStr) -> fs_core::DirectoryEntry {
        let (offset, entry) = Inode::load(self, parent).unwrap().dir_entries(self).unwrap().enumerate().next().unwrap();

        fs_core::DirectoryEntry {
            filetype: entry.ty(self).into(),
            name: entry.name,
            inode: entry.inode.into(),
            offset: offset as u64,
        }
    }
    fn close_directory(&mut self, handle: FileHandle) {
        self.close_file(handle)
    }
}
