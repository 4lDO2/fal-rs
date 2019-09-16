use std::{
    collections::HashMap,
    convert::TryInto,
    ffi::{OsStr, OsString},
    io::{self, prelude::*, SeekFrom},
    ops::{Add, Div, Mul, Rem},
    sync::Mutex,
};

use uuid::Uuid;

use fal::{time::Timespec, Filesystem as _};

pub mod block_group;
pub mod inode;
pub mod superblock;

pub use inode::Inode;
pub use superblock::Superblock;

pub use fal::{
    read_u16, read_u32, read_u64, read_u8, read_uuid, write_u16, write_u32, write_u64, write_u8,
    write_uuid,
};

fn read_block_to<D: fal::Device>(
    filesystem: &Filesystem<D>,
    block_address: u32,
    buffer: &mut [u8],
) -> io::Result<()> {
    debug_assert!(block_group::block_exists(block_address, filesystem)?);
    read_block_to_raw(filesystem, block_address, buffer)
}
fn read_block_to_raw<D: fal::Device>(
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
fn read_block<D: fal::Device>(
    filesystem: &Filesystem<D>,
    block_address: u32,
) -> io::Result<Box<[u8]>> {
    let mut vector = vec![0; filesystem.superblock.block_size.try_into().unwrap()];
    read_block_to(filesystem, block_address, &mut vector)?;
    Ok(vector.into_boxed_slice())
}
fn write_block_raw<D: fal::DeviceMut>(
    filesystem: &Filesystem<D>,
    block_address: u32,
    buffer: &[u8],
) -> io::Result<()> {
    filesystem.device.lock().unwrap().seek(SeekFrom::Start(
        block_address as u64 * filesystem.superblock.block_size,
    ))?;
    filesystem.device.lock().unwrap().write_all(buffer)?;
    Ok(())
}
fn write_block<D: fal::DeviceMut>(
    filesystem: &Filesystem<D>,
    block_address: u32,
    buffer: &[u8],
) -> io::Result<()> {
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
    T: Add<Output = T>
        + Copy
        + Div<Output = T>
        + Mul<Output = T>
        + Rem<Output = T>
        + From<u8>
        + PartialEq,
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

pub struct Filesystem<D: fal::Device> {
    pub superblock: Superblock,
    pub device: Mutex<D>,
    pub fhs: HashMap<u64, FileHandle>,
    last_fh: u64,
}

fn inode_attrs(inode: &Inode, addr: u32, superblock: &Superblock) -> fal::Attributes<u32> {
    fal::Attributes {
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
        block_count: div_round_up(inode.size(&superblock), superblock.block_size),
        flags: inode.flags,
        group_id: inode.gid.into(),
        hardlink_count: inode.hard_link_count.into(),
        inode: addr.into(),
        permissions: inode.permissions,
        rdev: 0,
        size: inode.size(&superblock),
        user_id: inode.uid.into(),
    }
}

impl fal::Inode for Inode {
    fn generation_number(&self) -> Option<u64> {
        Some(self.generation_number.into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FileHandle {
    fh: u64,
    offset: u64,
    inode_addr: u32,
    inode: Inode,
}

impl fal::FileHandle for FileHandle {
    fn fd(&self) -> u64 {
        self.fh
    }
}

impl<D: fal::Device> fal::Filesystem<D> for Filesystem<D> {
    type InodeAddr = u32;
    type InodeStruct = Inode;

    #[inline]
    fn root_inode(&self) -> u32 {
        2
    }

    fn mount(mut device: D) -> Self {
        Self {
            superblock: Superblock::parse(&mut device).unwrap(),
            device: Mutex::new(device),
            fhs: HashMap::new(),
            last_fh: 0,
        }
    }
    fn load_inode(&mut self, addr: Self::InodeAddr) -> fal::Result<Self::InodeStruct> {
        Ok(Inode::load(self, addr)?)
    }
    fn open_file(&mut self, addr: Self::InodeAddr) -> fal::Result<u64> {
        let fh = FileHandle {
            inode_addr: addr,
            fh: self.last_fh,
            inode: Inode::load(self, addr)?,
            offset: 0,
        };
        self.fhs.insert(self.last_fh, fh);
        self.last_fh += 1;

        Ok(fh.fh)
    }
    fn read(&mut self, fh: u64, offset: u64, buffer: &mut [u8]) -> fal::Result<usize> {
        match self.fhs.get(&fh) {
            Some(fh) => Ok(fh.inode.read(self, offset, buffer)?),
            None => Err(fal::Error::BadFd),
        }
    }
    fn close_file(&mut self, fh: u64) {
        // FIXME: Flush before closing.
        self.fhs.remove(&fh);
    }
    fn getattrs(&mut self, inode_addr: u32) -> fal::Result<fal::Attributes<u32>> {
        let inode = Inode::load(self, inode_addr)?;

        Ok(inode_attrs(&inode, inode_addr, &self.superblock))
    }
    fn open_directory(&mut self, inode: u32) -> fal::Result<u64> {
        self.open_file(inode)
    }
    fn read_directory(
        &mut self,
        fh: u64,
        offset: i64,
    ) -> fal::Result<Option<fal::DirectoryEntry<u32>>> {
        let handle = match self.fhs.get(&fh) {
            Some(handle) => handle,
            None => return Err(fal::Error::BadFd),
        };
        Ok(
            match handle
                .inode
                .dir_entries(self)?
                .enumerate()
                .skip(self.fhs[&fh].offset as usize + offset as usize)
                .next()
            {
                Some((offset, entry)) => Some({
                    let entry = fal::DirectoryEntry {
                        filetype: entry.ty(self)?.into(),
                        name: entry.name,
                        inode: entry.inode.into(),
                        offset: offset as u64,
                    };
                    self.fhs.get_mut(&fh).unwrap().offset = offset as u64 + 1;
                    entry
                }),
                None => None,
            },
        )
    }
    fn lookup_direntry(
        &mut self,
        parent: u32,
        name: &OsStr,
    ) -> fal::Result<fal::DirectoryEntry<u32>> {
        let (offset, entry) = Inode::load(self, parent)?
            .dir_entries(self)?
            .enumerate()
            .find(|(_, entry)| entry.name == name)
            .unwrap();

        Ok(fal::DirectoryEntry {
            filetype: entry.ty(self)?.into(),
            name: entry.name,
            inode: entry.inode.into(),
            offset: offset as u64,
        })
    }
    fn close_directory(&mut self, handle: u64) {
        self.close_file(handle)
    }
    fn inode_attrs(&self, addr: u32, inode: &Inode) -> fal::Attributes<u32> {
        inode_attrs(inode, addr, &self.superblock)
    }
    fn fh_inode(&self, fh: u64) -> &'_ Inode {
        &self.fhs[&fh].inode
    }
    fn readlink(&mut self, inode: u32) -> fal::Result<Box<[u8]>> {
        let mut location = None;
        let mut error = None;

        Inode::load(self, inode)?.with_symlink_target(self, |result| match result {
            Ok(data) => location = Some(data.to_owned()),
            Err(err) => error = Some(err),
        });

        if let Some(err) = error {
            return Err(err.into());
        }

        Ok(location.unwrap().into_boxed_slice())
    }
    fn fh_offset(&self, fh: u64) -> u64 {
        self.fhs[&fh].offset
    }
}
