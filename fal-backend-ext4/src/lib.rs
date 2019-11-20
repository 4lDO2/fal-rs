use std::{
    collections::HashMap,
    convert::TryInto,
    ffi::{OsStr, OsString},
    io::{self, SeekFrom},
    os::unix::ffi::OsStrExt,
    sync::Mutex,
    time::SystemTime,
};

use crc::{crc32, Hasher64};
use fal::{time::Timespec, Filesystem as _};

pub mod block_group;
pub mod extents;
pub mod inode;
pub mod journal;
pub mod superblock;

pub use inode::Inode;
pub use journal::Journal;
pub use superblock::Superblock;

pub use fal::{
    read_u16, read_u32, read_u64, read_u8, read_uuid, write_u16, write_u32, write_u64, write_u8,
    write_uuid,
};

fn read_block_to<D: fal::Device>(
    filesystem: &Filesystem<D>,
    block_address: u64,
    buffer: &mut [u8],
) -> io::Result<()> {
    debug_assert!(block_group::block_exists(block_address, filesystem)?);
    read_block_to_raw(filesystem, block_address, buffer)
}
fn read_block_to_raw<D: fal::Device>(
    filesystem: &Filesystem<D>,
    block_address: u64,
    buffer: &mut [u8],
) -> io::Result<()> {
    let mut guard = filesystem.device.lock().unwrap();
    guard.seek(SeekFrom::Start(
        block_address * u64::from(filesystem.superblock.block_size()),
    ))?;
    guard.read_exact(buffer)?;
    Ok(())
}
fn read_block<D: fal::Device>(
    filesystem: &Filesystem<D>,
    block_address: u64,
) -> io::Result<Box<[u8]>> {
    let mut vector = vec![0; filesystem.superblock.block_size().try_into().unwrap()];
    read_block_to(filesystem, block_address, &mut vector)?;
    Ok(vector.into_boxed_slice())
}
fn write_block_raw<D: fal::DeviceMut>(
    filesystem: &Filesystem<D>,
    block_address: u64,
    buffer: &[u8],
) -> io::Result<()> {
    let mut guard = filesystem.device.lock().unwrap();
    guard.seek(SeekFrom::Start(
        block_address as u64 * u64::from(filesystem.superblock.block_size()),
    ))?;
    guard.write_all(buffer)?;
    Ok(())
}
fn write_block<D: fal::DeviceMut>(
    filesystem: &Filesystem<D>,
    block_address: u64,
    buffer: &[u8],
) -> io::Result<()> {
    debug_assert!(block_group::block_exists(block_address, filesystem)?);
    write_block_raw(filesystem, block_address, buffer)
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
        string.as_bytes().into()
    }
    #[cfg(windows)]
    {
        string.to_string_lossy().as_bytes().into()
    }
}

pub struct Filesystem<D> {
    pub superblock: Superblock,
    pub device: Mutex<D>,
    pub fhs: HashMap<u64, FileHandle>,
    pub last_fh: u64,
    pub general_options: fal::Options,
    pub journal: Option<Journal>,
}

#[derive(Debug, PartialEq)]
enum Open {
    File,
    Directory,
}

impl<D: fal::DeviceMut> Filesystem<D> {
    fn open(&mut self, addr: u32, ty: Open) -> fal::Result<u64> {
        let fh = FileHandle {
            fh: self.last_fh,
            inode: self.load_inode(addr)?,
            offset: 0,
        };

        if ty == Open::Directory && fh.inode.ty() == inode::InodeType::File {
            return Err(fal::Error::NotDirectory);
        }

        self.fhs.insert(self.last_fh, fh);
        self.last_fh += 1;

        Ok(fh.fh)
    }
}

impl fal::Inode for Inode {
    type InodeAddr = u32;

    #[inline]
    fn generation_number(&self) -> Option<u64> {
        Some(self.generation())
    }
    #[inline]
    fn addr(&self) -> u32 {
        self.addr
    }
    fn attrs(&self) -> fal::Attributes<u32> {
        fal::Attributes {
            access_time: self.a_time(),
            change_time: self.c_time(),
            creation_time: self.cr_time().unwrap_or(Timespec { sec: 0, nsec: 0 }),
            modification_time: self.m_time(),
            filetype: self.ty().into(),
            block_count: self.size_in_blocks(),
            flags: self.flags,
            group_id: self.gid.into(),
            hardlink_count: self.hardlink_count.into(),
            inode: self.addr,
            permissions: self.permissions(),
            rdev: 0,
            size: self.size,
            user_id: self.uid.into(),
        }
    }
    #[inline]
    fn set_perm(&mut self, permissions: u16) {
        self.set_permissions(permissions)
    }

    #[inline]
    fn set_uid(&mut self, uid: u32) {
        self.raw.set_uid(uid, self.os)
    }

    #[inline]
    fn set_gid(&mut self, gid: u32) {
        self.raw.set_gid(gid, self.os)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FileHandle {
    fh: u64,
    offset: u64,
    inode: Inode,
}

impl<D: fal::DeviceMut> fal::Filesystem<D> for Filesystem<D> {
    type InodeAddr = u32;
    type InodeStruct = Inode;
    type Options = ();

    #[inline]
    fn root_inode(&self) -> u32 {
        2
    }

    fn mount(
        mut device: D,
        general_options: fal::Options,
        _ext_specific_options: (),
        path: &OsStr,
    ) -> Self {
        let mut superblock = Superblock::load(&mut device).unwrap();

        superblock.last_mount_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        superblock.mounts_since_fsck += 1;
        superblock.mounts_left_before_fsck -= 1;

        if let Some(extended) = superblock.extended.as_mut() {
            let path_bytes = path.as_bytes();
            extended.last_mount_path[..path_bytes.len()].copy_from_slice(path_bytes);

            // NUL
            if path_bytes.len() < 64 {
                extended.last_mount_path[63] = 0;
            }
        }

        if !general_options.immutable {
            superblock.store(&mut device).unwrap()
        }

        // TODO: Check for feature flags here.

        let mut filesystem = Self {
            superblock,
            device: Mutex::new(device),
            fhs: HashMap::new(),
            last_fh: 0,
            general_options,
            journal: None,
        };
        filesystem.journal = match Journal::load(&filesystem) {
            Ok(j) => dbg!(j),
            Err(err) => {
                eprintln!("The filesystem journal failed loading: {}", err);
                None
            }
        };
        filesystem
    }

    fn unmount(self) {}

    fn load_inode(&mut self, addr: Self::InodeAddr) -> fal::Result<Self::InodeStruct> {
        Inode::load(self, addr)
    }
    fn open_file(&mut self, addr: Self::InodeAddr) -> fal::Result<u64> {
        self.open(addr, Open::File)
    }
    fn read(&mut self, fh: u64, offset: u64, buffer: &mut [u8]) -> fal::Result<usize> {
        if self.fhs.get(&fh).is_some() {
            let inode: inode::Inode = self.fhs[&fh].inode;

            let bytes_read = inode.read(self, offset, buffer)?;

            self.fhs.get_mut(&fh).unwrap().offset += bytes_read as u64;

            Ok(bytes_read)
        } else {
            Err(fal::Error::BadFd)
        }
    }
    fn close(&mut self, fh: u64) -> fal::Result<()> {
        // FIXME: Flush before closing.
        match self.fhs.remove(&fh) {
            Some(_) => Ok(()),
            None => Err(fal::Error::BadFd),
        }
    }
    fn open_directory(&mut self, inode: u32) -> fal::Result<u64> {
        self.open(inode, Open::Directory)
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

        if handle.inode.ty() != inode::InodeType::Dir {
            return Err(fal::Error::NotDirectory);
        }

        Ok(
            match handle
                .inode
                .dir_entries(self)?
                .enumerate()
                .nth(self.fhs[&fh].offset as usize + offset as usize)
            {
                Some((offset, entry)) => Some({
                    fal::DirectoryEntry {
                        filetype: entry.ty(self)?.into(),
                        name: entry.name,
                        inode: entry.inode,
                        offset: offset as u64,
                    }
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
        let inode = self.load_inode(parent)?;

        if inode.ty() != inode::InodeType::Dir {
            return Err(fal::Error::NotDirectory);
        }

        let (offset, entry) = match inode
            .dir_entries(self)?
            .enumerate()
            .find(|(_, entry)| entry.name == name)
        {
            Some(inode) => inode,
            None => return Err(fal::Error::NoEntity),
        };

        Ok(fal::DirectoryEntry {
            filetype: entry.ty(self)?.into(),
            name: entry.name,
            inode: entry.inode,
            offset: offset as u64,
        })
    }
    fn readlink(&mut self, inode: u32) -> fal::Result<Box<[u8]>> {
        let inode = self.load_inode(inode)?;

        if inode.ty() != inode::InodeType::Symlink {
            return Err(fal::Error::Invalid);
        }

        Ok(inode.symlink_target(self)?)
    }

    fn fh_offset(&self, fh: u64) -> u64 {
        self.fhs[&fh].offset
    }
    fn fh_inode(&self, fh: u64) -> Inode {
        self.fhs[&fh].inode
    }

    fn set_fh_offset(&mut self, fh: u64, offset: u64) {
        self.fhs.get_mut(&fh).unwrap().offset = offset;
    }

    fn filesystem_attrs(&self) -> fal::FsAttributes {
        fal::FsAttributes {
            block_size: self.superblock.block_size(),
            free_blocks: self.superblock.unalloc_block_count.into(),
            available_blocks: self.superblock.unalloc_block_count.into(), // TODO: What role does reserved_block_count have?
            free_inodes: self.superblock.unalloc_inode_count.into(),
            inode_count: self.superblock.inode_count.into(),
            total_blocks: self.superblock.block_count.into(),
            max_fname_len: 255,
        }
    }
}

impl<D: fal::DeviceMut> fal::FilesystemMut<D> for Filesystem<D> {
    fn unmount(self) {
        self.superblock
            .store(&mut *self.device.lock().unwrap())
            .unwrap()
    }
    fn store_inode(&mut self, inode: &Inode) -> fal::Result<()> {
        if self.general_options.immutable {
            return Err(fal::Error::ReadonlyFs);
        }
        Inode::store(inode, self)
    }
    fn unlink(&mut self, _parent: u32, _name: &OsStr) -> fal::Result<()> {
        unimplemented!()
    }
}

pub fn calculate_crc32c(value: u32, bytes: &[u8]) -> u32 {
    crc32::update(value ^ (!0), &crc32::CASTAGNOLI_TABLE, bytes) ^ (!0)
}

