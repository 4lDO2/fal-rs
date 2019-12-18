use std::{
    collections::HashMap,
    sync::atomic::{self, AtomicU32, AtomicU64},
    time::SystemTime,
};

use chashmap::CHashMap;
use crc::crc32;
use fal::{time::Timespec, Filesystem as _};

pub mod block_group;
pub mod disk;
pub mod extents;
pub mod htree;
pub mod inode;
pub mod journal;
pub mod superblock;
pub mod xattr;

pub use inode::Inode;
pub use journal::Journal;
pub use superblock::Superblock;

use disk::Disk;
use inode::InodeIoError;
use xattr::XattrEntry;

pub fn allocate_block_bytes(superblock: &Superblock) -> Box<[u8]> {
    vec![0u8; superblock.block_size() as usize].into_boxed_slice()
}

trait ConvertToFalError<T> {
    fn into_fal_result(self, warning_start: &'static str) -> fal::Result<T>;
}
impl<T> ConvertToFalError<T> for Result<T, InodeIoError> {
    fn into_fal_result(self, warning_start: &'static str) -> fal::Result<T> {
        self.map_err(|err| {
            err.into_fal_error_or_with(|err| {
                log::warn!("{}, because of an internal error: {}", warning_start, err)
            })
        })
    }
}

pub use fal::{
    read_u16, read_u32, read_u64, read_u8, read_uuid, write_u16, write_u32, write_u64, write_u8,
    write_uuid,
};

pub struct Filesystem<D> {
    pub superblock: Superblock,
    pub disk: Disk<D>,
    pub(crate) fhs: CHashMap<u64, FileHandle>,
    pub(crate) last_fh: u64,
    pub general_options: fal::Options,
    pub(crate) journal: Option<Journal>,
    pub(crate) info: FsInfo,
}

pub(crate) struct FsInfo {
    pub(crate) free_blocks: AtomicU64,
    pub(crate) free_inodes: AtomicU32,
    pub(crate) kbs_written: AtomicU64,
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

        let num = fh.fh;

        self.fhs.insert(self.last_fh, fh);
        self.last_fh += 1;

        Ok(num)
    }
    fn update_superblock(&mut self) {
        self.superblock
            .set_free_block_count(self.info.free_blocks.load(atomic::Ordering::Release));
        self.superblock
            .set_free_inode_count(self.info.free_inodes.load(atomic::Ordering::Release));
        self.superblock
            .set_kbs_written(self.info.kbs_written.load(atomic::Ordering::Release));
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
            block_count: self.block_count,
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

#[derive(Clone, Debug, PartialEq, Eq)]
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
        path_bytes: &[u8],
    ) -> Self {
        let mut superblock = Superblock::load(&mut device).unwrap();

        superblock.last_mount_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        superblock.mounts_since_fsck += 1;
        superblock.mounts_left_before_fsck -= 1;

        if let Some(extended) = superblock.extended.as_mut() {
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
            disk: Disk::new(device).unwrap(),
            fhs: CHashMap::new(),
            last_fh: 0,
            general_options,
            journal: None,
            info: FsInfo {
                free_blocks: superblock.free_block_count().into(),
                free_inodes: superblock.free_inode_count().into(),
                kbs_written: superblock.kbs_written().unwrap_or(0).into(),
            },
            superblock,
        };
        filesystem.journal = match Journal::load(&filesystem) {
            Ok(j) => j,
            Err(err) => {
                log::warn!("The filesystem journal failed loading: {}", err);
                None
            }
        };
        let mut root = filesystem.load_inode(2).unwrap();
        let mut tree =
            extents::ExtentTree::from_inode_blocks_field(root.checksum_seed, &root.blocks).unwrap();
        extents::allocate_extent_blocks(&filesystem, &mut tree, 1337, 42).unwrap();
        extents::ExtentTree::to_inode_blocks_field(&tree, &mut root.blocks).unwrap();
        filesystem.store_inode(&root).unwrap();
        filesystem
    }

    fn load_inode(&mut self, addr: Self::InodeAddr) -> fal::Result<Self::InodeStruct> {
        Inode::load(self, addr).into_fal_result("Inode failed to load")
    }
    fn open_file(&mut self, addr: Self::InodeAddr) -> fal::Result<u64> {
        self.open(addr, Open::File)
    }
    fn read(&mut self, fh: u64, offset: u64, buffer: &mut [u8]) -> fal::Result<usize> {
        if self.fhs.get(&fh).is_some() {
            let inode: &inode::Inode = &self.fhs.get(&fh).unwrap().inode;

            // Check that the buffer doesn't overflow the inode size.
            let bytes_to_read = std::cmp::min(offset + buffer.len() as u64, inode.size()) - offset;
            let buffer = &mut buffer[..bytes_to_read as usize];

            let bytes_read = inode
                .read(self, offset, buffer)
                .into_fal_result("File couldn't be read")?;

            self.fhs.get_mut(&fh).unwrap().offset += bytes_read as u64;

            Ok(bytes_read)
        } else {
            Err(fal::Error::BadFd)
        }
    }
    fn write(&mut self, fh: u64, offset: u64, buffer: &[u8]) -> fal::Result<u64> {
        if self.fhs.get(&fh).is_some() {
            // There is no need here to check whether the buffer overflows the length, as there
            // will be allocation in that case.
            let mut inode_guard = self.fhs.get_mut(&fh).unwrap();

            inode_guard.inode.write(self, offset, buffer).into_fal_result("File couldn't be written to")?;
            Inode::store(&inode_guard.inode, self).into_fal_result("Inode couldn't be stored when writing to file")?;

            // The return value is the number of bytes written. Unless this driver actually splits the
            // writes depending on the buffer size, which I cannot find any real benefit of doing, the
            // return value will always be the buffer length.
            Ok(buffer.len() as u64)
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
                .dir_entries(self)
                .into_fal_result("Directory couldn't be read")?
                .enumerate()
                .nth(self.fhs.get(&fh).unwrap().offset as usize + offset as usize)
            {
                Some((offset, entry)) => Some({
                    fal::DirectoryEntry {
                        filetype: entry
                            .ty(self)
                            .into_fal_result("File type couldn't be detected")?
                            .into(),
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
        name: &[u8],
    ) -> fal::Result<fal::DirectoryEntry<u32>> {
        let inode = self.load_inode(parent)?;

        if inode.ty() != inode::InodeType::Dir {
            return Err(fal::Error::NotDirectory);
        }

        let (offset, entry) = match inode
            .lookup_direntry(self, name)
            .into_fal_result("Filename couldn't be looked up in directory")?
        {
            Some(inode) => inode,
            None => return Err(fal::Error::NoEntity),
        };

        Ok(fal::DirectoryEntry {
            filetype: entry
                .ty(self)
                .into_fal_result("File type couldn't be detected")?
                .into(),
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

        Ok(inode
            .symlink_target(self)
            .into_fal_result("Failed to resolve symlink")?
            .into_owned()
            .into_boxed_slice())
    }

    fn fh_offset(&self, fh: u64) -> u64 {
        self.fhs.get(&fh).unwrap().offset
    }
    fn fh_inode(&self, fh: u64) -> Inode {
        self.fhs.get(&fh).unwrap().inode.clone()
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

    fn unmount(mut self) {
        if !self.general_options.immutable {
            self.update_superblock();
            self.superblock.store(&mut *self.disk.inner()).unwrap()
        }
    }
    fn store_inode(&mut self, inode: &Inode) -> fal::Result<()> {
        if self.general_options.immutable {
            return Err(fal::Error::ReadonlyFs);
        }
        Inode::store(inode, self).into_fal_result("Failed to write inode")
    }
    fn unlink(&mut self, _parent: u32, _name: &[u8]) -> fal::Result<()> {
        unimplemented!()
    }
    fn get_xattr(&mut self, inode: &Inode, name: &[u8]) -> fal::Result<Vec<u8>> {
        // TODO: Support block-based xattrs as well.
        match inode.xattrs {
            Some(ref x) => x.entries.iter().find(|(k, _)| k.name() == name).ok_or(fal::Error::NoEntity).map(|(_, v)| v.clone()),
            None => Err(fal::Error::NoEntity),
        }
    }
    fn list_xattrs(&mut self, inode: &Inode) -> fal::Result<Vec<Vec<u8>>> {
        // TODO: Support block-based xattrs as well.
        match inode.xattrs {
            Some(ref x) => Ok(x.entries.iter().map(|(entry, _)| entry.name()).collect()),
            None => Ok(vec! []),
        }
    }
}

pub fn calculate_crc32c(value: u32, bytes: &[u8]) -> u32 {
    crc32::update(value ^ (!0), &crc32::CASTAGNOLI_TABLE, bytes) ^ (!0)
}
