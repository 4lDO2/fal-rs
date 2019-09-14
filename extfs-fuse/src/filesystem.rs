use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto}, io,
    ffi::OsStr,
    fs::File,
};

use extfs::{Filesystem, fs_core::Filesystem as _, Inode, inode::InodeType};
use fuse::{FileAttr, Request, ReplyAttr, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyDirectory, ReplyData};
use time::Timespec;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AccessTime {
    Atime,
    Noatime,
    Relatime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Options {
    write: bool,
    execute: bool,
    access_time: AccessTime,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            write: true,
            execute: true,
            access_time: AccessTime::Atime,
        }
    }
}
#[derive(Debug)]
pub enum OptionsParseError<'a> {
    UnknownOption(&'a str),
}
impl std::fmt::Display for OptionsParseError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let OptionsParseError::UnknownOption(option) = self;
        write!(f, "unknown option: `{}`", option)
    }
}
impl std::error::Error for OptionsParseError<'_> {}

impl Options {
    pub fn parse<'a>(options_str: &'a str) -> Result<Self, OptionsParseError<'a>> { 
        let mut options = Self::default();

        for option in options_str.split(',') {
            match option {
                "ro" => options.write = false,
                "rw" => options.write = true,
                "exec" => options.execute = true,
                "noexec" => options.execute = false,
                "atime" => options.access_time = AccessTime::Atime,
                "noatime" => options.access_time = AccessTime::Noatime,
                "relatime" => options.access_time = AccessTime::Relatime,
                other => return Err(OptionsParseError::UnknownOption(other))
            }
        }

        Ok(options)
    }
}

struct FileHandle {
    inode: u32,
    inode_struct: Inode,
    position: u64,
}

pub struct FuseFilesystem {
    inner: Filesystem<File>,
    file_handles: HashMap<u64, FileHandle>,
    last_fh: u64,
    options: Options,
}

fn fuse_filetype(ty: InodeType) -> fuse::FileType {
    match ty {
        InodeType::BlockDev => fuse::FileType::BlockDevice,
        InodeType::CharDev => fuse::FileType::CharDevice,
        InodeType::Dir => fuse::FileType::Directory,
        InodeType::File => fuse::FileType::RegularFile,
        InodeType::Fifo => fuse::FileType::NamedPipe,
        InodeType::Symlink => fuse::FileType::Symlink,
        InodeType::UnixSock => fuse::FileType::Socket,
        InodeType::Unknown => fuse::FileType::RegularFile,
    }
}

impl FuseFilesystem {
    pub fn init(device: File, options: Options) -> io::Result<Self> {
        Ok(Self {
            inner: Filesystem::mount(device),
            file_handles: HashMap::new(),
            last_fh: 0,
            options,
        })
    }
    fn file_attributes(&self, inode: u32, inode_struct: &Inode) -> FileAttr {
        FileAttr {
            ino: u64::from(inode),
            size: inode_struct.size(&self.inner.superblock),
            atime: Timespec::new(i64::from(inode_struct.last_access_time), 0),
            mtime: Timespec::new(i64::from(inode_struct.last_modification_time), 0),
            ctime: Timespec::new(i64::from(inode_struct.last_modification_time), 0), // Is there a ctime on ext2?
            crtime: Timespec::new(i64::from(inode_struct.creation_time), 0),
            uid: u32::from(inode_struct.uid),
            gid: u32::from(inode_struct.gid),
            kind: fuse_filetype(inode_struct.ty),
            flags: 0,
            nlink: u32::from(inode_struct.hard_link_count),
            perm: inode_struct.permissions,
            rdev: 0, // TODO
            blocks: inode_struct.size_in_blocks(&self.inner.superblock),
        }
    }
    fn fh(&mut self) -> u64 {
        let fh = self.last_fh;
        self.last_fh += 1;
        fh
    }
}

fn fuse_inode_to_extfs_inode(fuse_inode: u64) -> Option<u32> {
    u32::try_from(fuse_inode).ok().map(|fuse_inode| if fuse_inode == 1 { 2 } else { fuse_inode })
}
fn fuse_inode_from_extfs_inode(extfs_inode: u32) -> u64 {
    if extfs_inode == 2 { 1 } else { extfs_inode as u64 }
}

impl fuse::Filesystem for FuseFilesystem {
    fn init(&mut self, _req: &Request) -> Result<(), libc::c_int> {
        Ok(())
    }
    fn destroy(&mut self, _req: &Request) {
    }

    fn getattr(&mut self, _req: &Request, fuse_inode: u64, reply: ReplyAttr) {
        let inode = match fuse_inode_to_extfs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return
            }
        };
        let exists = extfs::block_group::inode_exists(inode as u32, &mut self.inner).unwrap();
        if !exists {
            reply.error(libc::ENOENT);
            return
        }

        let inode_struct = match Inode::load(&mut self.inner, inode) {
            Ok(inode_struct) => inode_struct,
            Err(error) => {
                reply.error(error.raw_os_error().unwrap());
                return
            }
        };

        let file_attributes = self.file_attributes(inode, &inode_struct);

        let validity_timeout = Timespec::new(0, 0); // TODO: Is this correct?

        reply.attr(&validity_timeout, &file_attributes);
    }
    fn lookup(&mut self, _req: &Request, parent_inode: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_inode = match fuse_inode_to_extfs_inode(parent_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return
            }
        };
        if !extfs::block_group::inode_exists(parent_inode, &mut self.inner).unwrap() {
            reply.error(libc::ENOENT);
            return
        }

        let inode_struct = match Inode::load(&mut self.inner, parent_inode) {
            Ok(inode_struct) => inode_struct,
            Err(_) => {
                reply.error(libc::EIO);
                return
            }
        };
        let entry = match inode_struct.dir_entries(&mut self.inner).unwrap().find(|entry| entry.name == name) {
            Some(entry) => entry,
            None => {
                reply.error(libc::ENOENT);
                return
            }
        };

        let entry_inode_struct = match Inode::load(&mut self.inner, entry.inode) {
            Ok(inode) => inode,
            Err(_) => {
                reply.error(libc::EIO); 
                return
            }
        };

        let dir_attributes = self.file_attributes(entry.inode, &entry_inode_struct);

        let validity_timeout = Timespec::new(0, 0); // TODO: Is this correct?

        reply.entry(&validity_timeout, &dir_attributes, u64::from(entry_inode_struct.generation_number)); // TODO: generation_number?
    }
    fn opendir(&mut self, _req: &Request, fuse_inode: u64, _flags: u32, reply: ReplyOpen) {
        let inode = match fuse_inode_to_extfs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return
            }
        };
        if !extfs::block_group::inode_exists(inode, &mut self.inner).unwrap() {
            reply.error(libc::ENOENT);
            return
        }
        let inode_struct = match Inode::load(&mut self.inner, inode) {
            Ok(inode_struct) => inode_struct,
            Err(_) => {
                reply.error(libc::EIO);
                return
            }
        };
        if inode_struct.ty != InodeType::Dir {
            reply.error(libc::ENOTDIR);
            return
        }
        let fh = self.fh();
        self.file_handles.insert(fh, FileHandle { inode, position: 0, inode_struct });
        reply.opened(fh, 0);
    }
    fn readdir(&mut self, _req: &Request, fuse_inode: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        let inode = match fuse_inode_to_extfs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return
            }
        };
        if !extfs::block_group::inode_exists(inode, &mut self.inner).unwrap() {
            reply.error(libc::ENOENT);
            return
        }
        let file_handle = match self.file_handles.get_mut(&fh) {
            Some(file_handle) => file_handle,
            None => {
                reply.error(libc::ENOENT);
                return
            }
        };

        assert_eq!(inode, file_handle.inode);

        if let Some(entry) = file_handle.inode_struct.dir_entries(&mut self.inner).unwrap().nth(file_handle.position as usize) {
            let new_position = i64::try_from(file_handle.position).unwrap() + offset;
            file_handle.position = u64::try_from(new_position).unwrap();
            reply.add(fuse_inode_from_extfs_inode(entry.inode), 1, fuse_filetype(Inode::load(&mut self.inner, entry.inode).unwrap().ty), entry.name);
        }
        reply.ok()
    }
    fn releasedir(&mut self, _req: &Request, _fuse_inode: u64, fh: u64, _flags: u32, reply: ReplyEmpty) {
        if self.file_handles.remove(&fh).is_some() {
            reply.ok()
        } else {
            reply.error(libc::EBADF);
        }
    }
    fn open(&mut self, _req: &Request, fuse_inode: u64, _flags: u32, reply: ReplyOpen) {
        let inode = match fuse_inode_to_extfs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return
            }
        };
        if !extfs::block_group::inode_exists(inode, &mut self.inner).unwrap() {
            reply.error(libc::ENOENT);
            return
        }
        let inode_struct = match Inode::load(&mut self.inner, inode) {
            Ok(inode_struct) => inode_struct,
            Err(_) => {
                reply.error(libc::EIO);
                return
            }
        };
        let fh = self.fh();
        self.file_handles.insert(fh, FileHandle { inode, position: 0, inode_struct });
        reply.opened(fh, 0);
    }
    fn read(&mut self, _req: &Request, fuse_inode: u64, fh: u64, offset: i64, size: u32, reply: ReplyData) {
        let inode = match fuse_inode_to_extfs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return
            }
        };
        let offset = match u64::try_from(offset) {
            Ok(offset) => offset,
            Err(_) => {
                // According to IEEE Std 1003.1-2017, the offset cannot be negative for regular files
                // or block devices when using pread.
                reply.error(libc::EINVAL);
                return
            }
        };
        if !extfs::block_group::inode_exists(inode, &mut self.inner).unwrap() {
            reply.error(libc::ENOENT);
            return
        }

        let file_handle = match self.file_handles.get(&fh) {
            Some(file_handle) => file_handle,
            None => {
                reply.error(libc::EBADF);
                return
            }
        };

        let inode_size = file_handle.inode_struct.size(&self.inner.superblock);

        // This will effectively be the size, but possibly reduced to prevent overflow.
        let bytes_to_read = std::cmp::min(offset + u64::from(size), inode_size) - offset;

        let mut buffer = vec![0u8; bytes_to_read.try_into().unwrap()];

        file_handle.inode_struct.read(&mut self.inner, offset, &mut buffer).unwrap();

        reply.data(&buffer);
    }
    fn release(&mut self, _req: &Request, _fuse_inode: u64, fh: u64, _flags: u32, _lock_owned: u64, _flush: bool, reply: ReplyEmpty) {
        if self.file_handles.remove(&fh).is_some() {
            reply.ok();
        } else {
            reply.error(libc::EBADF);
        }
    }
    fn readlink(&mut self, _req: &Request, fuse_inode: u64, reply: ReplyData) {
        let inode = match fuse_inode_to_extfs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return
            }
        };
        if !extfs::block_group::inode_exists(inode, &mut self.inner).unwrap() {
            reply.error(libc::ENOENT);
            return
        }
        let inode_struct = match Inode::load(&mut self.inner, inode) {
            Ok(inode_struct) => inode_struct,
            Err(_) => {
                reply.error(libc::EIO);
                return
            }
        };
        if inode_struct.ty != InodeType::Symlink {
            reply.error(libc::EINVAL);
            return
        }
        inode_struct.with_symlink_target(&mut self.inner, |result| match result {
            Ok(data) => reply.data(data),
            Err(_) => reply.error(libc::EIO),
        });
    }
    fn unlink(&mut self, _req: &Request, fuse_parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent = match fuse_inode_to_extfs_inode(fuse_parent) {
            Some(parent) => parent,
            None => {
                reply.error(libc::EOVERFLOW);
                return
            }
        };
        let parent_struct = match Inode::load(&mut self.inner, parent) {
            Ok(parent_struct) => parent_struct,
            Err(error) => {
                eprintln!("Error when unlinking: loading parent struct: {}.", error);
                reply.error(libc::EIO);
                return
            }
        };
        match parent_struct.remove_entry(&mut self.inner, name) {
            Ok(()) => (),
            Err(error) => {
                eprintln!("Error when unlinking: removing entry: {}.", error);
                reply.error(libc::EIO);
                return
            }
        };
        reply.ok();
    }
}
