use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto}, io,
    ffi::OsStr,
    fs::File,
};

use extfs::{Filesystem, fs_core::{Filesystem as _, Inode as _}, Inode, inode::InodeType};
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

fn fuse_filetype(ty: extfs::fs_core::FileType) -> fuse::FileType {
    use fs_core::FileType;

    match ty {
        FileType::BlockDevice => fuse::FileType::BlockDevice,
        FileType::CharacterDevice => fuse::FileType::CharDevice,
        FileType::Directory => fuse::FileType::Directory,
        FileType::RegularFile => fuse::FileType::RegularFile,
        FileType::NamedPipe => fuse::FileType::NamedPipe,
        FileType::Symlink => fuse::FileType::Symlink,
        FileType::Socket => fuse::FileType::Socket,
    }
}

fn fuse_attr(attrs: fs_core::Attributes<u32>) -> fuse::FileAttr {
    fuse::FileAttr {
        atime: attrs.access_time,
        blocks: attrs.block_count,
        crtime: attrs.creation_time,
        ctime: attrs.change_time,
        flags: attrs.flags,
        gid: attrs.group_id,
        ino: attrs.inode.into(),
        kind: fuse_filetype(attrs.filetype),
        mtime: attrs.modification_time,
        nlink: attrs.hardlink_count.try_into().unwrap(),
        perm: attrs.permissions,
        rdev: 0, // TODO
        size: attrs.size,
        uid: attrs.user_id,
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

        let file_attributes = match self.inner.getattrs(inode) {
            Ok(attrs) => fuse_attr(attrs),
            Err(err) => {
                match err {
                    extfs::fs_core::Error::NoEntity => reply.error(libc::ENOENT),
                    extfs::fs_core::Error::Io(_) => reply.error(libc::EIO),
                    extfs::fs_core::Error::BadFd => unreachable!(),
                }
                return
            }
        };

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

        let entry = match self.inner.lookup_direntry(parent_inode, name) {
            Ok(entry) => entry,
            Err(err) => {
                match err {
                    fs_core::Error::BadFd => unreachable!(),
                    fs_core::Error::NoEntity => reply.error(libc::ENOENT),
                    fs_core::Error::Io(_) => reply.error(libc::EIO),
                }
                return
            }
        };

        let inode_struct = match Inode::load(&mut self.inner, entry.inode as u32) {
            Ok(inode_struct) => inode_struct,
            Err(err) => {
                match err {
                    fs_core::Error::BadFd => unreachable!(),
                    fs_core::Error::NoEntity => reply.error(libc::ENOENT),
                    fs_core::Error::Io(_) => reply.error(libc::EIO),
                };
                return
            }
        };

        let validity_timeout = Timespec::new(0, 0); // TODO: Is this correct?

        reply.entry(&validity_timeout, &fuse_attr(self.inner.inode_attrs(entry.inode as u32, &inode_struct)), inode_struct.generation_number().unwrap_or(0)); // TODO: generation_number?
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
