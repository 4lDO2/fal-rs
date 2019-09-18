use std::{
    convert::{TryFrom, TryInto},
    ffi::OsStr,
    fs::File,
    io,
};

use fuse::{
    ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, Request,
};
use time::Timespec;

use fal::Inode;

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
                other => return Err(OptionsParseError::UnknownOption(other)),
            }
        }

        Ok(options)
    }
}

pub struct FuseFilesystem<Backend: fal::Filesystem<File>> {
    inner: Backend,
    options: Options,
}

fn fuse_filetype(ty: fal::FileType) -> fuse::FileType {
    use fal::FileType;

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

fn fuse_attr<InodeAddr: Into<u64>>(attrs: fal::Attributes<InodeAddr>) -> fuse::FileAttr {
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

impl<Backend: fal::Filesystem<File>> FuseFilesystem<Backend> {
    pub fn init(device: File, options: Options) -> io::Result<Self> {
        Ok(Self {
            inner: Backend::mount(device),
            options,
        })
    }
}

fn fuse_inode_to_fs_inode<InodeAddr: From<u32> + TryFrom<u64> + Eq>(
    fuse_inode: u64,
) -> Option<InodeAddr> {
    InodeAddr::try_from(fuse_inode).ok().map(|fuse_inode| {
        if fuse_inode == 1.into() {
            2.into()
        } else {
            fuse_inode
        }
    })
}
fn fuse_inode_from_fs_inode<InodeAddr: Into<u64> + Copy>(fs_inode: InodeAddr) -> u64 {
    if fs_inode.into() == 2 {
        1
    } else {
        fs_inode.into()
    }
}

impl<Backend: fal::Filesystem<File>> fuse::Filesystem for FuseFilesystem<Backend> {
    fn init(&mut self, _req: &Request) -> Result<(), libc::c_int> {
        Ok(())
    }
    fn destroy(&mut self, _req: &Request) {}

    fn getattr(&mut self, _req: &Request, fuse_inode: u64, reply: ReplyAttr) {
        let inode = match fuse_inode_to_fs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return;
            }
        };

        let file_attributes = match self.inner.getattrs(inode) {
            Ok(attrs) => fuse_attr(attrs),
            Err(err) => {
                reply.error(err.errno());
                return;
            }
        };

        let validity_timeout = Timespec::new(0, 0); // TODO: Is this correct?

        reply.attr(&validity_timeout, &file_attributes);
    }
    fn lookup(&mut self, _req: &Request, parent_inode: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_inode = match fuse_inode_to_fs_inode(parent_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return;
            }
        };

        let entry = match self.inner.lookup_direntry(parent_inode, name) {
            Ok(entry) => entry,
            Err(err) => {
                reply.error(err.errno());
                return;
            }
        };

        let inode_struct = match self.inner.load_inode(entry.inode.try_into().unwrap()) {
            Ok(inode_struct) => inode_struct,
            Err(err) => {
                reply.error(err.errno());
                return;
            }
        };

        let validity_timeout = Timespec::new(0, 0); // TODO: Is this correct?

        reply.entry(
            &validity_timeout,
            &fuse_attr(self.inner.inode_attrs(&inode_struct)),
            inode_struct.generation_number().unwrap_or(0),
        ); // TODO: generation_number?
    }
    fn opendir(&mut self, _req: &Request, fuse_inode: u64, _flags: u32, reply: ReplyOpen) {
        let inode = match fuse_inode_to_fs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return;
            }
        };
        let fh = match self.inner.open_directory(inode) {
            Ok(fh) => fh,
            Err(err) => {
                reply.error(err.errno());
                return;
            }
        };
        reply.opened(fh, 0);
    }
    fn readdir(
        &mut self,
        _req: &Request,
        fuse_inode: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let inode: Backend::InodeAddr = match fuse_inode_to_fs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return;
            }
        };

        assert_eq!(inode, self.inner.inode_attrs(self.inner.fh_inode(fh)).inode);

        match self.inner.read_directory(fh, offset) {
            Ok(Some(entry)) => {
                reply.add(
                    fuse_inode_from_fs_inode(entry.inode),
                    0,
                    fuse_filetype(entry.filetype),
                    entry.name,
                );
            }
            Ok(None) => (),
            Err(err) => {
                reply.error(err.errno());
                return;
            }
        }
        reply.ok();
    }
    fn releasedir(
        &mut self,
        _req: &Request,
        _fuse_inode: u64,
        fh: u64,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        self.inner.close(fh);
        reply.ok();
    }
    fn open(&mut self, _req: &Request, fuse_inode: u64, _flags: u32, reply: ReplyOpen) {
        let inode = match fuse_inode_to_fs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return;
            }
        };
        let fh = match self.inner.open_file(inode) {
            Ok(fh) => fh,
            Err(err) => {
                reply.error(err.errno());
                return;
            }
        };
        reply.opened(fh, 0);
    }
    fn read(
        &mut self,
        _req: &Request,
        fuse_inode: u64,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        let inode: Backend::InodeAddr = match fuse_inode_to_fs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return;
            }
        };
        let offset = match u64::try_from(offset) {
            Ok(offset) => offset,
            Err(_) => {
                // According to IEEE Std 1003.1-2017, the offset cannot be negative for regular files
                // or block devices when using pread.
                reply.error(libc::EINVAL);
                return;
            }
        };

        let inode_struct = self.inner.fh_inode(fh);

        assert_eq!(inode, self.inner.inode_attrs(inode_struct).inode);

        let inode_size = self.inner.inode_attrs(inode_struct).size;

        // This will effectively be the size, but possibly reduced to prevent overflow.
        let bytes_to_read = std::cmp::min(offset + u64::from(size), inode_size) - offset;

        let mut buffer = vec![0u8; bytes_to_read.try_into().unwrap()];

        self.inner.read(fh, offset, &mut buffer).unwrap();

        reply.data(&buffer);
    }
    fn release(
        &mut self,
        _req: &Request,
        _fuse_inode: u64,
        fh: u64,
        _flags: u32,
        _lock_owned: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.inner.close(fh).unwrap();
        reply.ok();
    }
    fn readlink(&mut self, _req: &Request, fuse_inode: u64, reply: ReplyData) {
        let inode = match fuse_inode_to_fs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EOVERFLOW);
                return;
            }
        };
        let data = match self.inner.readlink(inode) {
            Ok(data) => data,
            Err(err) => {
                reply.error(err.errno());
                return;
            }
        };
        reply.data(&data);
    }
    fn statfs(&mut self, _req: &Request, _inode: u64, reply: ReplyStatfs) {
        let stat: fal::FsAttributes = self.inner.filesystem_attrs();
        reply.statfs(
            stat.total_blocks.into(),
            stat.free_blocks.into(),
            stat.available_blocks.into(),
            stat.inode_count,
            stat.free_inodes,
            stat.block_size,
            stat.max_fname_len,
            stat.block_size,
        );
    }
    fn unlink(&mut self, _req: &Request, fuse_parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent = match fuse_inode_to_fs_inode(fuse_parent) {
            Some(parent) => parent,
            None => {
                reply.error(libc::EOVERFLOW);
                return;
            }
        };
        let parent_struct = match self.inner.load_inode(parent) {
            Ok(parent_struct) => parent_struct,
            Err(error) => {
                eprintln!("Error when unlinking: loading parent struct: {}.", error);
                reply.error(libc::EIO);
                return;
            }
        };
        /*match parent_struct.remove_entry(&mut self.inner, name) {
            Ok(()) => (),
            Err(error) => {
                eprintln!("Error when unlinking: removing entry: {}.", error);
                reply.error(libc::EIO);
                return;
            }
        };*/
        unimplemented!();
        reply.ok();
    }
}
