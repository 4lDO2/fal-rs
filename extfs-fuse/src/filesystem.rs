use std::{
    collections::HashMap,
    convert::TryFrom, io,
    ffi::OsStr,
    fs::File,
};

use extfs::{Filesystem, Inode, inode::InodeType};
use fuse::{FileAttr, Request, ReplyAttr, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyDirectory};
use time::Timespec;

struct FileHandle {
    inode: u64,
    position: u64,
    dir: bool,
}

pub struct FuseFilesystem {
    inner: Filesystem<File>,
    file_handles: HashMap<u64, FileHandle>,
    last_fh: u64,
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
        InodeType::Unknown => panic!("Unknown file"),
    }
}

impl FuseFilesystem {
    pub fn init(device: File) -> io::Result<Self> {
        Ok(Self {
            inner: Filesystem::mount(device)?,
            file_handles: HashMap::new(),
            last_fh: 0,
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
                reply.error(libc::EMFILE);
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
                reply.error(libc::EMFILE);
                return
            }
        };

        let inode_struct = match Inode::load(&mut self.inner, parent_inode) {
            Ok(inode_struct) => inode_struct,
            Err(error) => {
                reply.error(error.raw_os_error().unwrap()); // FIXME
                return
            }
        };
        let entries = inode_struct.ls(&mut self.inner).unwrap();

        // TODO: Give the inode a lookup function, instead of storing every value into a vec.
        let entry = match entries.iter().find(|entry| entry.name == name) {
            Some(entry) => entry,
            None => {
                reply.error(libc::ENOENT);
                return
            }
        };

        let entry_inode_struct = match Inode::load(&mut self.inner, entry.inode) {
            Ok(inode) => inode,
            error => error.unwrap(), // FIXME
        };

        let dir_attributes = self.file_attributes(entry.inode, &entry_inode_struct);

        let validity_timeout = Timespec::new(0, 0); // TODO: Is this correct?

        reply.entry(&validity_timeout, &dir_attributes, u64::from(entry_inode_struct.generation_number)); // TODO: generation_number?
    }
    fn opendir(&mut self, _req: &Request, fuse_inode: u64, _flags: u32, reply: ReplyOpen) {
        let inode = match fuse_inode_to_extfs_inode(fuse_inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::EMFILE);
                return
            }
        };
        let fh = self.fh();
        self.file_handles.insert(fh, FileHandle { inode: u64::from(inode), position: 0, dir: true });
        reply.opened(fh, 0);
    }
    fn readdir(&mut self, _req: &Request, fuse_inode: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        dbg!(offset);
        let inode = match u32::try_from(fuse_inode) {
            Ok(1) => 2,
            Ok(inode) => inode,
            Err(_) => {
                reply.error(libc::EMFILE);
                return
            }
        };
        let file_handle = &mut self.file_handles.get_mut(&fh).unwrap();

        assert_eq!(u64::from(inode), file_handle.inode);

        let entries = Inode::load(&mut self.inner, inode).unwrap();
        if let Some(entry) = entries.ls(&mut self.inner).unwrap().into_iter().nth(file_handle.position as usize) {
            let new_position = i64::try_from(file_handle.position).unwrap() + offset;
            file_handle.position = u64::try_from(new_position).unwrap();
            reply.add(fuse_inode_from_extfs_inode(entry.inode), 1, fuse_filetype(Inode::load(&mut self.inner, entry.inode).unwrap().ty), entry.name);
        }
        reply.ok()
    }
    fn releasedir(&mut self, _req: &Request, _fuse_inode: u64, fh: u64, _flags: u32, reply: ReplyEmpty) {
        self.file_handles.remove(&fh);
        reply.ok()
    }
    fn open(&mut self, _req: &Request, _inode: u64, _fh: u32, reply: ReplyOpen) {
        reply.error(libc::ENOSYS);
    }
}
