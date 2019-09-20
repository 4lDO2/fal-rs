use std::{
    convert::TryInto,
    ffi::{OsStr, OsString},
    fs::File,
    os::unix::ffi::OsStrExt,
    path::{Component, Components, Path},
    sync::{Mutex, MutexGuard},
};

use fal::{FileHandle, Filesystem, Inode};
use syscall::SchemeMut;

#[derive(Debug)]
pub struct RedoxFilesystem<Backend> {
    pub inner: Backend,
}

impl<Backend: fal::Filesystem<File>> RedoxFilesystem<Backend> {
    pub fn init(device: File) -> Self {
        Self {
            inner: Backend::mount(device).into(),
        }
    }
    fn lookup_dir_raw(
        &mut self,
        mut components: Components<'_>,
        parent: Backend::InodeAddr,
    ) -> Backend::InodeAddr {
        match components.next() {
            Some(component) => match component {
                Component::Normal(name) => {
                    let entry = self.inner().lookup_direntry(parent, name).unwrap();
                    self.lookup_dir_raw(components, entry.inode)
                }
                Component::RootDir => self.lookup_dir_raw(components, parent), // ignore Component::RootDir
                _ => panic!("Unsupported component type: {:?}", component),
            },
            None => parent,
        }
    }
    fn inner(&mut self) -> &mut Backend {
        &mut self.inner
    }

    pub fn lookup_dir(&mut self, path: &Path) -> Backend::InodeAddr {
        let root = self.inner().root_inode();
        self.lookup_dir_raw(path.components(), root)
    }
}

fn syscall_error(fal_error: fal::Error) -> syscall::error::Error {
    syscall::error::Error::new(fal_error.errno())
}
fn syscall_result<T>(fal_result: fal::Result<T>) -> syscall::Result<T> {
    fal_result.map_err(|err| syscall_error(err))
}

impl<Backend: fal::Filesystem<File>> SchemeMut for RedoxFilesystem<Backend> {
    fn open(&mut self, path: &[u8], flags: usize, uid: u32, gid: u32) -> syscall::Result<usize> {
        let path = Path::new(OsStr::from_bytes(path));
        let file_inode = self.lookup_dir(&path);

        let inode_struct = syscall_result(self.inner.load_inode(file_inode))?;
        let permissions = fal::check_permissions(uid, gid, &inode_struct.attrs());

        if ((flags & syscall::flag::O_RDONLY != 0) || flags & syscall::flag::O_RDWR != 0) && !permissions.read {
            return syscall_result(Err(fal::Error::AccessDenied));
        }
        if flags & syscall::flag::O_RDWR != 0 && !permissions.write {
            return syscall_result(Err(fal::Error::AccessDenied));
        }
        if flags & syscall::flag::O_EXCL != 0 && !permissions.execute {
            return syscall_result(Err(fal::Error::AccessDenied));
        }

        if flags & syscall::flag::O_DIRECTORY == 0 {
            self.inner()
                .open_file(file_inode)
                .map_err(|err| syscall_error(err))
                .map(|fd| fd as usize)
        } else {
            self.inner()
                .open_directory(file_inode)
                .map_err(|err| syscall_error(err))
                .map(|fd| fd as usize)
        }
    }

    fn read(&mut self, fh: usize, buf: &mut [u8]) -> syscall::Result<usize> {
        let inode = self.inner.fh(fh as u64).inode().clone();

        if inode.attrs().filetype == fal::FileType::Directory {
            // UNOPTIMIZED
            let mut contents = OsString::new();

            let mut offset = 0;

            while let Some(entry) = self.inner().read_directory(fh as u64, offset).unwrap() {
                if offset != 0 {
                    contents.push("\n");
                }
                contents.push(&entry.name);
                offset += 1;
            }

            let offset = self.inner().fh(fh as u64).offset() as usize;
            let len = std::cmp::min(buf.len(), contents.len() - offset);

            if offset >= contents.len() {
                return Ok(0);
            }

            buf[..len].copy_from_slice(&contents.as_bytes()[offset..offset + len]);
            self.inner()
                .fh_mut(fh as u64).set_offset(offset as u64 + len as u64);

            Ok(len)
        } else {
            let inode = self.inner().fh(fh as u64).inode().clone();
            let file_size = inode.attrs().size;
            let offset = self.inner().fh(fh as u64).offset();

            if inode.attrs().filetype == fal::FileType::Directory {
                return syscall_result(Err(fal::Error::IsDirectory));
            }

            let buf_end = std::cmp::min(buf.len(), file_size as usize);
            let buf = &mut buf[..buf_end];

            syscall_result(self.inner().read(fh as u64, offset, buf))
        }
    }

    fn close(&mut self, fh: usize) -> syscall::Result<usize> {
        syscall_result(self.inner.close(fh as u64).map(|_| 0))
    }
    fn chmod(&mut self, path: &[u8], mode: u16, uid: u32, gid: u32) -> syscall::Result<usize> {
        dbg!(OsStr::from_bytes(path), mode, uid, gid);
        dbg!(Ok(0))
    }
    fn rmdir(&mut self, path: &[u8], uid: u32, gid: u32) -> syscall::Result<usize> {
        dbg!(OsStr::from_bytes(path), uid, gid);
        dbg!(Ok(0))
    }
    fn unlink(&mut self, path: &[u8], uid: u32, gid: u32) -> syscall::Result<usize> {
        dbg!(OsStr::from_bytes(path), uid, gid);
        dbg!(Ok(0))
    }
    fn dup(&mut self, old_id: usize, buf: &[u8]) -> syscall::Result<usize> {
        dbg!(old_id, buf.len());
        dbg!(Ok(0))
    }
    fn write(&mut self, id: usize, buf: &[u8]) -> syscall::Result<usize> {
        dbg!(id, buf.len());
        dbg!(Ok(0))
    }

    fn seek(&mut self, fh: usize, pos: usize, whence: usize) -> syscall::Result<usize> {
        let mut offset = self.inner().fh(fh as u64).offset();

        match whence {
            syscall::flag::SEEK_SET => offset = pos as u64,
            syscall::flag::SEEK_END => unimplemented!(),
            syscall::flag::SEEK_CUR => offset += pos as u64,
            _ => return syscall_result(Err(fal::Error::Overflow)),
        }
        Ok(offset as usize)
    }

    fn fchmod(&mut self, id: usize, mode: u16) -> syscall::Result<usize> {
        dbg!(id, mode);
        dbg!(Ok(0))
    }
    fn fchown(&mut self, id: usize, uid: u32, gid: u32) -> syscall::Result<usize> {
        dbg!(id, uid, gid);
        dbg!(Ok(0))
    }
    fn fcntl(&mut self, id: usize, cmd: usize, arg: usize) -> syscall::Result<usize> {
        dbg!(id, cmd, arg);
        dbg!(Ok(0))
    }
    fn fevent(&mut self, id: usize, flags: usize) -> syscall::Result<usize> {
        dbg!(id, flags);
        dbg!(Ok(0))
    }
    fn fmap(&mut self, id: usize, map: &syscall::Map) -> syscall::Result<usize> {
        dbg!(id, map);
        dbg!(Ok(0))
    }
    fn funmap(&mut self, address: usize) -> syscall::Result<usize> {
        dbg!(address);
        dbg!(Ok(0))
    }
    fn fpath(&mut self, id: usize, buf: &mut [u8]) -> syscall::Result<usize> {
        dbg!(id, buf.len());
        dbg!(Ok(0))
    }
    fn frename(&mut self, id: usize, path: &[u8], uid: u32, gid: u32) -> syscall::Result<usize> {
        dbg!(id, OsStr::from_bytes(path), uid, gid);
        dbg!(Ok(0))
    }

    fn fstat(&mut self, id: usize, stat: &mut syscall::Stat) -> syscall::Result<usize> {
        let inode = self.inner().fh(id as u64).inode().clone();
        let attrs: fal::Attributes<_> = inode.attrs();

        *stat = syscall::Stat {
            st_atime: attrs.access_time.sec as u64,
            st_atime_nsec: attrs.access_time.nsec as u32,
            st_blksize: self.inner.filesystem_attrs().block_size,
            st_blocks: attrs.block_count,
            st_ctime: attrs.change_time.sec as u64,
            st_ctime_nsec: attrs.change_time.nsec as u32,
            st_dev: attrs.rdev.into(),
            st_gid: attrs.group_id,
            st_ino: attrs.inode.try_into().unwrap(),
            st_mode: (match attrs.filetype {
                fal::FileType::RegularFile => syscall::MODE_FILE,
                fal::FileType::Directory => syscall::MODE_DIR,
                fal::FileType::Symlink => syscall::MODE_SYMLINK,
                fal::FileType::NamedPipe | fal::FileType::Socket => syscall::MODE_FIFO, // TODO: right?
                fal::FileType::CharacterDevice | fal::FileType::BlockDevice => syscall::MODE_CHR, // TODO: right?
            } & syscall::MODE_TYPE)
                | (attrs.permissions & syscall::MODE_PERM), // TODO: setuid/setgid
            st_mtime: attrs.modification_time.sec as u64,
            st_mtime_nsec: attrs.modification_time.nsec as u32,
            st_nlink: attrs.hardlink_count.try_into().unwrap(),
            st_size: attrs.size,
            st_uid: attrs.user_id,
        };
        Ok(0)
    }

    fn fstatvfs(&mut self, _id: usize, stat: &mut syscall::StatVfs) -> syscall::Result<usize> {
        // FIXME: This functions is probably about a single file, but this function returns the
        // stat of the entire fs.

        let fs_attrs: fal::FsAttributes = self.inner.filesystem_attrs();

        *stat = syscall::StatVfs {
            f_bavail: fs_attrs.available_blocks.into(),
            f_bfree: fs_attrs.free_blocks.into(),
            f_blocks: fs_attrs.total_blocks.into(),
            f_bsize: fs_attrs.block_size,
        };
        Ok(0)
    }

    fn fsync(&mut self, id: usize) -> syscall::Result<usize> {
        dbg!(id);
        dbg!(Ok(0))
    }
    fn ftruncate(&mut self, id: usize, len: usize) -> syscall::Result<usize> {
        dbg!(id, len);
        dbg!(Ok(0))
    }
    fn futimens(&mut self, id: usize, times: &[syscall::TimeSpec]) -> syscall::Result<usize> {
        dbg!(id, times);
        dbg!(Ok(0))
    }
}
