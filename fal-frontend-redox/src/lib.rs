use std::{ffi::{OsStr, OsString}, fs::File, path::{Path, Component, Components}, os::unix::ffi::OsStrExt, sync::{Mutex, MutexGuard}};

use fal::Filesystem;
use syscall::Scheme;

pub struct RedoxFilesystem<Backend> {
    pub inner: Mutex<Backend>,
}

impl<Backend: fal::Filesystem<File>> RedoxFilesystem<Backend> {
    pub fn init(device: File) -> Self {
        Self {
            inner: Backend::mount(device).into(),
        }
    }
    fn lookup_dir_raw(&self, mut components: Components<'_>, parent: Backend::InodeAddr) -> Backend::InodeAddr {
        match components.next() {
            Some(component) => match component {
                Component::Normal(name) => {
                    dbg!(component);
                    let entry = self.inner().lookup_direntry(parent, name).unwrap();
                    self.lookup_dir_raw(components, entry.inode)
                }
                _ => panic!("Unsupported component type: {:?}", component),
            }
            None => parent,
        }
    }
    fn inner(&self) -> MutexGuard<'_, Backend> {
        self.inner.lock().unwrap()
    }

    pub fn lookup_dir(&self, path: &Path) -> Backend::InodeAddr {
        self.lookup_dir_raw(path.components(), self.inner().root_inode())
    }
}

fn syscall_error(fal_error: fal::Error) -> syscall::error::Error {
    syscall::error::Error::new(fal_error.errno())
}
fn syscall_result<T>(fal_result: fal::Result<T>) -> syscall::Result<T> {
    fal_result.map_err(|err| syscall_error(err))
}

impl<Backend: fal::Filesystem<File>> Scheme for RedoxFilesystem<Backend> {

    fn open(&self, path: &[u8], flags: usize, uid: u32, gid: u32) -> syscall::Result<usize> {
        dbg!(OsStr::from_bytes(path), flags, uid, gid);
        let path = Path::new(OsStr::from_bytes(path));
        let file_inode = self.lookup_dir(&path);

        dbg!(file_inode);

        if flags & syscall::flag::O_DIRECTORY == 0 {
            dbg!();
            self.inner().open_file(file_inode).map_err(|err| syscall_error(err)).map(|fd| fd as usize)
        } else {
            dbg!();
            self.inner().open_directory(file_inode).map_err(|err| syscall_error(err)).map(|fd| fd as usize)
        }
    }
    
    fn read(&self, fh: usize, buf: &mut [u8]) -> syscall::Result<usize> {
        dbg!(fh, buf.len());
        if self.inner().inode_attrs(0.into(), self.inner().fh_inode(fh as u64)).filetype == fal::FileType::Directory {
            // UNOPTIMIZED
            let mut contents = OsString::new();

            let entry = self.inner().read_directory(fh as u64, 0).unwrap().unwrap();
            contents.push(&entry.name);

            let offset = self.inner().fh_offset(fh as u64) as usize;
            let len = std::cmp::min(buf.len(), contents.len() - offset);

            buf[..len].copy_from_slice(&contents.as_bytes()[offset..offset + len]);

            dbg!(OsStr::from_bytes(&buf));

            Ok(len)
        } else {
            syscall_result(self.inner().read(fh as u64, self.inner().fh_offset(fh as u64), buf))
        }
    }

    fn close(&self, fh: usize) -> syscall::Result<usize> {
        dbg!(fh);
        if let fal::FileType::Directory =  self.inner().inode_attrs(0.into(), self.inner().fh_inode(fh as u64)).filetype {
            dbg!();
            self.inner().close_directory(fh as u64);
        } else {
            dbg!();
            self.inner().close_file(fh as u64);
        }
        Ok(0)
    }
    fn chmod(&self, path: &[u8], mode: u16, uid: u32, gid: u32) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn rmdir(&self, path: &[u8], uid: u32, gid: u32) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn unlink(&self, path: &[u8], uid: u32, gid: u32) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn dup(&self, old_id: usize, buf: &[u8]) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn write(&self, id: usize, buf: &[u8]) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn seek(&self, id: usize, pos: usize, whence: usize) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn fchmod(&self, id: usize, mode: u16) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn fchown(&self, id: usize, uid: u32, gid: u32) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn fcntl(&self, id: usize, cmd: usize, arg: usize) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn fevent(&self, id: usize, flags: usize) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn fmap(&self, id: usize, map: &syscall::Map) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn funmap(&self, address: usize) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn fpath(&self, id: usize, buf: &mut [u8]) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn frename(&self, id: usize, path: &[u8], uid: u32, gid: u32) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn fstat(&self, id: usize, stat: &mut syscall::Stat) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn fstatvfs(&self, id: usize, stat: &mut syscall::StatVfs) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn fsync(&self, id: usize) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn ftruncate(&self, id: usize, len: usize) -> syscall::Result<usize> { dbg!(Ok(0)) }
    fn futimens(&self, id: usize, times: &[syscall::TimeSpec]) -> syscall::Result<usize> { dbg!(Ok(0)) }
}
