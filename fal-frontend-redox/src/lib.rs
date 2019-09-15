use std::fs::File;

use fal::Filesystem;
use syscall::Scheme;

pub struct RedoxFilesystem<Backend> {
    pub inner: Backend,
}

impl<Backend: fal::Filesystem<File>> RedoxFilesystem<Backend> {
    pub fn init(device: File) -> Self {
        Self {
            inner: Backend::mount(device),
        }
    }
}

impl<Backend: fal::Filesystem<File>> Scheme for RedoxFilesystem<Backend> {
    fn open(&self, path: &[u8], flags: usize, uid: u32, gid: u32) -> syscall::Result<usize> {
        unimplemented!()
    }
}
