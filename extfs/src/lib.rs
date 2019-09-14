use std::{
    collections::HashMap,
    convert::TryInto,
    ffi::{OsStr, OsString},
    io::{self, prelude::*, SeekFrom},
    ops::{Add, Div, Mul, Rem},
    sync::Mutex,
};

use uuid::Uuid;

use fs_core::Filesystem as _;

pub mod block_group;
pub mod inode;
pub mod superblock;

pub use inode::Inode;
pub use superblock::Superblock;

pub use fs_core::{read_u8, read_u16, read_u32, read_u64, read_uuid, write_u8, write_u16, write_u32, write_u64, write_uuid};

fn read_block_to<D: fs_core::Device>(
    filesystem: &mut Filesystem<D>,
    block_address: u32,
    buffer: &mut [u8],
) -> io::Result<()> {
    debug_assert!(block_group::block_exists(block_address, filesystem)?);
    read_block_to_raw(filesystem, block_address, buffer)
}
fn read_block_to_raw<D: fs_core::Device>(
    filesystem: &mut Filesystem<D>,
    block_address: u32,
    buffer: &mut [u8],
) -> io::Result<()> {
    filesystem.device.lock().unwrap().seek(SeekFrom::Start(
        block_address as u64 * filesystem.superblock.block_size,
    ))?;
    filesystem.device.lock().unwrap().read_exact(buffer)?;
    Ok(())
}
fn read_block<D: fs_core::Device>(
    filesystem: &mut Filesystem<D>,
    block_address: u32,
) -> io::Result<Box<[u8]>> {
    let mut vector = vec![0; filesystem.superblock.block_size.try_into().unwrap()];
    read_block_to(filesystem, block_address, &mut vector)?;
    Ok(vector.into_boxed_slice())
}
fn write_block_raw<D: fs_core::DeviceMut>(filesystem: &mut Filesystem<D>, block_address: u32, buffer: &[u8]) -> io::Result<()> {
    filesystem.device.lock().unwrap().seek(SeekFrom::Start(block_address as u64 * filesystem.superblock.block_size))?;
    filesystem.device.lock().unwrap().write_all(buffer)?;
    Ok(())
}
fn write_block<D: fs_core::DeviceMut>(filesystem: &mut Filesystem<D>, block_address: u32, buffer: &[u8]) -> io::Result<()> {
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
    T: Add<Output = T> + Copy + Div<Output = T> + Mul<Output = T> + Rem<Output = T> + From<u8> + PartialEq,
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

pub struct Filesystem<D: fs_core::Device> {
    pub superblock: Superblock,
    pub device: Mutex<D>,
    pub fhs: HashMap<u64, RawFileHandle>,
    last_fh: u64,
}

impl fs_core::Inode for Inode {}

#[derive(Clone, Copy, Debug, PartialEq, Hash)]
pub struct RawFileHandle {
    size: u64,
    offset: u64,
    fh: u64,
}
impl RawFileHandle {
    fn read<D: fs_core::Device>(&self, filesystem: &Filesystem<D>, buf: &mut [u8]) -> io::Result<usize> {
        unimplemented!()
    }
}
impl Seek for RawFileHandle {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Current(offset) => self.offset = (self.offset as i64 + offset) as u64,
            SeekFrom::Start(offset) => self.offset = offset as u64,
            SeekFrom::End(offset) => {
                if offset != 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into())
                }
                self.offset = self.size;
            }
        }
        Ok(self.offset)
    }
}

pub struct FileHandle<'a, D: fs_core::Device> {
    raw: RawFileHandle,
    filesystem: &'a Filesystem<D>,
}

impl<'a, D: fs_core::Device> Read for FileHandle<'a, D> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.raw.read(&mut self.filesystem, buffer)
    }
}
impl<'a, D: fs_core::Device> Seek for FileHandle<'a, D> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.raw.seek(position)
    }
}

impl<'a, D: fs_core::Device + 'a> fs_core::Filesystem<'a, D> for Filesystem<D> {
    type InodeAddr = u32;
    type InodeStruct = Inode;
    type FileHandle = FileHandle<'a, D>;

    fn mount(mut device: D) -> Self {
        Self {
            superblock: Superblock::parse(&mut device).unwrap(),
            device: Mutex::new(device),
            fhs: HashMap::new(),
            last_fh: 0,
        }
    }
    fn load_inode(&mut self, addr: Self::InodeAddr) -> Self::InodeStruct {
        Inode::load(self, addr).unwrap()
    }
    fn open_file(&'a mut self, addr: Self::InodeAddr) -> FileHandle<'a, D> {
        let fh = RawFileHandle {
            offset: 0,
            size: Inode::load(self, addr).unwrap().size(&self.superblock),
            fh: self.last_fh,
        };
        self.fhs.insert(self.last_fh, fh);
        self.last_fh += 1;

        FileHandle {
            raw: fh,
            filesystem: self,
        }
    }
    fn close_file(&mut self, fh: Self::FileHandle) {
        // FIXME: Flush before closing.
        self.fhs.remove(&fh.raw.fh);
    }
}
impl<'a, D: fs_core::Device> Drop for Filesystem<D> {
    fn drop(&mut self) {
        // TODO: Optimize.
        for (_, fh) in self.fhs.drain() {
            self.close_file(FileHandle { raw: fh, filesystem: self });
        }
    }
}
