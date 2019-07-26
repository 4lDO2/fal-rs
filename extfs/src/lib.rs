use std::{
    convert::TryInto,
    ffi::OsString,
    io::{self, prelude::*, SeekFrom},
    mem,
    ops::{Add, Div, Mul, Rem},
};

use uuid::Uuid;

pub mod block_group;
pub mod inode;
pub mod superblock;

pub use inode::Inode;
pub use superblock::Superblock;

pub use fs_core::{read_u8, read_u16, read_u32, read_u64, read_uuid, write_u8, write_u16, write_u32, write_u64, write_uuid};

fn read_block_to<D: Read + Seek>(
    filesystem: &mut Filesystem<D>,
    block_address: u32,
    buffer: &mut [u8],
) -> io::Result<()> {
    debug_assert!(block_group::block_exists(block_address, filesystem)?);
    read_block_to_raw(filesystem, block_address, buffer)
}
fn read_block_to_raw<D: Read + Seek>(
    filesystem: &mut Filesystem<D>,
    block_address: u32,
    buffer: &mut [u8],
) -> io::Result<()> {
    filesystem.device.seek(SeekFrom::Start(
        block_address as u64 * filesystem.superblock.block_size,
    ))?;
    filesystem.device.read_exact(buffer)?;
    Ok(())
}
fn read_block<D: Read + Seek>(
    filesystem: &mut Filesystem<D>,
    block_address: u32,
) -> io::Result<Box<[u8]>> {
    let mut vector = vec![0; filesystem.superblock.block_size.try_into().unwrap()];
    read_block_to(filesystem, block_address, &mut vector)?;
    Ok(vector.into_boxed_slice())
}
fn write_block_raw<D: Read + Seek + Write>(filesystem: &mut Filesystem<D>, block_address: u32, buffer: &[u8]) -> io::Result<()> {
    filesystem.device.seek(SeekFrom::Start(block_address as u64 * filesystem.superblock.block_size))?;
    filesystem.device.write_all(buffer)?;
    Ok(())
}
fn write_block<D: Read + Seek + Write>(filesystem: &mut Filesystem<D>, block_address: u32, buffer: &[u8]) -> io::Result<()> {
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

pub struct Filesystem<D> {
    pub superblock: Superblock,
    pub device: D,
}
impl<D: Read + Seek + Write> Filesystem<D> {
    pub fn mount(mut device: D) -> io::Result<Self> {
        Ok(Self {
            superblock: Superblock::parse(&mut device)?,
            device,
        })
    }
}
