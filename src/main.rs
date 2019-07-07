use std::{convert::TryInto, env, fs, io::{self, prelude::*}, mem};

use uuid::Uuid;

fn read_uuid(block: &[u8], offset: usize) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&block[offset..offset + 16]);
    uuid::builder::Builder::from_bytes(bytes).build()
}
fn read_u32(block: &[u8], offset: usize) -> u32 {
    let mut bytes = [0u8; mem::size_of::<u32>()];
    bytes.copy_from_slice(&block[offset..offset + mem::size_of::<u32>()]);
    u32::from_le_bytes(bytes)
}
fn read_u16(block: &[u8], offset: usize) -> u16 {
    let mut bytes = [0u8; mem::size_of::<u16>()];
    bytes.copy_from_slice(&block[offset..offset + mem::size_of::<u16>()]);
    u16::from_le_bytes(bytes)
}
fn read_u8(block: &[u8], offset: usize) -> u8 {
    let mut bytes = [0u8; mem::size_of::<u8>()];
    bytes.copy_from_slice(&block[offset..offset + mem::size_of::<u8>()]);
    u8::from_le_bytes(bytes)
}
fn read_block<D: Read + Seek>(mut filesystem: Filesystem<D>) -> io::Result<Box<[u8]>> {
    let mut vector = vec! [0u8; filesystem.superblock.block_size.try_into().unwrap()];
    filesystem.device.read_exact(&mut vector)?;
    Ok(vector.into_boxed_slice())
}

mod superblock;
mod block_group;

use superblock::Superblock;

struct Filesystem<D> {
    superblock: Superblock,
    device: D,
}
impl<D: Read + Seek + Write> Filesystem<D> {
    pub fn open(mut device: D) -> io::Result<Self> {
        Ok(Self {
            superblock: Superblock::parse(&mut device)?,
            device,
        })
    }
}

fn main() {
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(env::args().nth(1).unwrap()).unwrap();

    let filesystem = Filesystem::open(file).unwrap();

    println!("{:?}", filesystem.superblock);
    println!("Block group count: {}.", filesystem.superblock.block_group_count());
}
