use std::{convert::TryInto, env, ffi::OsString, fs, io::{self, prelude::*, SeekFrom}, mem};

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
fn read_block<D: Read + Seek>(filesystem: &mut Filesystem<D>, block_address: u32) -> io::Result<Box<[u8]>> {
    filesystem.device.seek(SeekFrom::Start(block_address as u64 * filesystem.superblock.block_size))?;
    let mut vector = vec! [0u8; filesystem.superblock.block_size.try_into().unwrap()];
    filesystem.device.read_exact(&mut vector)?;
    Ok(vector.into_boxed_slice())
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

mod block_group;
mod inode;
mod superblock;

use superblock::Superblock;

pub struct Filesystem<D> {
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

    let mut filesystem = Filesystem::open(file).unwrap();

    println!("{:?}", filesystem.superblock);
    println!("Block group count: {}.", filesystem.superblock.block_group_count());

    let root_inode_block_group = block_group::inode_block_group_index(&filesystem.superblock, inode::ROOT);
    println!("Root block group: {:?}", block_group::load_block_group_descriptor(&mut filesystem, root_inode_block_group).unwrap());
    let inode = inode::Inode::load(&mut filesystem, inode::ROOT).unwrap();
    println!("Root inode info: {:?}", inode);
    println!("Ls root {:?}", inode.ls(&mut filesystem).unwrap());
}
