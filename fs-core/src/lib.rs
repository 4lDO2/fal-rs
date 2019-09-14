use std::{io::prelude::*, mem};
use uuid::Uuid;

pub fn read_uuid(block: &[u8], offset: usize) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&block[offset..offset + 16]);
    uuid::builder::Builder::from_bytes(bytes).build()
}
pub fn read_u64(block: &[u8], offset: usize) -> u64 {
    let mut bytes = [0u8; mem::size_of::<u64>()];
    bytes.copy_from_slice(&block[offset..offset + mem::size_of::<u64>()]);
    u64::from_le_bytes(bytes)
}
pub fn read_u32(block: &[u8], offset: usize) -> u32 {
    let mut bytes = [0u8; mem::size_of::<u32>()];
    bytes.copy_from_slice(&block[offset..offset + mem::size_of::<u32>()]);
    u32::from_le_bytes(bytes)
}
pub fn read_u16(block: &[u8], offset: usize) -> u16 {
    let mut bytes = [0u8; mem::size_of::<u16>()];
    bytes.copy_from_slice(&block[offset..offset + mem::size_of::<u16>()]);
    u16::from_le_bytes(bytes)
}
pub fn read_u8(block: &[u8], offset: usize) -> u8 {
    let mut bytes = [0u8; mem::size_of::<u8>()];
    bytes.copy_from_slice(&block[offset..offset + mem::size_of::<u8>()]);
    u8::from_le_bytes(bytes)
}
pub fn write_uuid(block: &mut [u8], offset: usize, uuid: &Uuid) {
    let bytes = uuid.as_bytes();
    block[offset..offset + bytes.len()].copy_from_slice(bytes);
}
pub fn write_u32(block: &mut [u8], offset: usize, number: u32) {
    let bytes = number.to_le_bytes();
    block[offset..offset + bytes.len()].copy_from_slice(&bytes)
}
pub fn write_u64(block: &mut [u8], offset: usize, number: u64) {
    let bytes = number.to_le_bytes();
    block[offset..offset + bytes.len()].copy_from_slice(&bytes)
}
pub fn write_u16(block: &mut [u8], offset: usize, number: u16) {
    let bytes = number.to_le_bytes();
    block[offset..offset + bytes.len()].copy_from_slice(&bytes)
}
pub fn write_u8(block: &mut [u8], offset: usize, number: u8) {
    let bytes = number.to_le_bytes();
    block[offset..offset + bytes.len()].copy_from_slice(&bytes)
}

/// An readonly device, such as the file /dev/sda. Typically implemented by the frontend.
pub trait Device: Read + Seek {
    // TODO: Add support for querying bad sectors etc.
    // TODO: Remove the std::io traits, TODO: right?.
}
/// A read-write device.
pub trait DeviceMut: Device + Write {}

/// An abstract inode structure.
pub trait Inode {
}

// TODO: Add some kind of Result type.
/// An abstract filesystem. Typically implemented by the backend.
pub trait Filesystem<'a, D: Device> {
    /// An inode address. u32 on ext2.
    type InodeAddr: Into<u64>;

    /// An inode structure, capable of retrieving inode information.
    type InodeStruct: Inode;

    /// A file handle, capable of reading and seeking.
    type FileHandle: Read + Seek + 'a;

    // TODO: Support mounting multiple devices as one filesystem, for filesystems that support it.
    /// Mount the filesystem from a device.
    fn mount(_device: D) -> Self;

    /// Load an inode structure from the filesystem. For example, on ext2, the address 2 would load the root directory.
    fn load_inode(&mut self, address: Self::InodeAddr) -> Self::InodeStruct;

    /// Open a file from an inode address.
    fn open_file(&'a mut self, inode: Self::InodeAddr) -> Self::FileHandle;

    /// Close an opened file. The filesystem implementation will ensure that unread bytes get
    /// flushed before closing.
    fn close_file(&mut self, file: Self::FileHandle);
}
