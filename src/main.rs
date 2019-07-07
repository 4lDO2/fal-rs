use std::{env, fs, io::{self, prelude::*}, mem};

use uuid::Uuid;

fn read_uuid<R: Read>(mut device: R) -> io::Result<Uuid> {
    let mut bytes = [0; mem::size_of::<Uuid>()];
    device.read_exact(&mut bytes)?;
    Ok(uuid::builder::Builder::from_bytes(bytes).build())
}
fn read_u32<R: Read>(mut device: R) -> io::Result<u32> {
    let mut bytes = [0; mem::size_of::<u32>()];
    device.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}
fn read_u16<R: Read>(mut device: R) -> io::Result<u16> {
    let mut bytes = [0; mem::size_of::<u16>()];
    device.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}
fn read_u8<R: Read>(mut device: R) -> io::Result<u8> {
    let mut bytes = [0; mem::size_of::<u8>()];
    device.read_exact(&mut bytes)?;
    Ok(u8::from_le_bytes(bytes))
}

mod superblock;

fn main() {
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(env::args().nth(1).unwrap()).unwrap();

    let superblock = superblock::Superblock::parse(&mut file).unwrap();
    println!("{:?}", superblock);
}
