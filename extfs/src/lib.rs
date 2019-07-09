use std::{
    convert::TryInto,
    env,
    ffi::{OsStr, OsString},
    fs,
    io::{self, prelude::*, SeekFrom},
    mem,
    ops::{Add, Div, Rem},
    path::{self, Path},
};

use uuid::Uuid;

pub mod block_group;
pub mod inode;
pub mod superblock;

pub use inode::Inode;
pub use superblock::Superblock;

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
    fn open_inode_raw<'a>(
        &mut self,
        parent: Inode,
        components: &[&OsStr],
    ) -> io::Result<inode::Inode> {
        if components.is_empty() {
            return Ok(parent);
        }
        let entries = parent.ls(self)?;

        if let Some(entry) = entries.iter().find(|entry| entry.name == components[0]) {
            let inode = Inode::load(self, entry.inode)?;
            self.open_inode_raw(inode, &components[1..])
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }
    pub fn open_inode<P: AsRef<Path>>(&mut self, path: P) -> io::Result<inode::Inode> {
        let root = Inode::load(self, inode::ROOT)?;

        let mut components = path.as_ref().components();

        let components = match components.next() {
            Some(path::Component::RootDir) => {
                let mut component_names = vec![];

                for component in components {
                    match component {
                        path::Component::Normal(string) => component_names.push(string),
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::NotFound,
                                "expected a normal path component",
                            ))
                        }
                    }
                }

                component_names
            }
            Some(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "only absolute paths are supported",
                ))
            }
            None => return Err(io::Error::new(io::ErrorKind::NotFound, "empty path")),
        };
        self.open_inode_raw(root, &components)
    }
}

fn main() {
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(env::args().nth(1).unwrap())
        .unwrap();

    let mut filesystem = Filesystem::mount(file).unwrap();

    eprintln!("{:?}", filesystem.superblock);
    eprintln!(
        "Block group count: {}.",
        filesystem.superblock.block_group_count()
    );

    let root_inode_block_group =
        block_group::inode_block_group_index(&filesystem.superblock, inode::ROOT);
    eprintln!(
        "Root block group: {:?}",
        block_group::load_block_group_descriptor(&mut filesystem, root_inode_block_group).unwrap()
    );
    let inode = inode::Inode::load(&mut filesystem, inode::ROOT).unwrap();
    eprintln!("Root inode info: {:?}", inode);

    fn recursion(inode: inode::Inode, filesystem: &mut Filesystem<impl Read + Seek + Write>) {
        if inode.ty == inode::InodeType::Dir {
            eprintln!("Recursion {{");
            for entry in inode.ls(filesystem).unwrap() {
                let name = entry.name.to_string_lossy();
                if name == "." || name == ".." {
                    continue;
                }
                let inode_struct = match inode::Inode::load(filesystem, entry.inode) {
                    Ok(inode_struct) => inode_struct,
                    Err(_) => continue,
                };
                eprintln!(
                    "{} => {} ({} bytes large)",
                    name,
                    entry.inode,
                    inode_struct.size(&filesystem.superblock)
                );

                recursion(inode_struct, filesystem);
            }
            eprintln!("}}");
        }
    }
    eprintln!("/");
    recursion(
        inode::Inode::load(&mut filesystem, inode::ROOT).unwrap(),
        &mut filesystem,
    );
}
