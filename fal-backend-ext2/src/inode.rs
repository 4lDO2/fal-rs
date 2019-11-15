use std::{
    convert::{TryFrom, TryInto},
    ffi::{OsStr, OsString},
    io, mem,
};

use crate::{
    block_group, os_str_to_bytes, os_string_from_bytes, read_block, read_block_to, read_u16,
    read_u32, read_u8,
    superblock::{OptionalFeatureFlags, OsId, RequiredFeatureFlags, RoFeatureFlags, Superblock},
    write_block, write_u16, write_u32, write_u8, Filesystem,
};

pub const ROOT: u32 = 2;

pub const BASE_INODE_SIZE: u64 = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Inode {
    pub addr: u32,
    pub ty: InodeType,
    pub permissions: u16,
    pub uid: u32,
    pub last_access_time: u32,
    pub creation_time: u32,
    pub last_modification_time: u32,
    pub deletion_time: u32,
    pub gid: u32,
    pub hard_link_count: u16,
    pub disk_sector_count: u32,
    pub flags: u32,
    pub os_specific_1: u32,
    pub direct_ptrs: [u32; Self::DIRECT_PTR_COUNT],
    pub singly_indirect_ptr: u32,
    pub doubly_indirect_ptr: u32,
    pub triply_indirect_ptr: u32,
    pub generation_number: u32,
    pub size: u64,

    pub extended_atribute_block: u32,
    pub size_high_or_acl: u32,
    pub fragment_baddr: u32,
    pub os_specific_2: [u8; 12],

    block_size: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InodeType {
    Fifo,
    CharDev,
    Dir,
    BlockDev,
    File,
    Symlink,
    UnixSock,
}

impl InodeType {
    const TYPE_MASK: u16 = 0xF000;
    const PERM_MASK: u16 = 0x0FFF;
    pub fn from_type_and_perm(raw: u16) -> (Option<Self>, u16) {
        (
            match raw & Self::TYPE_MASK {
                0x1000 => Some(InodeType::Fifo),
                0x2000 => Some(InodeType::CharDev),
                0x4000 => Some(InodeType::Dir),
                0x6000 => Some(InodeType::BlockDev),
                0x8000 => Some(InodeType::File),
                0xA000 => Some(InodeType::Symlink),
                0xC000 => Some(InodeType::UnixSock),
                _ => None,
            },
            raw & Self::PERM_MASK,
        )
    }
    pub fn to_type_and_perm((this, perm): (Self, u16)) -> u16 {
        assert_eq!(perm & Self::PERM_MASK, perm);

        (match this {
            InodeType::Fifo => 0x1000,
            InodeType::CharDev => 0x2000,
            InodeType::Dir => 0x4000,
            InodeType::BlockDev => 0x6000,
            InodeType::File => 0x8000,
            InodeType::Symlink => 0xA000,
            InodeType::UnixSock => 0xC000,
        }) | perm
    }
    pub fn from_direntry_ty_indicator(indicator: u8) -> Option<Self> {
        match indicator {
            1 => Some(InodeType::File),
            2 => Some(InodeType::Dir),
            3 => Some(InodeType::CharDev),
            4 => Some(InodeType::BlockDev),
            5 => Some(InodeType::Fifo),
            6 => Some(InodeType::UnixSock),
            7 => Some(InodeType::Symlink),
            _ => None,
        }
    }
    pub fn to_direntry_ty_indicator(this: Self) -> u8 {
        match this {
            InodeType::File => 1,
            InodeType::Dir => 2,
            InodeType::CharDev => 3,
            InodeType::BlockDev => 4,
            InodeType::Fifo => 5,
            InodeType::UnixSock => 6,
            InodeType::Symlink => 7,
        }
    }
}

impl From<InodeType> for fal::FileType {
    fn from(inode_type: InodeType) -> Self {
        match inode_type {
            InodeType::File => Self::RegularFile,
            InodeType::Dir => Self::Directory,
            InodeType::Symlink => Self::Symlink,
            InodeType::BlockDev => Self::BlockDevice,
            InodeType::UnixSock => Self::Socket,
            InodeType::CharDev => Self::CharacterDevice,
            InodeType::Fifo => Self::NamedPipe,
        }
    }
}

impl Inode {
    const DIRECT_PTR_COUNT: usize = 12;

    pub fn load<R: fal::Device>(
        filesystem: &Filesystem<R>,
        inode_address: u32,
    ) -> fal::Result<Self> {
        if inode_address == 0 {
            return Err(fal::Error::Invalid);
        }

        if !block_group::inode_exists(inode_address, filesystem)? {
            return Err(fal::Error::NoEntity);
        }

        let block_group_index =
            block_group::inode_block_group_index(&filesystem.superblock, inode_address);
        let block_group_descriptor =
            block_group::load_block_group_descriptor(filesystem, block_group_index.into())?;
        let inode_index_in_group =
            block_group::inode_index_inside_group(&filesystem.superblock, inode_address);
        let inode_size = filesystem.superblock.inode_size();

        let containing_block_index = block_group_descriptor.inode_table_start_baddr()
            + u64::from(inode_index_in_group * u32::from(inode_size) / filesystem.superblock.block_size);

        let max_inodes_in_block = filesystem.superblock.block_size / u32::from(inode_size);

        let inode_index_in_block =
            usize::try_from(inode_index_in_group % max_inodes_in_block).unwrap();
        let inode_size = usize::from(inode_size);

        let containing_block = read_block(filesystem, containing_block_index)?;
        let inode_bytes = &containing_block
            [inode_index_in_block * inode_size..inode_index_in_block * inode_size + inode_size];
        Ok(Self::parse(
            &filesystem.superblock,
            inode_address,
            inode_bytes,
        ))
    }
    pub fn store<D: fal::DeviceMut>(
        this: &Self,
        filesystem: &mut Filesystem<D>,
    ) -> fal::Result<()> {
        let inode_address = this.addr;

        if inode_address == 0 {
            return Err(fal::Error::Invalid);
        }

        if !block_group::inode_exists(inode_address, filesystem)? {
            return Err(fal::Error::NoEntity);
        }

        let block_group_index =
            block_group::inode_block_group_index(&filesystem.superblock, inode_address);
        let block_group_descriptor =
            block_group::load_block_group_descriptor(filesystem, block_group_index.into())?;
        let inode_index_in_group =
            block_group::inode_index_inside_group(&filesystem.superblock, inode_address);
        let inode_size = filesystem.superblock.inode_size();

        let containing_block_index = block_group_descriptor.inode_table_start_baddr()
            + u64::from(inode_index_in_group * u32::from(inode_size) / filesystem.superblock.block_size);

        let max_inodes_in_block = filesystem.superblock.block_size / u32::from(inode_size);

        let inode_index_in_block =
            usize::try_from(inode_index_in_group % max_inodes_in_block).unwrap();
        let inode_size = usize::from(inode_size);

        let mut containing_block = read_block(filesystem, containing_block_index)?;
        let inode_bytes = &mut containing_block
            [inode_index_in_block * inode_size..inode_index_in_block * inode_size + inode_size];

        Self::serialize(this, &filesystem.superblock, inode_bytes);

        Ok(write_block(
            filesystem,
            containing_block_index,
            &containing_block,
        )?)
    }
    pub fn parse(superblock: &Superblock, addr: u32, bytes: &[u8]) -> Self {
        let (ty, permissions) = InodeType::from_type_and_perm(read_u16(bytes, 0));
        let ty = ty.unwrap();

        let os_specific_2 = {
            let mut os_specific_bytes = [0u8; 12];
            os_specific_bytes.copy_from_slice(&bytes[116..128]);
            os_specific_bytes
        };
        let gid_low = read_u16(bytes, 24);
        let uid_low = read_u16(bytes, 2);

        let (gid_high, uid_high) = match superblock.os_id {
            OsId::Linux | OsId::Hurd => (read_u16(&os_specific_2, 4), read_u16(&os_specific_2, 6)),
            _ => (0, 0),
        };

        let gid = gid_low as u32 | ((gid_high as u32) << 16);
        let uid = uid_low as u32 | ((uid_high as u32) << 16);

        let size_low = read_u32(bytes, 4);
        let size_high_or_acl = read_u32(bytes, 108);

        let size = u64::from(size_low)
            | if let Some(extended) = superblock.extended.as_ref() {
                if extended
                    .req_features_for_rw
                    .contains(RoFeatureFlags::EXTENDED_FILE_SIZE)
                    && ty == InodeType::File
                {
                    u64::from(size_high_or_acl) << 32
                } else {
                    0
                }
            } else {
                0
            };

        Self {
            block_size: superblock.block_size,
            addr,
            ty,
            permissions,
            uid,
            size,
            last_access_time: read_u32(bytes, 8),
            creation_time: read_u32(bytes, 12),
            last_modification_time: read_u32(bytes, 16),
            deletion_time: read_u32(bytes, 20),
            gid,
            hard_link_count: read_u16(bytes, 26),
            disk_sector_count: read_u32(bytes, 28),
            flags: read_u32(bytes, 32),
            os_specific_1: read_u32(bytes, 36),
            direct_ptrs: {
                let mut direct_ptrs = [0u32; 12];
                for index in 0..12 {
                    let mut le_bytes = [0u8; mem::size_of::<u32>()];
                    le_bytes.copy_from_slice(&bytes[40 + index * 4..40 + index * 4 + 4]);
                    let direct_ptr = u32::from_le_bytes(le_bytes);
                    direct_ptrs[index] = direct_ptr;
                }
                direct_ptrs
            },
            singly_indirect_ptr: read_u32(bytes, 88),
            doubly_indirect_ptr: read_u32(bytes, 92),
            triply_indirect_ptr: read_u32(bytes, 96),
            generation_number: read_u32(bytes, 100),
            extended_atribute_block: read_u32(bytes, 104),
            size_high_or_acl: read_u32(bytes, 108),
            fragment_baddr: read_u32(bytes, 112),
            os_specific_2,
        }
    }
    pub fn serialize(this: &Inode, superblock: &Superblock, buffer: &mut [u8]) {
        write_u16(
            buffer,
            0,
            InodeType::to_type_and_perm((this.ty, this.permissions)),
        );
        write_u16(buffer, 2, this.uid as u16);
        write_u32(buffer, 4, this.size as u32);
        write_u32(buffer, 8, this.last_access_time);
        write_u32(buffer, 12, this.creation_time);
        write_u32(buffer, 16, this.last_modification_time);
        write_u32(buffer, 20, this.deletion_time);
        write_u16(buffer, 24, this.gid as u16);
        write_u16(buffer, 26, this.hard_link_count);
        write_u32(buffer, 28, this.disk_sector_count);
        write_u32(buffer, 32, this.flags);
        write_u32(buffer, 36, this.os_specific_1);

        let stride = mem::size_of::<u32>();
        for (index, direct_ptr) in this.direct_ptrs.iter().enumerate() {
            buffer[40 + index * stride..40 + (index + 1) * stride]
                .copy_from_slice(&direct_ptr.to_le_bytes());
        }

        write_u32(buffer, 88, this.singly_indirect_ptr);
        write_u32(buffer, 92, this.doubly_indirect_ptr);
        write_u32(buffer, 96, this.triply_indirect_ptr);
        write_u32(buffer, 100, this.generation_number);
        write_u32(buffer, 104, this.extended_atribute_block);
        write_u32(buffer, 108, this.size_high_or_acl);
        write_u32(buffer, 112, this.fragment_baddr);

        assert!(u32::from(superblock.inode_size()) <= superblock.block_size);

        // TODO: Serialize the os specific high uid and gid.
        buffer[116..128].copy_from_slice(&this.os_specific_2);

        for byte in &mut buffer[BASE_INODE_SIZE.try_into().unwrap()..] {
            *byte = 0;
        }
    }
    pub fn size_in_blocks(&self) -> u64 {
        fal::div_round_up(self.size, self.block_size.into())
    }
    fn entry_count(block_size: u32) -> usize {
        block_size as usize / mem::size_of::<u32>()
    }
    fn read_singly<D: fal::Device>(
        filesystem: &Filesystem<D>,
        singly_baddr: u32,
        rel_baddr: u32,
    ) -> io::Result<u32> {
        let singly_indirect_block_bytes = read_block(filesystem, singly_baddr.try_into().unwrap())?;
        let index = rel_baddr as usize - Self::DIRECT_PTR_COUNT;

        Ok(read_u32(
            &singly_indirect_block_bytes,
            index * mem::size_of::<u32>(),
        ))
    }
    fn read_doubly<D: fal::Device>(
        filesystem: &Filesystem<D>,
        doubly_baddr: u32,
        rel_baddr: u32,
    ) -> io::Result<u32> {
        let entry_count = Self::entry_count(filesystem.superblock.block_size);

        let doubly_indirect_block_bytes = read_block(filesystem, doubly_baddr.try_into().unwrap())?;
        let index = (rel_baddr as usize - Self::DIRECT_PTR_COUNT - entry_count) / entry_count;

        let singly = read_u32(&doubly_indirect_block_bytes, index * mem::size_of::<u32>());
        let singly_rel_baddr = rel_baddr - (index as u32 + 1) * entry_count as u32;

        Self::read_singly(filesystem, singly, singly_rel_baddr)
    }
    fn read_triply<D: fal::Device>(
        filesystem: &Filesystem<D>,
        triply_baddr: u32,
        rel_baddr: u32,
    ) -> io::Result<u32> {
        let entry_count = Self::entry_count(filesystem.superblock.block_size);

        let triply_indirect_block_bytes = read_block(filesystem, triply_baddr.try_into().unwrap())?;
        let index =
            (rel_baddr as usize - Self::DIRECT_PTR_COUNT - entry_count - entry_count * entry_count)
                / (entry_count * entry_count);

        let doubly = read_u32(&triply_indirect_block_bytes, index * mem::size_of::<u32>());
        let doubly_rel_baddr =
            rel_baddr - (index as u32 + 1) * entry_count as u32 * entry_count as u32;

        Self::read_doubly(filesystem, doubly, doubly_rel_baddr)
    }
    fn absolute_baddr<D: fal::Device>(
        &self,
        filesystem: &Filesystem<D>,
        rel_baddr: u32,
    ) -> io::Result<u32> {
        let entry_count = Self::entry_count(filesystem.superblock.block_size) as u32;

        let direct_size = Self::DIRECT_PTR_COUNT as u32;
        let singly_indir_size = direct_size + entry_count;
        let doubly_indir_size = singly_indir_size + entry_count * entry_count;
        let triply_indir_size = doubly_indir_size + entry_count * entry_count * entry_count;

        Ok(if rel_baddr < direct_size {
            self.direct_ptrs[usize::try_from(rel_baddr).unwrap()]
        } else if rel_baddr < singly_indir_size {
            Self::read_singly(filesystem, self.singly_indirect_ptr, rel_baddr)?
        } else if rel_baddr < doubly_indir_size {
            Self::read_doubly(filesystem, self.doubly_indirect_ptr, rel_baddr)?
        } else if rel_baddr < triply_indir_size {
            Self::read_triply(filesystem, self.triply_indirect_ptr, rel_baddr)?
        } else {
            panic!("Read exceeding maximum ext2 file size.");
        })
    }
    pub fn read_block_to<D: fal::Device>(
        &self,
        rel_baddr: u32,
        filesystem: &Filesystem<D>,
        buffer: &mut [u8],
    ) -> fal::Result<()> {
        if u64::from(rel_baddr) >= self.size_in_blocks() {
            return Err(fal::Error::Overflow);
        }
        let abs_baddr = self.absolute_baddr(filesystem, rel_baddr)?;
        Ok(read_block_to(filesystem, abs_baddr.try_into().unwrap(), buffer)?)
    }
    pub fn write_content_block<D: fal::DeviceMut>(
        &self,
        rel_baddr: u32,
        filesystem: &Filesystem<D>,
        buffer: &[u8],
    ) -> fal::Result<()> {
        if u64::from(rel_baddr) >= self.size_in_blocks() {
            return Err(fal::Error::Overflow);
        }
        let abs_baddr = self.absolute_baddr(filesystem, rel_baddr)?;
        Ok(write_block(filesystem, abs_baddr.try_into().unwrap(), buffer)?)
    }
    pub fn read<D: fal::Device>(
        &self,
        filesystem: &Filesystem<D>,
        offset: u64,
        mut buffer: &mut [u8],
    ) -> fal::Result<usize> {
        let mut bytes_read = 0;

        let off_from_rel_block = offset % u64::from(filesystem.superblock.block_size);
        let rel_baddr_start = offset / u64::from(filesystem.superblock.block_size);

        let mut block_bytes =
            (vec![0u8; usize::try_from(filesystem.superblock.block_size).unwrap()])
                .into_boxed_slice();

        if off_from_rel_block != 0 {
            self.read_block_to(
                rel_baddr_start.try_into().unwrap(),
                filesystem,
                &mut block_bytes,
            )?;

            let off_from_rel_block_usize = usize::try_from(off_from_rel_block).unwrap();
            let end = std::cmp::min(
                buffer.len(),
                usize::try_from(filesystem.superblock.block_size).unwrap()
                    - off_from_rel_block_usize,
            );
            buffer[..end].copy_from_slice(
                &block_bytes[off_from_rel_block_usize..off_from_rel_block_usize + end],
            );

            bytes_read += end;

            if u64::try_from(buffer.len()).unwrap() >= off_from_rel_block {
                return self
                    .read(
                        filesystem,
                        fal::round_up(offset, u64::from(filesystem.superblock.block_size)),
                        &mut buffer[end..],
                    )
                    .map(|b| b + bytes_read);
            } else {
                return Ok(bytes_read);
            }
        }

        let mut current_rel_baddr = u32::try_from(rel_baddr_start).unwrap();

        while buffer.len() >= usize::try_from(filesystem.superblock.block_size).unwrap() {
            self.read_block_to(current_rel_baddr, filesystem, &mut block_bytes)?;

            buffer[..usize::try_from(filesystem.superblock.block_size).unwrap()]
                .copy_from_slice(&block_bytes);

            bytes_read += block_bytes.len();

            buffer = &mut buffer[usize::try_from(filesystem.superblock.block_size).unwrap()..];
            current_rel_baddr += 1;
        }

        if buffer.len() != 0 {
            self.read_block_to(current_rel_baddr, filesystem, &mut block_bytes)?;
            let buffer_len = buffer.len();
            buffer.copy_from_slice(&block_bytes[..buffer_len]);
            bytes_read += buffer_len;
        }

        Ok(bytes_read)
    }
    pub fn write<D: fal::DeviceMut>(
        &self,
        filesystem: &mut Filesystem<D>,
        offset: u64,
        mut buffer: &[u8],
    ) -> fal::Result<()> {
        let off_from_rel_block = offset % u64::from(filesystem.superblock.block_size);
        let rel_baddr_start = offset / u64::from(filesystem.superblock.block_size);

        let mut block_bytes = vec![0u8; usize::try_from(filesystem.superblock.block_size).unwrap()]
            .into_boxed_slice();

        if off_from_rel_block != 0 {
            self.read_block_to(
                rel_baddr_start.try_into().unwrap(),
                filesystem,
                &mut block_bytes,
            )?;

            let off_from_rel_block_usize = usize::try_from(off_from_rel_block).unwrap();
            let end = std::cmp::min(
                buffer.len(),
                usize::try_from(filesystem.superblock.block_size).unwrap()
                    - off_from_rel_block_usize,
            );
            block_bytes[off_from_rel_block_usize..off_from_rel_block_usize + end]
                .copy_from_slice(&buffer[..end]);

            self.write_content_block(
                rel_baddr_start.try_into().unwrap(),
                filesystem,
                &block_bytes,
            )?;

            if u64::try_from(buffer.len()).unwrap() >= off_from_rel_block {
                return self.write(
                    filesystem,
                    fal::round_up(offset, u64::from(filesystem.superblock.block_size)),
                    &buffer[end..],
                );
            } else {
                return Ok(());
            }
        }

        let mut current_rel_baddr = u32::try_from(rel_baddr_start).unwrap();

        while buffer.len() >= usize::try_from(filesystem.superblock.block_size).unwrap() {
            self.read_block_to(current_rel_baddr, filesystem, &mut block_bytes)?;

            block_bytes.copy_from_slice(
                &buffer[..usize::try_from(filesystem.superblock.block_size).unwrap()],
            );

            self.write_content_block(current_rel_baddr, filesystem, &block_bytes)?;

            buffer = &buffer[usize::try_from(filesystem.superblock.block_size).unwrap()..];
            current_rel_baddr += 1;
        }

        if buffer.len() != 0 {
            self.read_block_to(current_rel_baddr, filesystem, &mut block_bytes)?;
            block_bytes[..buffer.len()].copy_from_slice(&buffer);
            self.write_content_block(current_rel_baddr, filesystem, &block_bytes)?;
        }

        Ok(())
    }
    fn raw_dir_entries<'a, D: fal::Device>(
        &'a self,
        filesystem: &'a Filesystem<D>,
    ) -> io::Result<RawDirIterator<'a, D>> {
        if self.ty != InodeType::Dir {
            // TODO: ENOTDIR
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        Ok(RawDirIterator {
            filesystem,
            inode_struct: self,
            current_entry_offset: 0,
            entry_bytes: vec![0; 6],
            finished: false,
        })
    }
    pub fn dir_entries<'a, D: fal::Device>(
        &'a self,
        filesystem: &'a Filesystem<D>,
    ) -> io::Result<DirIterator<'a, D>> {
        Ok(DirIterator {
            raw: self.raw_dir_entries(filesystem)?,
        })
    }
    pub fn with_symlink_target<D: fal::Device, F: FnOnce(fal::Result<&[u8]>) -> ()>(
        &self,
        filesystem: &mut Filesystem<D>,
        handler: F,
    ) {
        if self.size <= 60 {
            // fast symlink

            let mut bytes = [0u8; 60];
            let stride = mem::size_of::<u32>();
            for (index, value) in self
                .direct_ptrs
                .iter()
                .chain(&[
                    self.singly_indirect_ptr,
                    self.doubly_indirect_ptr,
                    self.triply_indirect_ptr,
                ])
                .enumerate()
            {
                bytes[index * stride..(index + 1) * stride].copy_from_slice(&value.to_le_bytes());
            }
            handler(Ok(&bytes));
        } else {
            // slow symlink

            // TODO: Waiting for try_reserve (https://github.com/rust-lang/rust/issues/48043).
            let mut bytes = vec![0u8; self.size.try_into().unwrap()];
            match self.read(filesystem, 0, &mut bytes) {
                Ok(_) => handler(Ok(&bytes)),
                Err(error) => handler(Err(error)),
            }
        }
    }

    pub fn remove<D: fal::DeviceMut>(
        &self,
        filesystem: &mut Filesystem<D>,
        inode: u32,
    ) -> io::Result<()> {
        assert_eq!(self.hard_link_count, 0);

        // Frees the inode and its owned blocks.
        block_group::free_inode(inode, filesystem)
    }
    pub fn remove_entry<D: fal::DeviceMut>(
        &self,
        filesystem: &mut Filesystem<D>,
        name: &OsStr,
    ) -> fal::Result<()> {
        // Remove the entry by setting the length of the entry with matching name to zero, and
        // append the length of that entry to the previous (if any).

        let (index, (mut entry, offset)) = match self
            .raw_dir_entries(filesystem)?
            .enumerate()
            .find(|(_, (entry, _))| entry.name == name)
        {
            Some(x) => x,
            None => return Err(fal::Error::NoEntity),
        };

        if index > 0 {
            // TODO: Avoid iterating twice.

            let (mut previous_entry, previous_offset) =
                self.raw_dir_entries(filesystem)?.nth(index - 1).unwrap();
            previous_entry.total_entry_size += entry.total_entry_size;

            let mut bytes = [0u8; 8];
            DirEntry::serialize_raw(&previous_entry, &filesystem.superblock, &mut bytes);
            self.write(filesystem, previous_offset, &bytes)?;
        }

        let mut entry_inode_struct = Inode::load(filesystem, entry.inode)?;

        entry.total_entry_size = 0;
        entry.inode = 0;
        entry.type_indicator = Some(InodeType::File);
        entry.name = OsString::new();

        let mut bytes = [0u8; 8];
        DirEntry::serialize(&entry, &filesystem.superblock, &mut bytes);
        self.write(filesystem, offset, &bytes)?;

        entry_inode_struct.hard_link_count -= 1;

        if entry_inode_struct.hard_link_count == 0 {
            entry_inode_struct.remove(filesystem, entry.inode)?;
        } else {
            Inode::store(&entry_inode_struct, filesystem)?;
        }

        Ok(())
    }
}
pub struct RawDirIterator<'a, D: fal::Device> {
    filesystem: &'a Filesystem<D>,
    inode_struct: &'a Inode,
    current_entry_offset: u64,
    entry_bytes: Vec<u8>,
    finished: bool,
}
pub struct DirIterator<'a, D: fal::Device> {
    raw: RawDirIterator<'a, D>,
}
impl<'a, D: fal::Device> Iterator for RawDirIterator<'a, D> {
    type Item = (DirEntry, u64);
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let size = self.inode_struct.size;

        if size == 0 {
            self.finished = true;
            return None;
        }

        if self.current_entry_offset + 6 < size {
            self.entry_bytes.clear();
            self.entry_bytes.resize(6, 0);

            if self
                .inode_struct
                .read(
                    self.filesystem,
                    self.current_entry_offset,
                    &mut self.entry_bytes[..6],
                )
                .is_err()
            {
                self.finished = true;
                return None;
            }
            let length = DirEntry::length(&self.entry_bytes[0..6])
                .try_into()
                .unwrap();

            if length == 0 {
                self.finished = true;
                return None;
            }

            self.entry_bytes.resize(length, 0);
            if self
                .inode_struct
                .read(
                    self.filesystem,
                    self.current_entry_offset,
                    &mut self.entry_bytes[..length],
                )
                .is_err()
            {
                self.finished = true;
                return None;
            }

            let entry = DirEntry::parse(&self.filesystem.superblock, &self.entry_bytes);

            let value = Some((entry, self.current_entry_offset));
            self.current_entry_offset += u64::try_from(length).unwrap();
            return value;
        } else {
            self.finished = true;
            return None;
        }
    }
}
impl<'a, D: fal::Device> Iterator for DirIterator<'a, D> {
    type Item = DirEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw.next().map(|(entry, _)| entry)
    }
}
#[derive(Debug)]
pub struct DirEntry {
    pub inode: u32,
    pub type_indicator: Option<InodeType>,
    pub name: OsString,
    pub total_entry_size: u16,
}

impl DirEntry {
    pub fn length(bytes: &[u8]) -> u16 {
        assert_eq!(bytes.len(), mem::size_of::<u32>() + mem::size_of::<u16>());
        if read_u32(bytes, 0) != 0 {
            read_u16(bytes, 4)
        } else {
            0
        }
    }
    pub fn parse(superblock: &Superblock, bytes: &[u8]) -> Self {
        let total_entry_size = read_u16(bytes, 4);

        let name_length = total_entry_size - 8;
        let name_bytes = &bytes[8..8 + usize::try_from(name_length).unwrap()];
        let name_bytes = &name_bytes[..name_bytes
            .iter()
            .copied()
            .position(|byte| byte == 0)
            .unwrap_or(name_bytes.len())];
        let name = os_string_from_bytes(name_bytes);

        Self {
            inode: read_u32(bytes, 0),
            type_indicator: if let Some(extended) = superblock.extended.as_ref() {
                if extended
                    .opt_features_present
                    .contains(OptionalFeatureFlags::INODE_EXTENDED_ATTRS)
                {
                    InodeType::from_direntry_ty_indicator(read_u8(bytes, 7))
                } else {
                    None
                }
            } else {
                None
            },
            name,
            total_entry_size,
        }
    }
    pub fn serialize_raw(this: &Self, superblock: &Superblock, bytes: &mut [u8]) {
        write_u32(bytes, 0, this.inode);
        write_u16(bytes, 4, this.total_entry_size);
        write_u8(bytes, 6, this.name.len() as u8);

        if superblock
            .extended
            .as_ref()
            .map(|extended| {
                extended
                    .req_features_present
                    .contains(RequiredFeatureFlags::DIR_TYPE)
            })
            .unwrap_or(false)
        {
            write_u8(
                bytes,
                7,
                InodeType::to_direntry_ty_indicator(this.type_indicator.unwrap()),
            );
        } else {
            write_u8(bytes, 7, (this.name.len() >> 8) as u8);
        }
    }
    pub fn serialize(this: &Self, superblock: &Superblock, bytes: &mut [u8]) {
        Self::serialize_raw(this, superblock, bytes);
        bytes[8..8 + this.name.len()].copy_from_slice(&os_str_to_bytes(&this.name));
    }
    pub fn ty<D: fal::Device>(&self, filesystem: &Filesystem<D>) -> fal::Result<InodeType> {
        Ok(match self.type_indicator {
            Some(ty) => ty,
            None => Inode::load(filesystem, self.inode)?.ty,
        })
    }
}
