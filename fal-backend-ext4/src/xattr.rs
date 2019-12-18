use std::collections::HashMap;

use arrayvec::ArrayVec;
use quick_error::quick_error;
use scroll::{Pread, Pwrite};

use crate::{Filesystem, inode::{BASE_INODE_SIZE, Inode, InodeIoError, InodeRaw}, superblock::RequiredFeatureFlags};

pub const MAGIC: u32 = 0xEA02_0000;
pub const INLINE_HEADER_SIZE: usize = 4;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Pread, Pwrite)]
pub struct XattrInlineHeader {
    pub magic: u32,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Pread, Pwrite)]
pub struct XattrBlockHeader {
    pub magic: u32,
    pub refcount: u32,
    pub blocks_used: u32,
    pub hash: u32,
    pub checksum: u32,
    pub zero: [u32; 2],
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct XattrEntry {
    pub name_length: u8,
    pub name_index: u8,
    pub value_offset: u16,
    pub value_inode: u32,
    pub value_size: u32,
    pub hash: u32,
    pub name: Vec<u8>,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum XattrNamePrefix {
    User = 1,
    SystemPosixAclAccess = 2,
    SystemPosixAclDefault = 3,
    Trusted = 4,
    Security = 6,
    System = 7,
    SystemRichAcl = 8,
}

#[derive(Debug)]
struct InvalidPrefix(u8);

impl XattrNamePrefix {
    fn from_raw(raw: u8) -> Result<Option<Self>, InvalidPrefix> {
        Ok(match raw {
            0 => None,
            1 => Some(Self::User),
            2 => Some(Self::SystemPosixAclAccess),
            3 => Some(Self::SystemPosixAclDefault),
            4 => Some(Self::Trusted),
            6 => Some(Self::Security),
            7 => Some(Self::System),
            8 => Some(Self::SystemRichAcl),
            other => return Err(InvalidPrefix(other)),
        })
    }
    fn as_str(&self) -> &[u8] {
        match self {
            Self::User => b"user",
            Self::SystemPosixAclAccess => b"system.posix_acl_access",
            Self::SystemPosixAclDefault => b"system.posix_acl_default",
            Self::Trusted => b"trusted",
            Self::Security => b"security",
            Self::System => b"system",
            Self::SystemRichAcl => b"system.rich_acl",
        }
    }
}

impl XattrEntry {
    pub fn prefix(&self) -> Option<XattrNamePrefix> {
        XattrNamePrefix::from_raw(self.name_index).expect("Validation passthrough when checking the xattr name prefix")
    }
    pub fn name(&self) -> Vec<u8> {
        let mut name = self.name.clone();

        if let Some(prefix) = self.prefix() {
            let prefix = prefix.as_str();

            // Prepend prefix and then a dot to the name.
            name.splice(0..0, ArrayVec::from([prefix, &[b'.']]).into_iter().flatten().copied());
        }
        name
    }
    pub fn is_system_data(&self) -> bool {
        &self.name == b"data" && self.prefix() == Some(XattrNamePrefix::System)
    }
}
impl<'a> scroll::ctx::TryFromCtx<'a, scroll::Endian> for XattrEntry {
    type Error = scroll::Error;

    fn try_from_ctx(from: &'a [u8], ctx: scroll::Endian) -> Result<(Self, usize), Self::Error> {
        let mut offset = 0;

        let name_length: u8 = from.gread_with(&mut offset, ctx)?;
        let name_index = from.gread_with(&mut offset, ctx)?;
        let value_offset = from.gread_with(&mut offset, ctx)?;
        let value_inode = from.gread_with(&mut offset, ctx)?;
        let value_size = from.gread_with(&mut offset, ctx)?;
        let hash = from.gread_with(&mut offset, ctx)?;

        let len = offset + name_length as usize;
        if from.len() < len {
            return Err(scroll::Error::BadOffset(len - 1));
        }

        let name = from[offset..len].to_owned();

        Ok((Self {
            name_length,
            name_index,
            value_offset,
            value_inode,
            value_size,
            hash,
            name,
        }, len))
    }
}
impl scroll::ctx::TryIntoCtx<scroll::Endian> for XattrEntry {
    type Error = scroll::Error;

    fn try_into_ctx(self, bytes: &mut [u8], ctx: scroll::Endian) -> Result<usize, Self::Error> {
        let mut offset = 0;

        bytes.gwrite_with(self.name_length, &mut offset, ctx)?;
        bytes.gwrite_with(self.name_index, &mut offset, ctx)?;
        bytes.gwrite_with(self.value_offset, &mut offset, ctx)?;
        bytes.gwrite_with(self.value_inode, &mut offset, ctx)?;
        bytes.gwrite_with(self.value_size, &mut offset, ctx)?;
        bytes.gwrite_with(self.hash, &mut offset, ctx)?;

        assert_eq!(self.name_length as usize, self.name.len());

        let len = offset + self.name_length as usize;

        if bytes.len() < len {
            return Err(scroll::Error::BadOffset(len));
        }

        bytes[offset..len].copy_from_slice(&self.name);

        Ok(len)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InlineXattrs {
    pub header: XattrInlineHeader,
    pub entries: HashMap<XattrEntry, Vec<u8>>,
}

quick_error! {
    #[derive(Debug)]
    pub enum LoadXattrsError {
        EaInodeIoError(err: Box<InodeIoError>) {
            from()
            cause(err)
            description("extended attribute inode i/o error")
            display("extended attribute inode i/o error: {}", err)
        }
        ParseError(err: scroll::Error) {
            from()
            cause(err)
            description("parse error")
            display("parse error: {}", err)
        }
    }
}
impl From<InodeIoError> for LoadXattrsError {
    fn from(io: InodeIoError) -> Self {
        Box::new(io).into()
    }
}

impl InlineXattrs {
    pub fn load<D: fal::Device>(filesystem: &Filesystem<D>, inode: &InodeRaw, inode_bytes: &[u8]) -> Result<Option<Self>, LoadXattrsError> {
        let inode_size = BASE_INODE_SIZE as u16 + inode.ext.as_ref().map(|ext| ext.extra_isize).unwrap_or(0);
        let space_for_xattrs = filesystem.superblock.inode_size() - inode_size;

        if space_for_xattrs < 4 {
            return Ok(None);
        }

        let mut offset = inode_size as usize;

        let header: XattrInlineHeader = inode_bytes.gread_with(&mut offset, scroll::LE)?;

        if header.magic != MAGIC {
            return Ok(None);
        }

        let mut entries = HashMap::new();

        loop {
            let entry: XattrEntry = match inode_bytes.gread_with(&mut offset, scroll::LE) {
                Ok(e) => e,
                Err(scroll::Error::BadOffset(_)) => break,
                Err(other) => return Err(other.into()),
            };

            if entry.name_length == 0 {
                // This makes little sense, but I'm not sure about an alternative.
                break
            }

            let value = if entry.value_inode == 0 {
                let base = inode_size as usize + INLINE_HEADER_SIZE;
                inode_bytes[base + entry.value_offset as usize..base + entry.value_offset as usize + entry.value_size as usize].to_owned()
            } else {
                if filesystem.superblock.incompat_features().contains(RequiredFeatureFlags::EA_INODE) {
                    panic!("Xattr using EA_INODE even though the feature flag has been disabled.")
                }
                let inode = Inode::load(filesystem, entry.value_inode)?;

                let mut bytes = vec! [0u8; entry.value_size as usize];
                let bytes_read = inode.read(filesystem, entry.value_offset.into(), &mut bytes)?;

                assert_eq!(bytes_read, bytes.len());

                bytes
            };

            entries.insert(entry, value);
        }

        Ok(Some(Self {
            header,
            entries,
        }))
    }
}
