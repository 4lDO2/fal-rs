use enum_primitive::*;
use fal::parsing::{read_u16, read_u32};

#[derive(Clone, Debug)]
pub struct CryptoFlags;

enum_from_primitive! {
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    pub enum ProtectionClass {
        DirNone = 0,
        A = 1,
        B = 2,
        C = 3,
        D = 4,
        F = 6,
    }
}

pub type CryptoKeyClass = ProtectionClass;

// TODO
type CryptoKeyOsVersion = u32;

type CryptoKeyRevision = u16;

#[derive(Debug)]
pub struct WrappedMetaCryptoState {
    major_version: u16,
    minor_version: u16,
    cpflags: CryptoFlags,
    persistent_class: CryptoKeyClass,
    key_os_version: CryptoKeyOsVersion,
    key_revision: CryptoKeyRevision,
    unused: u16,
}

impl WrappedMetaCryptoState {
    const LEN: usize = 20;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        Self {
            major_version: read_u16(bytes, &mut offset),
            minor_version: read_u16(bytes, &mut offset),
            cpflags: CryptoFlags, // takes up 4 bytes
            persistent_class: CryptoKeyClass::from_u32(read_u32(bytes, &mut offset)).unwrap(),
            key_os_version: read_u32(bytes, &mut offset),
            key_revision: read_u16(bytes, &mut offset),
            unused: read_u16(bytes, &mut offset),
        }
    }
}
