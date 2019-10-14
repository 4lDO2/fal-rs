use std::{
    io::SeekFrom,
    sync::Mutex,
};

use crate::superblock::Superblock;

const FIRST_CHUNK_TREE_OBJECTID: u64 = 256;

fn read_range<D: fal::Device>(device: &mut D, offset: u64, len: usize) -> Box<[u8]> {
    let mut bytes = vec! [0u8; len];
    device.seek(SeekFrom::Start(offset)).unwrap();
    device.read_exact(&mut bytes).unwrap();
    bytes.into_boxed_slice()
}

#[derive(Debug)]
pub struct Filesystem<D: fal::Device> {
    pub device: Mutex<D>,
    pub superblock: Superblock,
}

impl<D: fal::Device> Filesystem<D> {
    pub fn mount(mut device: D) -> Self {
        let superblock = Superblock::load(&mut device);

        let (key, first_chunk_tree_item) = match superblock.system_chunk_array.0.iter().find(|(k, v)| k.oid == FIRST_CHUNK_TREE_OBJECTID) {
            Some(t) => t,
            None => panic!("Couldn't find the first chunk tree"),
        };

        // TODO: RAID
        assert_eq!(key.offset, first_chunk_tree_item.stripe.offset);

        let chunk_tree_bytes = read_range(&mut device, key.offset, first_chunk_tree_item.len as usize);

        Self {
            device: Mutex::new(device),
            superblock,
        }
    }
}
