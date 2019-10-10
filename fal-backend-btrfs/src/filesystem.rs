use std::sync::Mutex;
use crate::superblock::Superblock;

const FIRST_CHUNK_TREE_OBJECTID: u64 = 256;

#[derive(Debug)]
pub struct Filesystem<D: fal::Device> {
    pub device: Mutex<D>,
    pub superblock: Superblock,
}

impl<D: fal::Device> Filesystem<D> {
    pub fn mount(mut device: D) -> Self {
        let superblock = Superblock::load(&mut device);

        let (_, first_chunk_tree_item) = match superblock.system_chunk_array.0.iter().find(|(k, v)| k.oid == FIRST_CHUNK_TREE_OBJECTID) {
            Some(t) => t,
            None => panic!("Couldn't find the first chunk tree"),
        };

        let chunk_tree_bytes = vec! [0; first_chunk_tree_item.stripe.size];

        Self {
            device: Mutex::new(device),
            superblock,
        }
    }
}
