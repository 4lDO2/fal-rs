use std::{
    io::SeekFrom,
    sync::Mutex,
};

use crate::{
    DiskKey, DiskKeyType,
    oid,
    superblock::Superblock,
    tree::Tree,
};

const FIRST_CHUNK_TREE_OBJECTID: u64 = 256;

pub fn read_node_raw<D: fal::Device>(device: &mut D, superblock: &Superblock, offset: u64) -> Box<[u8]> {
    let mut bytes = vec! [0u8; superblock.node_size as usize];
    device.seek(SeekFrom::Start(offset)).unwrap();
    device.read_exact(&mut bytes).unwrap();
    bytes.into_boxed_slice()
}
pub fn read_node<D: fal::Device>(filesystem: &mut Filesystem<D>, offset: u64) -> Box<[u8]> {
    read_node_raw(&mut *filesystem.device.lock().unwrap(), &filesystem.superblock, offset)
}

#[derive(Debug)]
pub struct Filesystem<D: fal::Device> {
    pub device: Mutex<D>,
    pub superblock: Superblock,
}

impl<D: fal::Device> Filesystem<D> {
    pub fn mount(mut device: D) -> Self {
        let superblock = Superblock::load(&mut device);

        let (key, first_chunk_tree_item) = match superblock.system_chunk_array.0.iter().find(|(k, _)| k.oid == FIRST_CHUNK_TREE_OBJECTID) {
            Some(t) => t,
            None => panic!("Couldn't find the first chunk tree"),
        };

        // TODO: RAID
        assert_eq!(key.offset, first_chunk_tree_item.stripes[0].offset);

        let mapping = key.offset .. key.offset + first_chunk_tree_item.len;
        assert!(mapping.contains(&superblock.chunk_root) && mapping.contains(&(superblock.chunk_root + u64::from(superblock.node_size) - 1)));

        let chunk_tree = Tree::load(&mut device, &superblock, superblock.chunk_root);
        let root_tree = Tree::load(&mut device, &superblock, superblock.root);

        let extent_tree = Self::load_tree(&mut device, &superblock, &root_tree, oid::EXTENT_TREE);
        let dev_tree = Self::load_tree(&mut device, &superblock, &root_tree, oid::DEV_TREE);
        let fs_tree = Self::load_tree(&mut device, &superblock, &root_tree, oid::FS_TREE);
        let csum_tree = Self::load_tree(&mut device, &superblock, &root_tree, oid::CSUM_TREE);

        for _ in chunk_tree.pairs(&mut device, &superblock) {}
        for _ in root_tree.pairs(&mut device, &superblock) {}
        for _ in extent_tree.pairs(&mut device, &superblock) {}
        for _ in dev_tree.pairs(&mut device, &superblock) {}
        for _ in fs_tree.pairs(&mut device, &superblock) {}
        for _ in csum_tree.pairs(&mut device, &superblock) {}

        Self {
            device: Mutex::new(device),
            superblock,
        }
    }
    fn load_tree(device: &mut D, superblock: &Superblock, tree: &Tree, oid: u64) -> Tree {
        let root_item = tree.get(device, superblock, &DiskKey {
            oid,
            ty: DiskKeyType::RootItem,
            offset: 0,
        }).unwrap().into_root_item().unwrap();

        Tree::load(device, superblock, root_item.addr)
    }
}
