use std::{
    io::SeekFrom,
    sync::Mutex,
};

use crate::{
    chunk_map::ChunkMap,
    DiskKey, DiskKeyType,
    oid,
    superblock::Superblock,
    tree::Tree,
};

const FIRST_CHUNK_TREE_OBJECTID: u64 = 256;

pub fn read_node_phys<D: fal::Device>(device: &mut D, superblock: &Superblock, offset: u64) -> Box<[u8]> {
    let mut bytes = vec! [0u8; superblock.node_size as usize];
    device.seek(SeekFrom::Start(offset)).unwrap();
    device.read_exact(&mut bytes).unwrap();
    bytes.into_boxed_slice()
}

pub fn read_node<D: fal::Device>(device: &mut D, superblock: &Superblock, chunk_map: &ChunkMap, offset: u64) -> Box<[u8]> {
    read_node_phys(device, superblock, chunk_map.get(superblock, offset).unwrap())
}

#[derive(Debug)]
pub struct Filesystem<D: fal::Device> {
    pub device: Mutex<D>,
    pub superblock: Superblock,

    pub chunk_map: ChunkMap,
}

impl<D: fal::Device> Filesystem<D> {
    pub fn mount(mut device: D) -> Self {
        let superblock = Superblock::load(&mut device);

        let mut chunk_map = ChunkMap::read_sys_chunk_array(&superblock);

        let chunk_tree = Tree::load(&mut device, &superblock, &chunk_map, superblock.chunk_root);
        chunk_map.read_chunk_tree(&mut device, &superblock, &chunk_tree);

        let root_tree = Tree::load(&mut device, &superblock, &chunk_map, superblock.root);

        let extent_tree = Self::load_tree(&mut device, &superblock, &chunk_map, &root_tree, oid::EXTENT_TREE).unwrap();
        let dev_tree = Self::load_tree(&mut device, &superblock, &chunk_map, &root_tree, oid::DEV_TREE).unwrap();
        let fs_tree = Self::load_tree(&mut device, &superblock, &chunk_map, &root_tree, oid::FS_TREE).unwrap();
        let csum_tree = Self::load_tree(&mut device, &superblock, &chunk_map, &root_tree, oid::CSUM_TREE).unwrap();

        // It seems like the quota tree may not necessarily exist. Or, the quota tree is simply
        // stored somewhere else.
        let quota_tree = Self::load_tree(&mut device, &superblock, &chunk_map, &root_tree, oid::QUOTA_TREE);

        let uuid_tree = Self::load_tree(&mut device, &superblock, &chunk_map, &root_tree, oid::UUID_TREE).unwrap();

        // The existence of the free space tree also seems optional. It's possibly located in the
        // fs tree, right?
        let free_space_tree = Self::load_tree(&mut device, &superblock, &chunk_map, &root_tree, oid::FREE_SPACE_TREE);

        // Simply iterate through the tree to see if a disk key type is unimplemented.
        for (key, chunk_item) in chunk_tree.pairs(&mut device, &superblock, &chunk_map) {
            if let crate::tree::Value::Chunk(item) = chunk_item {
                assert_eq!(key.offset, item.stripes[0].offset);
            }
        }
        for (k, v) in root_tree.pairs(&mut device, &superblock, &chunk_map) {
            if k.ty == DiskKeyType::RootItem {
                dbg!(k, v);
            }
        }
        for _ in extent_tree.pairs(&mut device, &superblock, &chunk_map) {}
        for _ in dev_tree.pairs(&mut device, &superblock, &chunk_map) {}
        for _ in fs_tree.pairs(&mut device, &superblock, &chunk_map) {}
        for _ in csum_tree.pairs(&mut device, &superblock, &chunk_map) {}
        if let Some(quota_tree) = quota_tree { for _ in quota_tree.pairs(&mut device, &superblock, &chunk_map) {} }
        for _ in uuid_tree.pairs(&mut device, &superblock, &chunk_map) {}
        if let Some(free_space_tree) = free_space_tree { for _ in free_space_tree.pairs(&mut device, &superblock, &chunk_map) {} }

        Self {
            device: Mutex::new(device),
            superblock,

            chunk_map,
        }
    }
    fn load_tree(device: &mut D, superblock: &Superblock, chunk_map: &ChunkMap, tree: &Tree, oid: u64) -> Option<Tree> {
        let value = match tree.get(device, superblock, chunk_map, &DiskKey {
            oid,
            ty: DiskKeyType::RootItem,
            offset: 0,
        }) {
            Some(v) => v,
            None => return None,
        };
        let root_item = value.into_root_item().unwrap();

        Some(Tree::load(device, superblock, chunk_map, root_item.addr))
    }
}