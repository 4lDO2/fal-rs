use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::OsStr,
    io::SeekFrom,
    sync::{
        atomic::{self, AtomicU64},
        Mutex
    },
};

use chashmap::CHashMap;

use crate::{
    btree::{BTree, BTreeKey, Pairs},
    file_io::{Extra, FileHandle, Inode},
    fsobjects::{InodeType, JAnyKey, JDrecKey, JDrecHashedKey, JInodeKey, JFileExtentKey, JXattrKey},
    checkpoint::{
        self, CheckpointMapping, CheckpointMappingPhys, GenericObject,
    },
    omap::{Omap, OmapKey, OmapValue},
    read_block, read_block_to,
    spacemanager::SpacemanagerPhys,
    superblock::{ApfsSuperblock, NxSuperblock},
    BlockAddr, ObjPhys, ObjectIdentifier, ObjectType,
};

pub struct Filesystem<D: fal::Device> {
    pub device: Mutex<D>,
    pub container_superblock: NxSuperblock,
    pub ephemeral_objects: HashMap<ObjectIdentifier, GenericObject>,
    pub mounted_volumes: Vec<Volume>,

    pub file_handles: CHashMap<u64, FileHandle>,
    pub last_fh: AtomicU64,

    pub spacemanager_oid: ObjectIdentifier,
    pub reaper_oid: ObjectIdentifier,
}

impl<D: fal::Device> Filesystem<D> {
    pub fn mount(mut device: D) -> Self {
        let container_superblock = NxSuperblock::load(&mut device);

        if container_superblock.chkpnt_desc_blkcnt & (1 << 31) != 0 {
            unimplemented!("B-tree checkpoints aren't implemented as of now")
        }
        // Otherwise, checkpoint descriptor area is contiguous.

        let block_size = container_superblock.block_size as usize;

        let mut descriptor_area =
            vec![0u8; block_size * container_superblock.chkpnt_desc_blkcnt() as usize];

        for block_index in 0..container_superblock.chkpnt_desc_blkcnt() {
            let range = block_index as usize * block_size..(block_index as usize + 1) * block_size;
            read_block_to(
                &container_superblock,
                &mut device,
                &mut descriptor_area[range.clone()],
                container_superblock.chkpnt_desc_base + i64::from(block_index),
            );

            let obj = ObjPhys::parse(&descriptor_area[range]);
            assert!(obj.object_type.ty == ObjectType::CheckpointMap || obj.object_type.ty == ObjectType::NxSuperblock);
        }

        let superblock = (0..container_superblock.chkpnt_desc_len)
            .map(|i| {
                checkpoint::read_from_desc_area(
                    &mut device,
                    &container_superblock,
                    container_superblock.chkpnt_desc_first + i,
                )
            })
            .filter_map(|entry| entry.into_superblock())
            .filter(|superblock| superblock.is_valid())
            .max_by_key(|superblock| superblock.header.transaction_id)
            .unwrap();

        let ephemeral_object_ids = (0..container_superblock.chkpnt_desc_len)
            .map(|i| {
                checkpoint::read_from_desc_area(
                    &mut device,
                    &container_superblock,
                    container_superblock.chkpnt_desc_first + i,
                )
            })
            .filter_map(|entry| entry.into_mapping())
            .map(|mapping: CheckpointMappingPhys| {
                Vec::from(mapping.mappings)
                    .into_iter()
                    .map(|mapping: CheckpointMapping| (mapping.oid, mapping.paddr))
            })
            .flatten()
            .collect::<Vec<_>>();

        let ephemeral_objects = ephemeral_object_ids
            .into_iter()
            .map(|(id, paddr)| {
                (
                    id,
                    checkpoint::read_from_data_area(
                        &mut device,
                        &container_superblock,
                        (paddr.0 - superblock.chkpnt_data_base as u64) as u32,
                    )
                    .unwrap(),
                )
            })
            .collect::<HashMap<ObjectIdentifier, GenericObject>>();

        let spacemanager_oid = ephemeral_objects.iter().find(|(_, v)| v.header().ty() == ObjectType::SpaceManager).expect("Volume missing space manager").1.header().oid();
        let reaper_oid = ephemeral_objects.iter().find(|(_, v)| v.header().ty() == ObjectType::NxReaper).expect("Volume missing reaper").1.header().oid();

        dbg!(&ephemeral_objects[&spacemanager_oid]);

        let container_superblock: NxSuperblock = superblock;

        let omap = Omap::load(
            &mut device,
            &container_superblock,
            container_superblock.object_map_oid.0 as i64,
        );

        let mounted_volumes = container_superblock
            .volumes_oids
            .iter()
            .copied()
            .take_while(|oid| oid.is_valid())
            .map(|volume| {
                let (_, omap_value) = omap
                    .get_partial_latest(
                        &mut device,
                        &container_superblock,
                        OmapKey::partial(volume),
                    )
                    .expect("Volume virtual oid_t wasn't found in the omap B+ tree.");

                Volume::load(&mut device, &container_superblock, omap_value.paddr)
            })
            .collect::<Vec<_>>();

        Self {
            container_superblock,
            device: Mutex::new(device),
            ephemeral_objects,
            mounted_volumes,
            file_handles: CHashMap::new(),
            last_fh: AtomicU64::new(0),
            spacemanager_oid,
            reaper_oid,
        }
    }
    pub fn volume(&self) -> &Volume {
        &self.mounted_volumes[0]
    }
    pub fn fh(&self) -> u64 {
        self.last_fh.fetch_add(1, atomic::Ordering::SeqCst)
    }
}
impl<D: fal::DeviceMut> Filesystem<D> {
    pub fn write_block(
        superblock: &NxSuperblock,
        device: &mut D,
        address: BlockAddr,
        block: &[u8],
    ) {
        debug_assert_eq!(block.len(), superblock.block_size as usize);
        device
            .seek(SeekFrom::Start(
                address as u64 * u64::from(superblock.block_size),
            ))
            .unwrap();
        device.write_all(&block).unwrap();
    }
}

#[derive(Debug)]
pub struct Volume {
    pub superblock: ApfsSuperblock,
    pub omap: Omap,
    pub root_tree: BTree,
}

impl Volume {
    pub fn load<D: fal::Device>(device: &mut D, nx_super: &NxSuperblock, phys: BlockAddr) -> Self {
        let superblock = ApfsSuperblock::parse(&read_block(nx_super, device, phys));

        let omap = Omap::load(device, nx_super, superblock.omap_oid.0 as i64);

        let root_oid_phys = Self::root_oid_phys(device, nx_super, &omap, &superblock).paddr;
        let root_tree = BTree::load(device, nx_super, root_oid_phys);

        Self {
            omap,
            superblock,
            root_tree,
        }
    }
    fn root_oid_phys<D: fal::Device>(
        device: &mut D,
        superblock: &NxSuperblock,
        omap: &Omap,
        vol_superblock: &ApfsSuperblock,
    ) -> OmapValue {
        omap.get_partial_latest(
            device,
            superblock,
            OmapKey::partial(vol_superblock.root_tree_oid),
        )
        .unwrap()
        .1
    }
}

type Result<T> = fal::Result<T>;
impl<D: fal::Device> fal::Filesystem<D> for Filesystem<D> {
    type InodeAddr = u64;
    type InodeStruct = Inode;

    fn root_inode(&self) -> Self::InodeAddr {
        Inode::ROOT_DIR_INODE_ADDR
    }
    fn mount(device: D, _path: &OsStr) -> Self {
        Filesystem::mount(device)
    }
    fn unmount(self) {}

    fn load_inode(&mut self, address: Self::InodeAddr) -> Result<Self::InodeStruct> {
        let mut device = self.device.lock().unwrap();

        let key = JInodeKey::new(address.into());
        let value = match self.volume().root_tree.get(&mut *device, &self.container_superblock, Some(&self.volume().omap), &BTreeKey::FsLayerKey(JAnyKey::InodeKey(key.clone()))) {
            Some(i) => i,
            None => return Err(fal::Error::NoEntity)
        };
        let inode = Inode {
            block_size: self.container_superblock.block_size,
            key,
            value: value.into_inode_value().unwrap(),
        };
        Ok(inode)
    }

    fn open_file(&mut self, inode: Self::InodeAddr) -> Result<u64> {
        let mut device = self.device.lock().unwrap();

        let inode_key = JInodeKey::new(inode.into());

        let inode_val = match self.volume().root_tree.get(&mut *device, &self.container_superblock, Some(&self.volume().omap), &BTreeKey::FsLayerKey(JAnyKey::InodeKey(inode_key.clone()))) {
            Some(i) => i,
            None => return Err(fal::Error::NoEntity),
        }.into_inode_value().unwrap();

        // Normally we would check that this really is a file and not a directory, however the
        // Redox frontend opens the root dir as a file and then checks whether or not it's a
        // directory. We check if it's a file at read() calls.

        let partial_key = BTreeKey::FsLayerKey(JAnyKey::FileExtentKey(JFileExtentKey::partial(inode.into())));

        let fh = self.fh();
        let extra = match self.volume().root_tree.get_generic(&mut *device, &self.container_superblock, Some(&self.volume().omap), &partial_key, JAnyKey::partial_compare) {
            Some((_, p)) => {
                Some(Extra {
                    path: p.into_iter().map(|(tree, idx)| (Cow::Owned(tree.into_owned()), idx)).collect(),
                    previous_key: None,
                })
            }
            None => {
                None
            }
        };
        self.file_handles.insert(fh,
                FileHandle {
                    fh,
                    offset: 0,
                    inode: Inode {
                        block_size: self.container_superblock.block_size,
                        value: inode_val,
                        key: inode_key,
                    },
                    extra,
                    current_extent: None,
                });
        Ok(fh)
    }

    fn read(&mut self, fh: u64, mut offset: u64, buffer: &mut [u8]) -> Result<usize> {
        let mut device = self.device.lock().unwrap();

        let mut file_handle = match self.file_handles.get_mut(&fh) {
            Some(fh) => fh,
            None => return Err(fal::Error::BadFd),
        };
        if file_handle.inode.value.ty == InodeType::Dir {
            return Err(fal::Error::IsDirectory);
        }

        let mut buf = buffer;

        let mut bytes_read = 0;

        // TODO: Wrap most of this function into something like read_datastream(oid, offset, buffer).

        loop {
            if buf.is_empty() {
                break
            }
            let (current_extent_key, current_extent_val) = match file_handle.current_extent {
                Some((k, v)) if offset >= k.logical_addr && offset - k.logical_addr < v.length => (k, v),
                _ => {
                    // Get a new extent.

                    let extra = match file_handle.extra.take() {
                        Some(d) => d,
                        None => return Ok(0),
                    };
                    let mut iterator = Pairs::<'static, '_> {
                        device: &mut *device,
                        compare: JAnyKey::partial_compare,
                        omap: Some(&self.volume().omap),
                        superblock: &self.container_superblock,

                        path: extra.path,
                        previous_key: extra.previous_key,
                    };

                    let (k, v) = iterator.next().unwrap();
                    file_handle.current_extent = Some((k.into_fs_layer_key().unwrap().into_file_extent_key().unwrap(), v.into_file_extent_value().unwrap()));

                    let extra = Extra {
                        path: iterator.path,
                        previous_key: iterator.previous_key,
                    };

                    file_handle.extra = Some(extra);
                    file_handle.current_extent.unwrap()
                }
            };
            let block_size = u64::from(self.container_superblock.block_size);
            assert_eq!(current_extent_val.length % block_size, 0);
            let extent_block_count = current_extent_val.length / block_size;

            let bytes_from_extent_start = offset - current_extent_key.logical_addr;
            let bytes_to_extent_end = current_extent_val.length - bytes_from_extent_start;
            let bytes_to_read = std::cmp::min(bytes_to_extent_end as usize, buf.len());

            let mut extent_bytes_read = 0;

            let blocks_before_offset = bytes_from_extent_start / block_size;
            let blocks_to_read = fal::div_round_up(bytes_to_read, block_size as usize);

            for (index, baddr) in (current_extent_val.physical_block_num..current_extent_val.physical_block_num + extent_block_count).enumerate().skip(blocks_before_offset as usize).take(blocks_to_read) {
                let bytes_before_offset = bytes_from_extent_start.saturating_sub(index as u64 * block_size) as usize;

                let block_bytes_to_read = std::cmp::min(bytes_to_read, self.container_superblock.block_size as usize);
                let block = read_block(&self.container_superblock, &mut *device, baddr as i64);
                buf[extent_bytes_read..extent_bytes_read + block_bytes_to_read].copy_from_slice(&block[bytes_before_offset..block_bytes_to_read]);
                extent_bytes_read += block_bytes_to_read;
            }

            bytes_read += extent_bytes_read;
            offset += extent_bytes_read as u64;
            buf = &mut buf[extent_bytes_read..];
        }

        file_handle.offset += bytes_read as u64;

        Ok(bytes_read)
    }

    fn close(&mut self, fh: u64) -> Result<()> {
        match self.file_handles.remove(&fh) {
            Some(_) => Ok(()),
            None => Err(fal::Error::BadFd),
        }
    }

    fn open_directory(&mut self, address: Self::InodeAddr) -> Result<u64> {
        let mut device = self.device.lock().unwrap();

        let inode_key = JInodeKey::new(address.into());
        let inode_val = match self.volume().root_tree.get(&mut *device, &self.container_superblock, Some(&self.volume().omap), &BTreeKey::FsLayerKey(JAnyKey::InodeKey(inode_key.clone()))) {
            Some(i) => i,
            None => return Err(fal::Error::NoEntity),
        }.into_inode_value().unwrap();

        if inode_val.ty != InodeType::Dir {
            return Err(fal::Error::NotDirectory);
        }

        // TODO: Support both JDrecHashedKey and JDrecKey.

        // Precompute the path to the first entry.
        let partial_key = BTreeKey::FsLayerKey(JAnyKey::DrecHashedKey(JDrecHashedKey::partial(address.into())));

        let fh = self.fh();
        let extra = match self.volume().root_tree.get_generic(&mut *device, &self.container_superblock, Some(&self.volume().omap), &partial_key, JAnyKey::partial_compare) {
            Some((_, p)) => {
                Some(Extra {
                    path: p.into_iter().map(|(tree, idx)| (Cow::Owned(tree.into_owned()), idx)).collect(),
                    previous_key: None,
                })
            }
            None => {
                None
            }
        };
        self.file_handles.insert(fh,
            FileHandle {
                fh,
                offset: 0,
                inode: Inode {
                    block_size: self.container_superblock.block_size,
                    value: inode_val,
                    key: inode_key,
                },
                extra,
                current_extent: None,
            }
        );
        Ok(fh)
    }

    fn read_directory(
        &mut self,
        fh: u64,
        offset: i64,
    ) -> Result<Option<fal::DirectoryEntry<Self::InodeAddr>>> {
        let mut device = self.device.lock().unwrap();

        let mut file_handle = match self.file_handles.get_mut(&fh) {
            Some(fh) => fh,
            None => return Err(fal::Error::BadFd),
        };
        if file_handle.inode.value.ty != InodeType::Dir {
            return Err(fal::Error::NotDirectory);
        }
        let extra = match file_handle.extra.take() {
            Some(d) => d,
            None => return Ok(None),
        };
        let mut iterator = Pairs::<'static, '_> {
            device: &mut *device,
            compare: JAnyKey::partial_compare,
            omap: Some(&self.volume().omap),
            superblock: &self.container_superblock,

            path: extra.path,
            previous_key: extra.previous_key,
        };
        if offset < 0 { unimplemented!("offsets below zero, no walking backwards is implemented for the Pairs iterator") }

        let to_skip = (offset as usize).checked_sub(1).unwrap_or(0);

        let (idx, (key, value)) = match (&mut iterator).enumerate().skip(to_skip).next() {
            Some((idx, (key, value))) => (idx, (key.into_fs_layer_key().unwrap().into_drec_hashed_key().unwrap(), value.into_drec_value().unwrap())),
            None => return Ok(None),
        };
        let extra = Extra {
            path: iterator.path,
            previous_key: iterator.previous_key,
        };
        file_handle.offset += idx as u64;
        file_handle.extra = Some(extra);

        let dir_entry = fal::DirectoryEntry {
            inode: key.header.oid.into(),
            name: key.name.into(),
            filetype: value.flags.ty.into(),
            offset: file_handle.offset,
        };

        Ok(Some(dir_entry))
    }

    fn lookup_direntry(
        &mut self,
        parent: Self::InodeAddr,
        name: &OsStr,
    ) -> Result<fal::DirectoryEntry<Self::InodeAddr>> {
        let mut device = self.device.lock().unwrap();

        let hashed_key = JDrecHashedKey::new(parent.into(), name.to_string_lossy().into_owned());
        let value = match self.volume().root_tree.get(&mut *device, &self.container_superblock, Some(&self.volume().omap), &BTreeKey::FsLayerKey(JAnyKey::DrecHashedKey(hashed_key.clone()))) {
            Some(i) => i,
            None => {
                let regular_key = JDrecKey::new(parent.into(), name.to_string_lossy().into_owned());
                match self.volume().root_tree.get(&mut *device, &self.container_superblock, Some(&self.volume().omap), &BTreeKey::FsLayerKey(JAnyKey::DrecKey(regular_key.clone()))) {
                    Some(i) => i,
                    None => return Err(fal::Error::NoEntity),
                }
            }
        }.into_drec_value().unwrap();

        Ok(fal::DirectoryEntry {
            offset: 0,
            filetype: value.flags.ty.into(),
            inode: value.file_id,
            name: name.to_owned(),
        })
    }

    fn readlink(&mut self, inode: Self::InodeAddr) -> Result<Box<[u8]>> {
        let mut device = self.device.lock().unwrap();

        let inode_key = JInodeKey::new(inode.into());
        let inode_val = match self.volume().root_tree.get(&mut *device, &self.container_superblock, Some(&self.volume().omap), &BTreeKey::FsLayerKey(JAnyKey::InodeKey(inode_key.clone()))) {
            Some(i) => i,
            None => return Err(fal::Error::NoEntity),
        }.into_inode_value().unwrap();

        let xattr_key = JXattrKey::new(inode.into(), JXattrKey::SYMLINK_XATTR_NAME.to_owned());
        let xattr_val = match self.volume().root_tree.get(&mut *device, &self.container_superblock, Some(&self.volume().omap), &BTreeKey::FsLayerKey(JAnyKey::XattrKey(xattr_key.clone()))) {
            Some(i) => i,
            None => return Err(fal::Error::Other(-1)),
        };

        // TODO: streams
        Ok(xattr_val.into_xattr_value().unwrap().embedded.unwrap().into_boxed_slice())
    }

    fn fh_offset(&self, fh: u64) -> u64 {
        self.file_handles.get(&fh).unwrap().offset
    }

    fn fh_inode(&self, fh: u64) -> Inode {
        self.file_handles.get(&fh).unwrap().inode.clone()
    }

    fn set_fh_offset(&mut self, fh: u64, offset: u64) {
        self.file_handles.get_mut(&fh).unwrap().offset = offset;
    }

    fn filesystem_attrs(&self) -> fal::FsAttributes {
        let spacemanager: &SpacemanagerPhys = &self.ephemeral_objects[&self.spacemanager_oid].as_spacemanager().unwrap();

        let device = spacemanager.main_device();

        fal::FsAttributes {
            block_size: spacemanager.block_size,

            available_blocks: device.free_count, // FIXME
            free_blocks: device.free_count,

            free_inodes: 42, // since APFS doesn't have an inode bitmap and array like ext2, any amount of inodes may be allocated (as long as there are blocks left).
            inode_count: 42,
            max_fname_len: 255, // TODO: I could't find the value of this constant in the documentation, however calling statvfs on a macOS machine gave me 255.
            total_blocks: device.block_count,
        }
    }
}

impl<D: fal::DeviceMut> fal::FilesystemMut<D> for Filesystem<D> {
    fn store_inode(&mut self, inode: &Inode) -> Result<()> {
        unimplemented!()
    }
}
