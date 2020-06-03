use std::sync::{Mutex, RwLock};

use core::convert::{TryFrom, TryInto};
use core::sync::atomic::{self, AtomicU64};

use alloc::borrow::Cow;
use alloc::collections::BTreeMap;

use fal::Inode as _;

use crate::{
    chunk_map::ChunkMap,
    items::{DirItem, Filetype, InodeItem},
    oid,
    superblock::Superblock,
    tree::{Tree, TreeOwned, Value},
    u64_le, DiskKey, DiskKeyType, Timespec,
};

pub const FIRST_CHUNK_TREE_OBJECTID: u64 = 256;

pub fn read_node_phys<D: fal::DeviceRo>(
    device: &D,
    superblock: &Superblock,
    offset: u64,
) -> Box<[u8]> {
    let mut bytes = vec![0u8; superblock.node_size.get() as usize];
    // FIXME
    debug_assert_eq!(
        u64::from(superblock.node_size.get()) % u64::from(device.disk_info().unwrap().block_size),
        0
    );
    device
        .read_blocks(
            offset / u64::from(device.disk_info().unwrap().block_size),
            &mut bytes,
        )
        .unwrap();
    bytes.into_boxed_slice()
}

pub fn read_node<D: fal::DeviceRo>(
    device: &D,
    superblock: &Superblock,
    chunk_map: &ChunkMap,
    offset: u64,
) -> Box<[u8]> {
    read_node_phys(
        device,
        superblock,
        chunk_map.get(superblock, offset).unwrap(),
    )
}

#[derive(Debug)]
pub enum Handle {
    File,
    Directory {
        fal_address: u64,
        oid: u64,
        subvolid: u64,
        offset: u64,
    },
}

#[derive(Debug)]
pub struct Filesystem<D: fal::DeviceRo> {
    device: D,
    superblock: Superblock,

    handles: RwLock<BTreeMap<u64, Mutex<Handle>>>,
    next_handle: AtomicU64,

    chunk_map: ChunkMap,

    /// Handles translation between the inode numbers that the fal API sees, and the
    /// objectid+subvol-id pair.
    inodes_translation: RwLock<BTreeMap<u64, (u64, u64)>>,
    inodes_translation_rev: RwLock<BTreeMap<(u64, u64), u64>>,
    next_inode: AtomicU64,

    root_tree: TreeOwned,
    chunk_tree: TreeOwned,
    extent_tree: TreeOwned,
    dev_tree: TreeOwned,
    pub subvolumes: RwLock<BTreeMap<u64, RwLock<TreeOwned>>>,
    csum_tree: TreeOwned,
    quota_tree: Option<TreeOwned>,
    uuid_tree: TreeOwned,
    free_space_tree: Option<TreeOwned>,
}

impl<D: fal::Device> Filesystem<D> {
    pub fn mount(mut device: D) -> Self {
        let superblock = Superblock::load(&mut device);

        println!("Superblock: {:?}", superblock);

        let (ro_compat_flags, readonly) = match superblock.ro_compat_flags() {
            Ok(f) => (f, false),
            Err((f, rest)) => {
                eprintln!("Features potentially from the future: {:#0x} (mask)", rest);
                (f, true)
            }
        };
        let incompat_flags = match superblock.incompat_flags() {
            Ok(f) => f,
            Err(rest) => {
                panic!("Incompat flags were found: {:#0x} (mask)", rest);
            }
        };

        println!(
            "Superblock RO compat flags: {:?}, can mount write: {}",
            ro_compat_flags, !readonly
        );
        println!("Superblock incompat flags: {:?}", incompat_flags);

        let mut chunk_map = ChunkMap::read_sys_chunk_array(&superblock);

        let chunk_tree = Tree::load(
            &mut device,
            &superblock,
            &chunk_map,
            superblock.chunk_root.get(),
        )
        .expect("failed to load chunk tree");
        chunk_map.read_chunk_tree(&mut device, &superblock, chunk_tree.as_ref());

        let root_tree = Tree::load(&mut device, &superblock, &chunk_map, superblock.root.get())
            .expect("failed to load root tree");

        let extent_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::EXTENT_TREE,
        )
        .unwrap();
        let dev_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::DEV_TREE,
        )
        .unwrap();

        let fs_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::FS_TREE,
        )
        .unwrap();

        let csum_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::CSUM_TREE,
        )
        .unwrap();

        // It seems like the quota tree may not necessarily exist. Or, the quota tree is simply
        // stored somewhere else.
        let quota_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::QUOTA_TREE,
        );

        let uuid_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::UUID_TREE,
        )
        .unwrap();

        // TODO: BTRFS_FEATURE_ROCOMPAT_FREESPACETREE{,VALID}
        let free_space_tree = Self::load_tree(
            &mut device,
            &superblock,
            &chunk_map,
            root_tree.as_ref(),
            oid::FREE_SPACE_TREE,
        );

        for item in chunk_tree
            .as_ref()
            .pairs()
            .iter(&device, &superblock, &chunk_map)
        {
            println!("CHUNK ITEM: {:?}\n", item);
        }
        let mut subvol_tree = Some((oid::FS_TREE, fs_tree.clone()));

        loop {
            let (tree_oid, tree) = match subvol_tree.take() {
                Some(t) => t,
                None => break,
            };
            for (key, value) in tree.as_ref().pairs().iter(&device, &superblock, &chunk_map) {
                println!("FS ({}) ITEM: {:?}\n", tree_oid, (key, &value));
                if let (Some(DiskKeyType::DirItem), Some(v)) = (key.ty(), value.into_dir_index()) {
                    let dir_item = v.into_owned();
                    if dir_item.location.ty() == Some(DiskKeyType::RootItem) {
                        // we have found a submodule
                        let oid = dir_item.location.oid.get();
                        subvol_tree = Some((oid, Self::load_tree(&device, &superblock, &chunk_map, root_tree.as_ref(), oid).unwrap()));
                    }
                }
            }
        }
        for item in dev_tree
            .as_ref()
            .pairs()
            .iter(&device, &superblock, &chunk_map)
        {
            println!("DEV ITEM: {:?}\n", item);
        }
        for item in root_tree
            .as_ref()
            .pairs()
            .iter(&device, &superblock, &chunk_map)
        {
            println!("ROOT ITEM: {:?}\n", item);
        }
        for item in extent_tree
            .as_ref()
            .pairs()
            .iter(&device, &superblock, &chunk_map)
        {
            println!("EXTENT ITEM: {:?}\n", item);
        }
        for item in csum_tree
            .as_ref()
            .pairs()
            .iter(&device, &superblock, &chunk_map)
        {
            println!("CSUM ITEM: {:?}\n", item);
        }
        for item in uuid_tree
            .as_ref()
            .pairs()
            .iter(&device, &superblock, &chunk_map)
        {
            println!("UUID ITEM: {:?}\n", item);
        }

        Self {
            device,
            superblock,

            chunk_map,

            root_tree,
            chunk_tree,
            extent_tree,
            dev_tree,
            subvolumes: RwLock::new(std::iter::once((oid::FS_TREE, RwLock::new(fs_tree))).collect()),
            csum_tree,
            quota_tree,
            uuid_tree,
            free_space_tree,

            inodes_translation: RwLock::new(std::iter::once((2, (256, oid::FS_TREE))).collect()),
            inodes_translation_rev: RwLock::new(std::iter::once(((256, oid::FS_TREE), 2)).collect()),
            next_inode: AtomicU64::new(2),

            handles: RwLock::new(BTreeMap::new()),
            next_handle: AtomicU64::new(0),
        }
    }
    fn load_tree(
        device: &D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        tree: Tree,
        oid: u64,
    ) -> Option<TreeOwned> {
        let value = match tree.get(
            device,
            superblock,
            chunk_map,
            &DiskKey {
                oid: u64_le::new(oid),
                ty: DiskKeyType::RootItem as u8,
                offset: u64_le::new(0),
            },
        ) {
            Some(v) => v,
            None => return None,
        };
        let root_item = value.as_root_item().unwrap();

        Some(Tree::load(device, superblock, chunk_map, root_item.addr.get()).unwrap())
    }
    pub fn name_hash(name: &[u8]) -> u32 {
        crc::crc32::update(!0, &crc::crc32::CASTAGNOLI_TABLE, name)
    }
    pub fn lookup_dir_item<'a>(
        device: &mut D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        tree: Tree<'a>,
        oid: u64,
        name: &[u8],
    ) -> Option<Box<DirItem>> {
        let key = DiskKey {
            oid: u64_le::new(oid),
            ty: DiskKeyType::DirItem as u8,
            offset: u64_le::new(u64::from(dbg!(Self::name_hash(name)))),
        };
        let value = tree.get(device, superblock, chunk_map, &key)?;
        Some(value.into_dir_item()?.into_owned())
    }
    pub fn insert_handle(&self, handle: Handle) -> u64 {
        let num = self.next_handle.fetch_add(1, atomic::Ordering::Relaxed);
        let _ = self
            .handles
            .write()
            .unwrap()
            .insert(num, Mutex::new(handle));
        num
    }
    // XXX: Too bad fuse doesn't allow for inode+rdev lookups, instead of just the inode. This
    // doesn't play that well with submodules; namely, two root directories inside the same
    // subvolume with have identical objectids, with the sole difference being the rdev.
    //
    // TODO: Right now the FAL API doesn't account for what could be called "extended inode addresses".
    // However in the future there might as well be support for optional inode+rdev indexing, where the
    // frontends have to translate e.g. the FUSE inode numbers to the filesystem inode numbers
    // through a BTreeMap for example. For now we translate FAL InodeAddrs ourselves to the
    // inode+subvol.
    pub fn load_inode_inner(&mut self, fal_address: u64) -> fal::Result<(Inode, (u64, u64))> {
        let &(oid, subvolid) = self.inodes_translation.read().unwrap().get(&fal_address).ok_or(fal::Error::NoEntity)?;

        let disk_key = DiskKey {
            oid: u64_le::new(oid),
            ty: DiskKeyType::InodeItem as u8,
            offset: u64_le::new(0),
        };

        let subvolumes_read_guard = self.subvolumes.read().unwrap();
        let subvolume_read_guard = subvolumes_read_guard.get(&subvolid).ok_or(fal::Error::NoEntity)?.read().unwrap();

        let inner = subvolume_read_guard
            .as_ref()
            .get(&self.device, &self.superblock, &self.chunk_map, &disk_key)
            .ok_or(fal::Error::NoEntity)?
            .into_inode_item()
            .ok_or(fal::Error::NoEntity)?;

        Ok((Inode {
            inner,
            fal_address,
            real_address: oid,
            node_size: self.superblock.node_size.get(),
        }, (oid, subvolid)))
    }
    pub fn translation_for(&self, oid: u64, subvolid: u64) -> u64 {
        let mut translation_write_guard = self.inodes_translation_rev.write().unwrap();

        *translation_write_guard.entry((oid, subvolid)).or_insert_with(|| {
            self.next_inode.fetch_add(1, atomic::Ordering::Relaxed)
        })
    }
}

impl<D: fal::Device> fal::Filesystem<D> for Filesystem<D> {
    type InodeAddr = u64;
    type InodeStruct = Inode;
    type Options = ();

    fn root_inode(&self) -> Self::InodeAddr {
        // TODO: Stop pretending and fix the FAL API.
        2
    }
    fn mount(
        device: D,
        _general_options: fal::Options,
        _fs_specific_options: Self::Options,
        _path: &[u8],
    ) -> Self {
        Self::mount(device)
    }
    fn unmount(self) {}

    fn load_inode(&mut self, fal_address: Self::InodeAddr) -> fal::Result<Self::InodeStruct> {
        self.load_inode_inner(fal_address).map(|(s, _)| s)
    }
    fn store_inode(&mut self, inode: &Self::InodeStruct) -> fal::Result<()> {
        Err(fal::Error::ReadonlyFs)
    }
    fn open_file(&mut self, inode: Self::InodeAddr) -> fal::Result<u64> {
        todo!()
    }
    fn read(&mut self, fh: u64, offset: u64, buffer: &mut [u8]) -> fal::Result<usize> {
        todo!()
    }
    fn write(&mut self, fh: u64, offset: u64, buffer: &[u8]) -> fal::Result<u64> {
        todo!()
    }
    fn open_directory(&mut self, fal_address: Self::InodeAddr) -> fal::Result<u64> {
        let (inode, (oid, subvolid)) = self.load_inode_inner(fal_address)?;
        if inode.attrs().filetype != fal::FileType::Directory {
            return Err(fal::Error::NotDirectory);
        }
        Ok(self.insert_handle(Handle::Directory {
            fal_address,
            oid,
            subvolid,
            offset: 0,
        }))
    }
    fn read_directory(
        &mut self,
        fh: u64,
        offset: i64,
    ) -> fal::Result<Option<fal::DirectoryEntry<Self::InodeAddr>>> {
        let handles_read_guard = self.handles.read().unwrap();
        let mut handle = handles_read_guard.get(&fh).ok_or(fal::Error::BadFd)?.lock().unwrap();

        let &mut Handle::Directory { fal_address, oid, subvolid, ref mut offset } = match &mut *handle {
            handle @ &mut Handle::Directory { .. } => handle,
            _ => return Err(fal::Error::BadFd),
        };

        let subvolumes_read_guard = self.subvolumes.read().unwrap();
        let subvolume_read_guard = subvolumes_read_guard.get(&subvolid).ok_or(fal::Error::BadFdState)?.read().unwrap();
        let tree = subvolume_read_guard.as_ref();

        todo!()
    }
    fn lookup_direntry(
        &mut self,
        parent_fal_address: Self::InodeAddr,
        name: &[u8],
    ) -> fal::Result<fal::DirectoryEntry<Self::InodeAddr>> {
        let &(parent_oid, parent_subvolid) = self.inodes_translation.read().unwrap().get(&parent_fal_address).ok_or(fal::Error::NoEntity)?;

        let disk_key = DiskKey {
            oid: u64_le::new(parent_oid),
            ty: DiskKeyType::DirItem as u8,
            offset: u64_le::new(u64::from(Self::name_hash(name))),
        };
        let subvolumes_read_guard = self.subvolumes.read().unwrap();
        let subvolume_read_guard = subvolumes_read_guard.get(&parent_subvolid).ok_or(fal::Error::NoEntity)?.read().unwrap();
        let tree = subvolume_read_guard.as_ref();

        let value = tree
            .get(&self.device, &self.superblock, &self.chunk_map, &disk_key)
            .ok_or(fal::Error::NoEntity)?;

        let item = value.into_dir_item().ok_or(fal::Error::NoEntity)?;

        let fal_inode = if item.location.ty() == Some(DiskKeyType::DirItem) {
            self.translation_for(item.location.oid.get(), parent_subvolid)
        } else if item.location.ty() == Some(DiskKeyType::RootItem) {
            self.translation_for(256, item.location.oid.get())
        } else {
            return Err(fal::Error::NoEntity);
        };

        Ok(fal::DirectoryEntry {
            name: name.to_vec(),
            filetype: item.file_type().expect("todo: handle unknown filetype").try_into_fal().expect("todo: handle filetype unknown to FAL"),
            inode: fal_inode,
            offset: 0, // TODO
        })
    }
    fn readlink(&mut self, inode: Self::InodeAddr) -> fal::Result<Box<[u8]>> {
        todo!()
    }
    fn fh_inode(&self, fh: u64) -> Self::InodeStruct {
        todo!()
    }
    fn fh_offset(&self, fh: u64) -> u64 {
        todo!()
    }
    fn set_fh_offset(&mut self, fh: u64, offset: u64) {
        todo!()
    }
    fn filesystem_attrs(&self) -> fal::FsAttributes {
        todo!()
    }
    fn close(&mut self, file: u64) -> fal::Result<()> {
        todo!()
    }
    fn unlink(&mut self, parent: Self::InodeAddr, name: &[u8]) -> fal::Result<()> {
        todo!()
    }
    fn get_xattr(&mut self, inode: &Self::InodeStruct, name: &[u8]) -> fal::Result<Vec<u8>> {
        todo!()
    }
    fn list_xattrs(&mut self, inode: &Self::InodeStruct) -> fal::Result<Vec<Vec<u8>>> {
        todo!()
    }
}
#[derive(Clone)]
pub struct Inode {
    inner: InodeItem,
    real_address: u64,
    fal_address: u64,
    node_size: u32,
}
impl Filetype {
    pub fn try_into_fal(self) -> Option<fal::FileType> {
        Some(match self {
            Self::RegularFile => fal::FileType::RegularFile,
            Self::Directory => fal::FileType::Directory,
            Self::BlockDevice => fal::FileType::BlockDevice,
            Self::Symlink => fal::FileType::Symlink,
            Self::Fifo => fal::FileType::NamedPipe,
            Self::Socket => fal::FileType::Socket,
            Self::CharacterDevice => fal::FileType::CharacterDevice,

            Self::Unknown | Self::Xattr => return None,
        })
    }
}
impl From<Timespec> for fal::Timespec {
    fn from(tm: Timespec) -> Self {
        let mut sec = tm.sec.get();
        let nsec = tm.nsec.get();
        // TODO: Shouldn't fal use unsigned nanoseconds?
        let nsec = i32::try_from(nsec).unwrap_or_else(|_| {
            const NSECS_PER_SEC: u32 = 1_000_000_000;
            sec += i64::from(nsec / NSECS_PER_SEC);
            let new_nsec = nsec % NSECS_PER_SEC;
            i32::try_from(new_nsec).unwrap_or(i32::max_value())
        }); // TODO

        Self { sec, nsec }
    }
}

impl fal::Inode for Inode {
    type InodeAddr = u64;

    fn generation_number(&self) -> Option<u64> {
        Some(self.inner.generation.get())
    }
    fn addr(&self) -> Self::InodeAddr {
        self.fal_address
    }
    fn attrs(&self) -> fal::Attributes<Self::InodeAddr> {
        const S_IFMT: u32 = 0o170000;

        const S_IFSOCK: u32 = 0o140000;
        const S_IFLNK: u32 = 0o120000;
        const S_IFREG: u32 = 0o100000;
        const S_IFBLK: u32 = 0o060000;
        const S_IFDIR: u32 = 0o040000;
        const S_IFCHR: u32 = 0o020000;
        const S_IFIFO: u32 = 0o010000;

        fal::Attributes {
            filetype: match self.inner.mode.get() & S_IFMT {
                S_IFSOCK => fal::FileType::Socket,
                S_IFLNK => fal::FileType::Symlink,
                S_IFREG => fal::FileType::RegularFile,
                S_IFBLK => fal::FileType::BlockDevice,
                S_IFDIR => fal::FileType::Directory,
                S_IFCHR => fal::FileType::CharacterDevice,
                S_IFIFO => fal::FileType::NamedPipe,
                _ => todo!("handle invalid mode"),
            },
            access_time: self.inner.atime.into(),
            change_time: self.inner.ctime.into(),
            modification_time: self.inner.mtime.into(),
            creation_time: self.inner.otime.into(),

            block_count: self.inner.byte_count.get() / u64::from(self.node_size),
            flags: self.inner.flags.get().try_into().unwrap(),

            hardlink_count: self.inner.hardlink_count.get().into(),
            inode: self.real_address,

            permissions: (self.inner.mode.get() & 0o7777) as u16,
            rdev: self.inner.rdev.get(),
            size: self.inner.size.get(),

            user_id: self.inner.uid.get(),
            group_id: self.inner.gid.get(),
        }
    }
    fn set_gid(&mut self, gid: u32) {
        self.inner.gid.set(gid);
    }
    fn set_uid(&mut self, uid: u32) {
        self.inner.uid.set(uid);
    }
    fn set_perm(&mut self, permissions: u16) {
        let mut mode = self.inner.mode.get();
        mode &= 0o170000;
        assert_eq!(permissions & 0o7777, permissions);
        mode |= u32::from(permissions);
        self.inner.mode.set(mode);
    }
}
