# fal-rs, a filesystem abstraction layer

Allows backends (like ext2, apfs, btrfs, xfs etc.) to communicate with the frontends (currently including only FUSE and Redox), by implementing the `Filesystem` and `FilesystemMut` traits.

# Features implemented
## Backends
- ext2: Read-only (+writing metadata)
- apfs: WIP (parsing the fundamental container structures is currently being implemented)
- btrfs: TODO
- xfs: TODO
- f2fs: TODO

## Frontends
- FUSE: Read-only access to files and directories, including metadata.
- Redox: Same as FUSE.
- Linux kernel modules: TODO
- macOS kexts: TODO

# TODO
- [ ] Create/use a proper parsing library (maybe use `nom`?)
- [ ] Optimizations
- [ ] Testing, which requires either storing binary fs images in the repo, or depending on the system's mkfs.
