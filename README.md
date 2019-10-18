# fal-rs, a filesystem abstraction layer

Allows backends (like ext2, apfs, btrfs, xfs etc.) to communicate with the frontends (currently including only FUSE and Redox), by implementing the `Filesystem` and `FilesystemMut` traits.

# Features implemented
## Backends
- ext2: Read-only (+writing metadata)
- apfs: WIP (parsing the fundamental container structures is currently being implemented)
- btrfs: WIP (status similar to that of APFS)
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

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
