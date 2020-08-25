use std::convert::TryFrom;
use std::io;

use thiserror::Error;

pub type Result<T = (), E = IoctlError> = ::core::result::Result<T, E>;

// actual types based on `block/ioctl.c` and `block/blk-zoned.c` in the Linux kernel source,
// and the numbers are defined in `include/linux/fs.h`.

mod raw {
    // NOTE: The term "sector" here __always__ refers to 512-byte sectors, i.e. the size
    // divided by 512. For real sector sizes, use `get_physical_blocksize`.

    nix::ioctl_write_ptr!(set_readonly, 0x12, 93, libc::c_int); // BLKROSET
    nix::ioctl_read!(get_readonly, 0x12, 94, libc::c_int); // BLKROGET
    nix::ioctl_none!(reread_partition_table, 0x12, 95); // BLKRRPART
    nix::ioctl_read!(get_sector_count, 0x12, 96, libc::c_ulong); // BLKGETSIZE

    // NOTE: BLKFLSBUF appears not to be called anywhere in the kernel, but is it still used?

    nix::ioctl_write_int!(set_sector_read_ahead, 0x12, 98, libc::c_ulong); // BLKRASET
    nix::ioctl_read!(get_sector_read_ahead, 0x12, 99, libc::c_long); // BLKRAGET
    nix::ioctl_write_int!(set_sector_fs_read_ahead, 0x12, 100, libc::c_ulong);// BLKFRASET
    nix::ioctl_read!(get_sector_fs_read_ahead, 0x12, 101, libc::c_long); // BLKFRAGET

    // NOTE: BLKSECTGET appears not to be implemented whatsoever; it is only ever used in a
    // switch statement in ioctl_compat, where it is a no-op.

    nix::ioctl_read!(get_sector_per_request, 0x12, 103, libc::c_ushort); // BLKSECTGET
    nix::ioctl_read!(get_logical_blocksize, 0x12, 104, libc::c_int); // BLKSSZGET

    nix::ioctl_read!(get_soft_blocksize, 0x12, 112, libc::c_int); // BLKBSZGET
    nix::ioctl_write_ptr!(set_soft_blocksize, 0x12, 113, libc::c_int); // BLKBSZSET
    nix::ioctl_read!(get_size64, 0x12, 114, u64); // BLKGETSIZE64

    // TODO: BLKTRACESETUP
    // TODO: BLKTRACESTART
    // TODO: BLKTRACESTOP
    // TODO: BLKTRACETEARDOWN

    nix::ioctl_write_ptr!(discard_sector_range, 0x12, 119, [u64; 2]); // BLKDISCARD
    nix::ioctl_read!(get_minimum_io_size, 0x12, 120, libc::c_uint); // BLKIOMIN
    nix::ioctl_read!(get_optimal_io_size, 0x12, 121, libc::c_uint); // BLKIOOPT
    nix::ioctl_read!(get_alignment_offset, 0x12, 122, libc::c_int); // BLKALIGNOFF
    nix::ioctl_read!(physical_blocksize, 0x12, 123, libc::c_uint); // BLKPBSZGET

    // NOTE: There is no documentation on what this does, and the source code simply returns a
    // zero.
    nix::ioctl_read!(_discard_zeroes, 0x12, 124, libc::uint); // BLKDISCARDZEROES

    nix::ioctl_write_ptr!(securely_discard_sector_range, 0x12, 125, [u64; 2]); // BLKSECDISCARD
    nix::ioctl_read!(is_rotational, 0x12, 126, libc::c_ushort); // BLKROTATIONAL
    nix::ioctl_write_ptr!(zeroout_sector_range, 0x12, 127, [u64; 2]); // BLKZEROOUT

    // TODO: BLKREPORTZONE
    // TODO: BLKRESETZONE
    // TODO: BLKGETZONESZ
    // TODO: BVKGETNRZONES
    // TODO: BLKOPENZONE
    // TODO: BLKCLOSEZONE
    // TODO: BLKFINISHZONE
}

pub unsafe fn set_readonly(fd: libc::c_int, readonly: bool) -> Result<()> {
    let data: libc::c_int = readonly.into();
    raw::set_readonly(fd, &data)
}
pub unsafe fn get_readonly(fd: libc::c_int) -> Result<bool> {
    let mut data: libc::c_int = -1; // begin with an initially invalid value
    raw::get_readonly(&mut data)?;
}
pub unsafe fn reread_partition_table(fd: libc::c_int) -> Result<()> {
    raw::reread_partition_table(fd)?;
    Ok(())
}
pub unsafe fn get_sector_count(fd: libc::c_int) -> Result<u64> {
    let mut count: libc::c_ulong = 0;
    raw::get_sector_count(fd, count)?;
    Ok(count)
}
pub unsafe fn set_sector_read_ahead(fd: libc::c_int, read_ahead: u64) -> Result<()> {
    raw::set_sector_read_ahead(fd, read_ahead)?;
    Ok(())
}
pub unsafe fn get_sector_read_ahead(fd: libc::c_int) -> Result<u64> {
    let mut read_ahead: libc::c_long = -1;
    raw::get_sector_read_ahead(fd, &mut read_ahead)?;
    Ok(read_ahead.try_into()?)
}
pub unsafe fn set_sector_fs_read_ahead(fd: libc::c_int, read_ahead: u64) -> Result<()> {
    raw::set_sector_fs_read_ahead(fd, read_ahead)?;
    Ok(())
}
pub unsafe fn get_sector_fs_read_ahead(fd: libc::c_int) -> Result<u64> {
    let mut read_ahead: libc::c_long = -1;
    raw::get_sector_fs_read_ahead(fd, &mut read_ahead)?;
    Ok(read_ahead.try_into()?)
}
pub unsafe fn get_sector_per_request(fd: libc::c_int) -> Result<u16> {
    let mut sector_per_req: libc::c_ushort = 0;
    raw::get_sector_per_request(fd, &mut sector_per_req)?;
    Ok(sector_per_req.try_into()?)
}

pub unsafe fn get_soft_blocksize(fd: libc::c_int) -> Result<u32> {
    let mut size: libc::c_int = -1;
    raw::get_soft_blocksize(fd, &mut size)?;
    Ok(u32::try_from(size)?)
}
pub unsafe fn set_soft_blocksize(fd: libc::c_int, size: u32) -> Result<()> {
    let blocksize: libc::c_int = size.try_into()?;
    raw::set_soft_blocksize(fd, &blocksize)?;
    Ok(())
}
pub unsafe fn get_logical_blocksize(fd: libc::c_int) -> Result<u32> {
    let mut size: libc::c_int = -1;
    raw::get_logical_blocksize(fd, &mut size)?;
    Ok(u32::try_from(size)?)
}
pub unsafe fn get_physical_blocksize(fd: libc::c_int) -> Result<u32> {
    let mut size: libc::c_int = -1;
    raw::get_physical_blocksize(fd, &mut size)?;
    Ok(u32::try_from(size)?)
}
pub unsafe fn get_size_in_bytes(fd: libc::c_int) -> Result<u64> {
    let mut size: u64 = 0;
    raw::get_size64(fd, &mut size)?;
    Ok(size)
}
pub unsafe fn get_minimum_io_size(fd: libc::c_int) -> Result<u32> {
    let mut size: libc::c_uint = 0;
    raw::get_minimum_io_size(fd, &mut size)?;
    Ok(size.try_into()?)
}
pub unsafe fn get_optimal_io_size(fd: libc::c_int) -> Result<u32> {
    let mut size: libc::c_uint = 0;
    raw::get_optimal_io_size(fd, &mut size)?;
    Ok(size.try_into()?)
}
pub unsafe fn get_alignment_offset(fd: libc::c_int) -> Result<u32> {
    let mut size: libc::c_int = 0;
    raw::get_alignment_offset(fd, &mut size)?;
    Ok(size.try_into()?)
}
pub unsafe fn is_rotational(fd: libc::c_int) -> Result<bool> {
    let mut value: libc::c_ushort = 2; // pick an initially invalid value
    raw::is_rotational(fd, &mut value)?;
    Ok(bool::try_from(value)?)
}

pub unsafe fn discard_sector_range(
    fd: libc::c_int,
    base: u64,
    count: u64,
    securely: bool,
) -> Result<(), IoctlError> {
    let array: [u64; 2] = [base, count];
    if securely {
        raw::securely_discard_sector_range(fd, &array)?;
    } else {
        raw::discard_sector_range(fd, &array)?;
    }
    Ok(())
}
pub unsafe fn zeroout_sector_range(fd: libc::c_int, base: u64, count: u64) -> Result<(), IoctlError> {
    let array = [u64; 2] = [base, count];
    raw::zeroout_sector_range(&array)?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum IoctlError {
    #[error("disk i/o error: {0}")]
    IoError(#[from] nix::Error),

    #[error("integer conversion error")]
    Overflow(#[from] core::num::TryFromIntError),
}
