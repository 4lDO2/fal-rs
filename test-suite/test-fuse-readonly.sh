#!/bin/sh

fs_type="$1"
prefix="$2"

DEFAULT_DISK_SIZE=512M # not sure about which size is the best
DEFAULT_RANDOM1_SIZE=32M
DEFAULT_RANDOM2_SIZE=128M

disk="$prefix/disk_$fs_type.img"
mountpoint="$prefix/disk_${fs_type}_mountpoint"
dir_struct_file="$prefix/disk_${fs_type}_struct"
hashes_file="$prefix/dish_${fs_type}_hashes"

if [ -z "$fs_type" ] || [ -z "$prefix" ]; then
    echo "Usage: test-fuse-readonly.sh FS_TYPE PREFIX"
    exit 1
fi

disk_size="$DEFAULT_DISK_SIZE"
random1_size="$DEFAULT_RANDOM1_SIZE"
random2_size="$DEFAULT_RANDOM2_SIZE"

if ! [ -x "$mountpoint" ]; then
    mkdir "$mountpoint"
fi

if ! [ -e "$disk" ]; then
    dd if=/dev/zero of="$disk" bs="$disk_size" count=1

    case "$fs_type" in
        "apfs")
            echo "Unimlemented"
            exit 1
            ;;
        "ext2")
            mkfs.ext2 "$disk"
            mount "$disk" "$mountpoint"
            chown -R `whoami` "$mountpoint"
            ;;
    esac

    # Just create a basic structure, and write random bytes to some files.
    mkdir "$mountpoint/a"
    mkdir "$mountpoint/b"
    mkdir "$mountpoint/filenames_can_be_very_long_as_well_and_possibly_longer_than_a_few_bytes"
    mkdir "$mountpoint/c"
    mkdir "$mountpoint/d"

    mkdir "$mountpoint/a/b"
    mkdir "$mountpoint/a/b/c"
    mkdir "$mountpoint/a/b/c/d"
    mkdir "$mountpoint/a/b/c/d/e"
    mkdir "$mountpoint/a/b/c/d/e/f"
    mkdir "$mountpoint/a/b/c/d/e/f/g"

    dd if="/dev/urandom" of="$mountpoint/a/random.bin" bs="$random1_size" count=1
    dd if="/dev/urandom" of="$mountpoint/random2.bin" bs="$random2_size" count=1
    echo "Hello, world!" >> "$mountpoint/random2.bin" # make sure that the second file gets some block size unalignment.
else
    mount "$disk" "$mountpoint"
    chown -R `whoami` "$mountpoint"
fi

find "$mountpoint" | sort > "$dir_struct_file"
cp "$dir_struct_file" "/tmp/x.txt"

sha1sum "$dir_struct_file" "$mountpoint/a/random.bin" "$mountpoint/random2.bin" > "$hashes_file"

umount "$mountpoint"

cargo run --bin fal-frontend-fuse -- --fs-type="$fs_type" mount "$disk" "$mountpoint" &
sleep 1

find "$mountpoint" | sort > "$dir_struct_file"
cp "$dir_struct_file" "/tmp/y.txt"
sha1sum -c "$hashes_file"

umount "$mountpoint"
wait
exit $?
