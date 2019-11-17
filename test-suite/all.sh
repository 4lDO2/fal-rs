#!/bin/sh
mkdir prefix

if ! sh test-suite/test-fuse-readonly.sh ext2 prefix; then
    exit 1
fi
if ! sh test-suite/test-fuse-readonly.sh ext3 prefix; then
    exit 1
fi
if ! sh test-suite/test-fuse-readonly.sh ext4 prefix; then
    exit 1
fi
