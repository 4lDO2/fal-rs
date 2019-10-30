#!/bin/sh
mkdir prefix

if ! sh ./test-fuse-readonly.sh ext2 prefix; then
    exit 1
fi
