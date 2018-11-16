#!/bin/bash

make clean
make
dd if=/dev/zero of=disk.img bs=1M count=1024
mkdir mnt
./objfs mnt -o use_ino
