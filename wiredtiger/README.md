# WiredTiger

The code in `wiredtiger_db.cc` assumes your libwiredtiger to have built-in `<compressor>` that you specify in the
`wiredtiger.properties`.

For example, if you specify `wiredtiger.blk_mgr.compressor=snappy` in `wiredtiger.properties`, your libwiredtiger
should be built with `DHAVE_BUILTIN_EXTENSION_SNAPPY=1`.

## Install WiredTiger library on POSIX

Download and extract the source:
```
wget https://github.com/wiredtiger/wiredtiger/archive/refs/tags/11.1.0.tar.gz
tar xvzf 11.1.0.tar.gz
mkdir -p wiredtiger-11.1.0/build
cd wiredtiger-11.1.0/build
```

Enable built-in compressors and build
```
cmake -DHAVE_BUILTIN_EXTENSION_LZ4=1 -DHAVE_BUILTIN_EXTENSION_SNAPPY=1 -DHAVE_BUILTIN_EXTENSION_ZLIB=1 -DHAVE_BUILTIN_EXTENSION_ZSTD=1 -DENABLE_STATIC=1 ../.
make -j$(nproc)
sudo make install
```

## Install WiredTiger library on Windows with vcpkg

Build wiredtiger on Windows with built-in compressors is a little more complicated, we need to modify some source code of wiredtiger.

So I made a vcpkg port of wiredtiger, which simplifies the process.

1. install cmake/vcpkg on Windows
2. refer to the step 1-2 of [wiredtiger-vcpkg](https://github.com/kabu1204/wiredtiger-vcpkg) to install wiredtiger.lib.