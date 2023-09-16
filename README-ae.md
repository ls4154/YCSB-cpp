# YCSB-cpp for Artifact Evaluation

## Architecture
The evaluation cluster consists of 8 machines:

- 1 Client machine: run YCSB benchmark
- 1 App server machine: run database applications
- 3 Ceph machine: run CephFS
- 3 NCL peers: host replication of NCL-backed files

The CephFS is mounted to the app server. Client use eRPC or TCP (depend on application implementation) to send commands to app server. App server use RDMA to backup NCL-backed files to NCL peers.
```
                                           ┌────────────┐
                                     ┌─────► NCL Peer 0 │
                                     │     └────────────┘
┌──────────┐          ┌───────────┐  │
│          │          │           │  │     ┌────────────┐
│  client  ├─eRPC/TCP─► app server├─RDMA───► NCL Peer 1 │
│          │          │           │  │     └────────────┘
└──────────┘          └─────┬─────┘  │
                            │        │     ┌────────────┐
                            │        └─────► NCL Peer 2 │
                            │              └────────────┘
                            │
                            │
           ┌────────────────▼────────────────┐
           │  cephfs cluster                 │
           │                                 │
           │  ┌───────┐ ┌───────┐ ┌───────┐  │
           │  │ rep 0 │ │ rep 1 │ │ rep 2 │  │
           │  └───────┘ └───────┘ └───────┘  │
           │                                 │
           └─────────────────────────────────┘
```

## Install Dependency Packages

### Regular dependencies
Install on client machine
```bash
sudo apt-get install -y libhiredis-dev cmake

git clone https://github.com/sewenew/redis-plus-plus.git $dir/redis++
cd $dir/redis++
mkdir build
cd build
cmake ..
make -j
sudo make install
cd ..
```

Install on app server machine
```bash
sudo apt-get install -y \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev
```

### eRPC
Download and compile
```bash
git clone https://github.com/erpc-io/eRPC.git
cd eRPC
cmake . -DPERF=ON -DTRANSPORT=infiniband -DROCE=ON -DLOG_LEVEL=info
make -j
```
Configure huge pages
```bash
echo 1024 | sudo tee /proc/sys/vm/nr_hugepages
```
To automatic configure on boot, edit `/etc/sysctl.conf`
```
vm.nr_hugepages = 1024
```
See https://help.ubuntu.com/community/KVM%20-%20Using%20Hugepages for detail.

## Install Applications
Install on the App server machine

### Install RocksDB
```bash
git clone https://github.com/dassl-uiuc/rocksdb.git
cd rocksdb
git checkout csl-dev
make shared_lib -j
```
See https://github.com/dassl-uiuc/rocksdb/blob/main/INSTALL.md for detail.

### Install Redis
```bash
git clone https://github.com/dassl-uiuc/redis.git
cd redis
git checkout ncl-dev
make -j
```

### Install SQLite
```bash
git clone https://github.com/dassl-uiuc/sqlite.git
cd sqlite
git checkout ncl-dev
mkdir build
cd build
../configure
make -j
```
