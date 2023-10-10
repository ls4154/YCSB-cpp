# YCSB-cpp for Artifact Evaluation

## ❗️For AE Reviewers
Currently, the 8 nodes listed here will be reclaimed by Cloudlab on **Oct 4, 9am CDT** thus will soon be unaccessible. I'm reserving for new nodes and will inform you as soon as I get them prepared. You may use your own machine to [test the basic functionalities](https://github.com/dassl-uiuc/compute-side-log/tree/main#readme) for now. Sorry for the inconvenience.

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
           ┌────────────────▼────────────────┐
           │  cephfs cluster                 │
           │                                 │
           │  ┌───────┐ ┌───────┐ ┌───────┐  │
           │  │ rep 0 │ │ rep 1 │ │ rep 2 │  │
           │  └───────┘ └───────┘ └───────┘  │
           │                                 │
           └─────────────────────────────────┘
```

## Download
Download to app server machine
```bash
# On app server
git clone https://github.com/dassl-uiuc/YCSB-cpp.git --recurse-submodules
```
Then, for convenience, mount the folder to client machine via NFS
```bash
# On client
sudo mount.nfs4 ${server_ip}:/data/YCSB-cpp /data/YCSB-cpp
```

## Install Dependency Packages

### Regular dependencies
Install on client machine
```bash
sudo apt-get install -y libhiredis-dev cmake

git clone https://github.com/sewenew/redis-plus-plus.git redis++
cd redis++
mkdir build
cd build
cmake ..
make -j
sudo make install
cd ..
```

Install on app server machine
```bash
cd YCSB-cpp
./install-dep.sh ${DIR_TO_INSTALL}
```

### eRPC
On both client and app server, download and compile
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

## Run Experiments

### Node list (temporarily unavailable)
- ~~app server (node-0): `ssh luoxh@hp089.utah.cloudlab.us`~~
- memory replicas
  - ~~node-1: `ssh luoxh@hp009.utah.cloudlab.us`~~
  - ~~node-2: `ssh luoxh@hp019.utah.cloudlab.us`~~
  - ~~node-3: `ssh luoxh@hp100.utah.cloudlab.us`~~
- ~~client (node-4): `ssh luoxh@hp097.utah.cloudlab.us`~~
- Ceph nodes
  - ~~node-5: `ssh luoxh@hp082.utah.cloudlab.us`~~
  - ~~node-6: `ssh luoxh@hp007.utah.cloudlab.us`~~
  - ~~node-7: `ssh luoxh@hp93.utah.cloudlab.us`~~

### (C1) Write Microbenchmark (🟢 ready)
On the **app server node** (node-0), run
```bash
cd /data/YCSB-cpp
./scripts/run_raw_wr.sh
python3 ./scripts/draw/raw_wr.py /data/result
```
Generated figure will be at `/data/result/fig/write_lat.pdf` on **app server node**

### (C2) Insert-Only Workload (🟢 ready)
You may use one script to run all 3 applications on the **client node** (node-4)
```bash
```
Or you can run each application separately in the following steps:

#### RocksDB (Estimated runtime within 2.5h)
Build on **server node** (node-0)
```bash
cd /data/YCSB-cpp
rm ycsb rocksdb_svr
make BIND_ROCKSDBCLI=1 EXTRA_CXXFLAGS="-I/data/eRPC/src -I/data/eRPC/third_party/asio/include -I/data/rocksdb/include -L/data/eRPC/build -L/data/rocksdb" -j
```
Run on **client node** (node-4).
```bash
cd /data/YCSB-cpp
./scripts/run_rocksdb.sh cephfs load
./scripts/run_rocksdb.sh sync load
./scripts/run_rocksdb.sh sync_ncl load
# Each of the script runs for 50min
python3 ./scripts/draw/insert_only.py /data/result
```
Generated figure will be at `/data/result/fig/rocksdb_lattput.pdf` on **client node**

#### Redis (Estimated runtime within 1.5h)
Build on **server node** (node-0)
```bash
cd /data/YCSB-cpp
rm ycsb
make BIND_REDIS=1 -j
```
Run on **client node** (node-4)
```bash
cd /data/YCSB-cpp
./scripts/run_redis.sh cephfs load
./scripts/run_redis.sh sync load
./scripts/run_redis.sh sync_ncl load

python3 ./scripts/draw/insert_only.py /data/result
```
Generated figure will be at `/data/result/fig/redis_lattput.pdf` on **client node**

#### SQLite (Estimated runtime within 20min)
Build on **server node** (node-0)
```bash
make BIND_SQLITE=1 EXTRA_CXXFLAGS="-I/data/eRPC/src -I/data/eRPC/third_party/asio/include -I/data/sqlite/build -L/data/eRPC/build" -j
```
Run on **client node** (node-4)
```bash
cd /data/YCSB-cpp
./scripts/run_sqlite.sh cephfs load
./scripts/run_sqlite.sh sync load
./scripts/run_sqlite.sh sync_ncl load

python3 ./scripts/draw/insert_only.py /data/result
```
Generated figure will be at `/data/result/fig/sqlite_lattput.pdf` on **client node**
### (C3) YCSB Workload (🔴 not ready)

### (C4) Recovery Benchmark (🟡 partial ready)

#### Read Microbenchmark (🟢 ready)
On the **app server node** (node-0), run
```bash
cd /data/YCSB-cpp
./scripts/run_raw_rd.sh
python3 ./scripts/draw/raw_rd.py /data/result
```
Generated figure will be at `/data/result/fig/read_lat.pdf` on **app server node**

#### Application Recovery (🔴 not ready)
