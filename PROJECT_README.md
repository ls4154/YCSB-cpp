# Exposing the “Bleeding-Edge” of Modern Embedded Key-Value Databases

- **CS 6400 DB Project (Group 8)**
- _Members: Richard So, Nandha Sundaravadivel, Drumil Deliwala, Saatvik Agrawal_

## Results

## Setup / Instructions

### Prerequisites

- AWS EC2 Instance, `t2.micro` (although any instance should work)

  - Amazon Linux 2 AMI (or any Linux distro)

- Install the following:

  - `cmake`, `make`, `gcc`, `g++`
  - Install `zlib-devel` or equivalent for compiling `YCSB-cpp`
  - `java` (8+) and `maven` (>= 3); using `sdkman` is recommended
  - `python3` for running `YCSB`

### Running YCSB (Java) benchmarks

> **Implemented bindings: `xodus`, `mapdb`, `halodb`**

Below is an example of running the `mapdb` binding:

```sh
cd YCSB
./bin/ycsb.sh load mapdb -s -P workloads/workloada
./bin/ycsb.sh run mapdb -s -P workloads/workloada
```

Rinse and repeat for the other bindings.

### Running YCSB-cpp (C++) benchmarks

> **Impmeneted bindings: `unqlite`, `terarkdb`**

Below is an example of compiling and running the `unqlite` binding:

```sh
cd YCSB-cpp
mkdir build && cd build
cmake -DBIND_UNQLITE=1 ..
make clean && make
./ycsb -load -db unqlite -P ../workloads/workloada -s
./ycsb -run -db unqlite -P ../workloads/workloada -s
```

There are additional instructions in the `YCSB-cpp` directory for `terarkdb`,
`leveldb`, and `rocksdb`.

**It is recommended to only bind ONE database at a time when compiling w/
cmake to avoid conflicts.**
