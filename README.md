# Courier

## Prerequisites

### Environment

This project is developed on CentOS 8.5.2111. 
It should work on other Linux distributions as well.

This project requires a C++ compiler with C++20 support. 
It is known to work with GCC 13.1.0 and GCC 12.2.0.

### Library

The following libraries are required necessarily. 
- numa
- jemalloc
- [Intel OneAPI-OneTBB](https://github.com/oneapi-src/oneTBB)
- [spdlog](https://github.com/gabime/spdlog)
- [magic_enum](https://github.com/Neargye/magic_enum)

There are some unnecessary libraries. You have to remove them from CMakeLists.txt and source code if you don't have.
- [papi](https://icl.utk.edu/papi)
- [gperftools](https://github.com/gperftools/gperftools.git)

It's suggested to use [vcpkg](https://github.com/microsoft/vcpkg) to build up these libraries(unless papi):
```shell
cd <path-of-vcpkg>
git pull
./vcpkg install tbb spdlog magic_enum gperftools
```

## Build and Run

### Preparation
Before building process, you should execute a python script to detect
the information about architecture.
```shell
python3 arch_dect.py --pmem_dir=[path]
```
Where `[path]` denote the directory on PMEM for data storage.

Besides, you need to edit the file `src/main.cpp` to determine the configuration of 
components.

### Normal execution
Then enter the build directory to launch cmake and unix makefile
```shell
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j
./PTM
```

### Automatic batching execution
For automatic building and testing, you need to edit the file `auto_test.py` 
and execute it.
```shell
python3 auto_test.py
```

## Detail

- Design: [Design Principle](doc/DesignPattern.md)
- Workload: [Workload README](workload/README.md)

### Module Organization

There are four modules in this code:

- util
- concurrent_control
- storage_manager
- workload

The `util` module is independent, while the other are ONLY based on module `util`. 
This means you can move one of them to other programs only with util module.

Besides, the directories `include` and `src` are just coordinators among modules above in this project.

`graph` contains some test result and corresponding scripts for graph plotting.

Note that the directory `concurrentqueue` is an outer library.

### Component Configuration

- `WORKLOAD_DEFINED` type of workload
  - **TPCC**
  - **YCSB_A**
  - **YCSB_B**
  - **YCSB_C**
  - **YCSB_D**
  - **YCSB_E**
  - **YCSB_F**
- `INDEX_DEFINED` type of index
  - **HashMap**
  - **BPTree**
- `CONCURRENT_CONTROL_DEFINED` type of concurrent control
  - **OCC**
  - **TPC**
  - **TICTOC**
  - **MVCC**
  - **SP**
  - **COURIER**
  - **COURIER_SAVE**
- `TRANSACTION_MANAGER_DEFINED` type of transaction manager
- `STORAGE_MANAGER_DEFINED` type of storage manager

### NVM Configuration

This option comes from an outdated idea, which I'm too lazy to maintain...

- `PWB` type of instruction for flushing
  - **CLWB**
  - **CLFLUSH**
  - **CLFLUSHOPT**


## Known Issues

- Under debug mode, `BPTree`, which I implement by FAST_FAIR, may have confusing behaviors with multi-thread initialization.
- **COURIER_SAVE** and **COURIER** may fail to pass the DEBUG_ASSERT of TPCC, because a worker thread may get a outdated pointer which is to be reclaimed by delay write module. However, this can be figured out by timestamp check and get aborted in validation phase.
- When using **BPTree**, Courier may fail inserting elements or have confusing high abort rate. You can retry again or use HashMap instead.

## Reference

- CRWWP SpinLock 
- [FAST_FAIR](https://github.com/DICL/FAST_FAIR) A concurrent B+ tree
- [moodycamel::ConcurrentQueue](https://github.com/cameron314/concurrentqueue) A scalable concurrent queue