# Changelog

All notable changes to **RunDB** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-06-13

### Added
- **C-Heap Open-Addressing HashMap**: Implemented a native, open-addressing Hash Map on the C heap utilizing FNV-1a hashing and tombstoning, completely eliminating Python `dict` overhead from core database storage.
- **Intset and HashTable Upgradeable Sets**: Implemented Redis-style Sets that start as contiguous integer-sorted arrays (`Intset`) and automatically upgrade to C-heap Hash Tables (`HashTable`) when non-integer values are inserted or size thresholds are exceeded.
- **Redis Set Commands Support**: Added command handlers and evaluator dispatching for `SADD`, `SISMEMBER`, `SCARD`, `SMEMBERS`, `SRANDMEMBER`, and `SREM`.
- **QuickList and ZipList Data Structures**: Implemented memory-efficient list types using doubly-linked structures (`QuickList`) of packed contiguous memory buffers (`ZipList`).
- **Redis List Commands Support**: Added command handlers and evaluator dispatching for `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LINDEX`, and `LRANGE`.
- **`DEBUG OBJECT` Command**: Added a diagnostic tool command `DEBUG OBJECT <key>` to retrieve object pointers, encoding types, serialized lengths, and LRU idle times.
- **Double-Free Safe Ownership Handoff**: Added pointer finalizer detaching via a new `release()` mechanism in `QuickList`, `Set`, and `RedisObject` to safely transfer structure ownership from Python to `RedisObject` without double-free errors.
- **Comprehensive Set & List Test Suites**: Added new automated unit tests in `tests/test_set.py` and `tests/test_list_commands.py` validating command lifecycles, encodings, and memory recycling.

## [1.0.0] - 2026-06-11

### Added
- **Pre-run Prerequisite Check**: Added a system compatibility verification in `main.py` checking for Linux OS, `select.epoll` support, and `ctypes` C-library binding before booting the server.
- **Eviction and Transaction Storm Utilities**: Added new test helper scripts `eviction_storm.py` and `transaction_storm.py` in `testing_utils/` to load-test key evictions and concurrent transactions.
- **Native Memory Management (`zmalloc`)**: Implemented a custom `ctypes` wrapper for C-level memory allocation, enabling real-time memory tracking and integration with `jemalloc` for optimized memory layout.
- **Approximated LRU Eviction**: Added Approximated Least Recently Used eviction strategy using a dynamic sorted eviction candidate pool, alongside `simple-first` and `allkeys-random` strategies.
- **Redis Transactions**: Full transaction support with `MULTI`, `EXEC`, and `DISCARD` isolating transaction states/queues per client.
- **Graceful Shutdown**: Traps OS termination signals to cleanly exit the event loop, serialize all in-memory keyspaces to AOF, and shut down gracefully.
- **Large Request Handling**: Added read/write buffers in `FDComm` to handle MTU chunking and partial/large RESP command streaming.
- **Active & Passive Expiration**: Dual-strategy expiration cleaning (passive on-access deletion + periodic active sampling cron).
- **AOF Snapshotting & Forking**: Non-blocking database snapshot dumps using background process forking, with full TTL/expiration persistence for active keys.


### Changed
- **Directory Restructuring**: Renamed the `utils/` directory to `testing_utils/` and updated documentation references.
- **Server Utility Modularization**: Reorganized the server architecture, placing `Printer.py` and `Shutdown.py` under the `server/util/` package and updating imports.
- **Architecture Diagram**: Generated and updated the visual architecture schema (`RunDB.png`) with clean, modernized nodes mapping all current components.
