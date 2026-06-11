# Changelog

All notable changes to **RunDB** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- **AOF Snapshotting & Forking**: Non-blocking database snapshot dumps using background process forking.


### Changed
- **Directory Restructuring**: Renamed the `utils/` directory to `testing_utils/` and updated documentation references.
- **Server Utility Modularization**: Reorganized the server architecture, placing `Printer.py` and `Shutdown.py` under the `server/util/` package and updating imports.
- **Architecture Diagram**: Generated and updated the visual architecture schema (`RunDB.png`) with clean, modernized nodes mapping all current components.
