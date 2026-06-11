# Changelog

All notable changes to **runDB** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-06-11

This is the initial stable release of `runDB`, featuring high-concurrency event-loop networking, native memory management, and Redis protocol compatibility.

### Added
- **Native Memory Management (`zmalloc`)**: Implemented a custom `ctypes` wrapper for C-level memory allocation, enabling real-time memory tracking and integration with `jemalloc` for optimized memory layout.
- **Approximated LRU Eviction**: Added Approximated Least Recently Used eviction strategy using a dynamic sorted eviction candidate pool, alongside `simple-first` and `allkeys-random` strategies.
- **Redis Transactions**: Full transaction support with `MULTI`, `EXEC`, and `DISCARD` isolating transaction states/queues per client.
- **Graceful Shutdown**: Traps OS termination signals to cleanly exit the event loop, serialize all in-memory keyspaces to AOF, and shut down gracefully.
- **Large Request Handling**: Added read/write buffers in `FDComm` to handle MTU chunking and partial/large RESP command streaming.
- **Active & Passive Expiration**: Dual-strategy expiration cleaning (passive on-access deletion + periodic active sampling cron).
- **AOF Snapshotting & Forking**: Non-blocking database snapshot dumps using background process forking.
