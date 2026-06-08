# runDB

> [!NOTE]
> **runDB** is a learning-focused project created to explore and understand Redis internals in simpler terms. It is intended for educational purposes and is not meant to be a production database replacement.

> [!IMPORTANT]
> **Linux-Only**: Since the high-concurrency asynchronous network engine is built using the Linux-specific `select.epoll` API, `runDB` is compatible with **Linux environments only**.

A lightweight, simplified implementation of a Redis-like in-memory Key-Value store. It demonstrates core concepts such as the Redis Serialization Protocol (RESP), high-concurrency asynchronous networking with `epoll`, and internal memory-management strategies.

## Learning Objectives

This project was built to gain hands-on experience with:

- **RESP Protocol**: Implementing the logic to parse and generate Redis-compatible messages.
- **Asynchronous I/O**: Understanding how `epoll` enables a single-threaded server to handle thousands of concurrent clients.
- **Snapshotting**: Implementing point-in-time data dumps using Append Only Files (AOF).
- **Background Maintenance**: Learning how to use process forking for non-blocking maintenance tasks like AOF rewriting/dumping.
- **Data Eviction & Expiration**: Learning how Redis manages memory and cleans up stale keys using active and passive strategies.
- **Memory Optimization**: Using Python `__slots__` and bit-packing (packing type/encoding into a single byte) to minimize memory overhead per object.
- **Concurrency**: Handling multiple connections in an asynchronous event loop environment.

## Features

- **RESP Support**: Fully compatible with the Redis Serialization Protocol.
- **Asynchronous Server**: High-concurrency TCP server utilizing Linux's `select.epoll` in a non-blocking single-threaded event loop.
- **AOF Snapshotting**: Manually triggerable point-in-time state dumps to an AOF file.
- **Background Forking**: Non-blocking AOF dumping using `multiprocessing` forking.
- **Pipelining**: Support for batching multiple commands in a single network request.
- **Command Set**: Supports core Redis commands like `PING`, `SET`, `GET`, `DEL`, `EXPIRE`, `TTL`, `INCR`, `INFO`, `CLIENT`, `LATENCY`, and `BGREWRITEAOF`.
- **Memory Optimized**: Uses `__slots__` and bit-packed metadata (4-bit type, 4-bit encoding) to store data efficiently.
- **Type Awareness**: Automatically deduces and stores object types (`STRING`) and encodings (`INT`, `EMBSTR`, `RAW`).
- **Key Expiration**: Passive (lazy) deletion on access and active expiration strategies (random sampling) to clean up stale data.
- **Graceful Shutdown**: Traps OS termination signals, coordinates with active request executors atomically, and triggers a final AOF persistence dump before exiting cleanly.
- **Redis Transactions**: Full transaction support with `MULTI`, `EXEC`, and `DISCARD` executing queued commands isolated per client connection.
- **Eviction Policies**: Memory reclamation strategies to maintain a hard keys limit:
  - `simple-first`: Evicts a single random key when the memory limit is reached.
  - `allkeys-random`: Evicts a configurable ratio (e.g., 20%) of random keys in the database using high-performance sample selection.
  - `allkeys-lru`: Approximated Least Recently Used eviction strategy utilizing a globally managed, sorted eviction pool similar to Redis.

## Core Optimizations & Architecture

### 1. Memory Optimization & Bit-Packing

To minimize the memory footprint of storing millions of keys in-memory, `runDB` employs custom optimization techniques:

- **`__slots__` in `RedisObject`**: By defining `__slots__ = ["val", "expire_at", "typeEncoding", "lat"]` inside our core object wrapper, we disable the automatic creation of dynamic instance dictionaries (`__dict__`) and weak references (`__weakref__`). This reduces memory consumption by **~60%** per object.
- **Bit-Packed Type/Encoding**: Instead of storing the object type and encoding as separate integer attributes (which consume 28 bytes each in Python), they are bit-packed into a single 8-bit integer field (`typeEncoding`).
  - High 4 bits: Redis Object Type (e.g., `TYPE_STRING = 0`)
  - Low 4 bits: Redis Object Encoding (e.g., `RAW = 0`, `INT = 1`, `EMBSTR = 8`)
  - Bit-packing formula: `((type & 0x0F) << 4) | (encoding & 0x0F)`

### 2. High-Performance Eviction Strategy

- **Sampling over Shuffling**: Dict key retrieval in Python preserves insertion order. To select a random key for eviction, a naive approach would shuffle the entire key list using `random.shuffle()`, which incurs a highly inefficient $O(N)$ operation for shuffling all keys.
- `runDB` solves this with ultra-fast sampling methods:
  - For `simple-first`, it uses `random.choice(list(store.keys()))` to instantly locate and evict a single random key.
  - For `allkeys-random`, it uses `random.sample(list(store.keys()), evict_keys_count)` to retrieve a specific sub-sample of keys to evict in a single pass, avoiding the CPU bottleneck of shuffling the entire keyspace.
  - For `allkeys-lru`, it employs an **Approximated LRU Eviction Pool** of size 16 (or custom size) sorted by key idle times ascending. It samples keys dynamically, inserts them into the sorted pool by comparing with the worst candidate (index 0), and evicts from the pool's end (maximum idle time).

### 3. Active Expiration Loop

- While expired keys are lazily deleted on access (passive deletion), `runDB` also runs an **Active Expiration cron job** every 1 second inside the event loop.
- It samples up to 20 keys containing set expirations. If any of those 20 keys are expired, they are immediately deleted.
- If more than 25% (i.e. > 5 keys) of the sampled set are found to be expired, `runDB` loops again to actively clean up expired keys. This active loop continues until the fraction of expired keys drops below 25%, protecting memory without blocking client sockets.

### 4. Non-Blocking Background Operations

- Operations like `BGREWRITEAOF` are CPU and I/O intensive. If run on the main event loop, they would block thousands of connected clients.
- `runDB` offloads this by spawning a child process using Python's `multiprocessing.Process`. By leveraging the OS-level `fork()` capability, the child process operates on a **Copy-On-Write (COW)** snapshot of the memory pages. This allows the server to continue handling incoming client queries concurrently with zero locks.

### 5. Asynchronous Graceful Shutdown & Atomic State Coordination

To prevent data loss and ensure system stability upon termination:
- **Signal Trapping**: `runDB` intercepts OS process termination signals (`SIGTERM`, `SIGINT`, etc.) and schedules a shutdown via an asynchronous signal monitor task (`waitForSignal`).
- **Atomic Engine Status (`AtomicInt`)**: State is tracked using three atomic statuses: `ENGINE_IDLE = 0`, `ENGINE_BUSY = 1`, and `ENGINE_SHUTDOWN = 2`.
  - While processing events or cron key expirations, the server transitions the state from `ENGINE_IDLE` to `ENGINE_BUSY` using Compare-And-Swap (CAS).
  - When a shutdown signal is caught, the signal monitor waits for the engine to leave `ENGINE_BUSY` (ensuring in-flight commands and background maintenance complete cleanly).
  - Once idle, the status transitions to `ENGINE_SHUTDOWN` atomically, preventing the event loop from accepting new work or requests.
- **Persistence Dump**: The monitor invokes a final `AOF.dumpAllAOF()` persistence dump to dump the in-memory keyspace before exiting.
- **Clean Coroutine Exit**: The orchestrator in `main.py` awaits the shutdown monitor, cleanly cancels the running TCP server task, and exits the process with code 0 without traceback noise.

### 6. Object-Oriented Client Model & Asynchronous Transaction Isolation

- **Encapsulated Client State**: Active connections are tracked cleanly in `Server.con_clients` via modern `Client` objects, removing manual connection/socket mappings.
- **Isolated Transaction States**: Transaction queues (`cqueue`) and states (`isTrans`) are bound directly to their corresponding `Client` instance, ensuring concurrent client transactions are fully isolated and executed in RESP array batch format.

---

## Architecture

The project is structured into modular components:

- **`core/`**:

  - `resp.py`: Implementation of RESP protocol for decoding requests.
  - `encoding.py`: RESP encoding logic and type/encoding deduction.
  - `evaluator.py`: The command processor that handles operation logic.
  - `redisObject.py`: Memory-optimized object representation using `__slots__` and bit-packing.
  - `assertions.py`: Shared validation logic for Redis object types and encodings.
  - `aof.py`: Manages point-in-time state dumps using background child processes.
  - `store.py`: In-memory storage for key-value pairs and metadata.
  - `expiration.py`: Manages active expiration sampling.
  - `eviction.py`: Implements memory-reclamation strategies (`simple-first`, `allkeys-random`, `allkeys-lru`).
  - `stats.py`: Tracks and manages keyspace statistics across multiple Redis databases.
  - `RedisCmd.py`: Data structure representing a parsed Redis command.
  - `FDComm.py`: Helper for non-blocking file descriptor communication.
  - `Client.py`: Encapsulates client socket references, connection states, and transaction queues per client.
- **`server/`**:

  - `Server.py`: Contains a high-concurrency, asynchronous TCP server utilizing Linux `select.epoll`.
- **`config.py`**: Centralized configuration for server parameters.

## Getting Started

### Prerequisites

- Python 3.8+
- **Linux OS**: Required because the event-loop relies on the Linux-specific `select.epoll` system call.

### Running the Server

To start the runDB server with default settings:

```bash
python3 main.py
```

### Testing the Server

Since **runDB** is compatible with the RESP protocol, you can use the standard `redis-cli` to interact with it:

```bash
redis-cli -p 7379
```

Once connected, you can try various commands:

```text
127.0.0.1:7379> PING
PONG
127.0.0.1:7379> SET mykey 100
OK
127.0.0.1:7379> INCR mykey
(integer) 101
127.0.0.1:7379> INFO
# Keyspace
db0:keys=1,expires=0,avg_ttl=0
db1:keys=0,expires=0,avg_ttl=0
db2:keys=0,expires=0,avg_ttl=0
db3:keys=0,expires=0,avg_ttl=0
127.0.0.1:7379> BGREWRITEAOF
OK
```

### Pipelining

**runDB** supports Redis pipelining. You can test this using `netcat`:

```bash
(printf '*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n';) | nc localhost 7379
```

---

## Configuration

Modify `config.py` to adjust system limits:

| Parameter              | Default          | Description                                                            |
| ---------------------- | ---------------- | ---------------------------------------------------------------------- |
| `HOST`                 | `0.0.0.0`        | Binding address                                                        |
| `PORT`                 | `7379`           | Listening port                                                         |
| `KEY_LIMIT`            | `100`            | Maximum number of keys allowed in the store                            |
| `MAX_CLIENTS`          | `10,000`         | Maximum number of concurrent client connections                        |
| `AOF_FILE`             | `run-master.aof` | Filename used for AOF persistence dumping                              |
| `EVICTION_STRATEGY`    | `allkeys-lru`    | Strategy for memory reclamation (`simple-first`, `allkeys-random`, `allkeys-lru`) |
| `EVICTION_RATIO`       | `0.2`            | Fraction of keys evicted during eviction                               |
| `EVICTION_POOL_SIZE`   | `16`             | Candidate pool size for `allkeys-lru` eviction strategy                |
| `EVICTION_SAMPLE_SIZE` | `5`              | Number of keys sampled on each pass to populate eviction pool          |
| `DB_COUNT`             | `4`              | Number of databases configured in the server                           |

---

## Supported Commands

| Command                        | Description                                                               |
| ------------------------------ | ------------------------------------------------------------------------- |
| `PING [message]`             | Returns PONG or the provided message.                                     |
| `SET key value [EX seconds]` | Sets a key-value pair in memory, with optional expiration in seconds.     |
| `GET key`                    | Retrieves the value associated with a key (with passive/lazy expiration). |
| `DEL key [key ...]`          | Deletes one or more keys.                                                 |
| `EXPIRE key seconds`         | Sets a timeout on a key in seconds.                                       |
| `TTL key`                    | Returns the remaining time-to-live of a key in seconds.                   |
| `INCR key`                   | Increments the integer value of a key by one.                             |
| `INFO`                       | Returns keyspace metrics (number of active keys in each DB).              |
| `CLIENT [args...]`           | Client connection command placeholder.                                    |
| `LATENCY [args...]`          | Latency monitoring command placeholder.                                   |
| `BGREWRITEAOF`               | Triggers a background process (forked child) to dump state to AOF file.   |
| `MULTI`                      | Marks the start of a transaction block.                                   |
| `EXEC`                       | Executes all queued commands in a transaction block.                      |
| `DISCARD`                    | Flushes all queued commands inside a transaction block.                   |

## License

This repository is protected under [MIT](LICENSE) License.

## Author

**Darshan Aguru**

- 📧 Email: agurudf@gmail.com
- 🌐 Website: [thisdarshiii.in](https://thisdarshiii.in)
- 🐙 GitHub: [@DarshanAguru](https://github.com/DarshanAguru)

---

If you find this useful, give it a ⭐ on [GitHub](https://github.com/DarshanAguru/runDB)!
