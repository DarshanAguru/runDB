# runDB

> [!NOTE]
> **runDB** is a learning-focused project created to explore and understand Redis internals in simpler terms. It is intended for educational purposes and is not meant to be a production database replacement.

A lightweight, simplified implementation of a Redis-like in-memory Key-Value store. It demonstrates core concepts such as the Redis Serialization Protocol (RESP), asynchronous networking with `epoll`, and internal memory management strategies.

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
- **Asynchronous Server**: High-concurrency TCP server using Python's `select.epoll`.
- **AOF Snapshotting**: Manually triggerable point-in-time state dumps to an AOF file.
- **Background Forking**: Non-blocking AOF dumping using `multiprocessing` forking.
- **Pipelining**: Support for batching multiple commands in a single network request.
- **Command Set**: Supports core Redis commands like `PING`, `SET`, `GET`, `DEL`, `EXPIRE`, `TTL`, `INCR`, and `BGREWRITEAOF`.
- **Memory Optimized**: Uses `__slots__` and bit-packed metadata (4-bit type, 4-bit encoding) to store data efficiently.
- **Type Awareness**: Automatically deduces and stores object types (`STRING`) and encodings (`INT`, `EMBSTR`, `RAW`).
- **Key Expiration**: Passive and active expiration strategies to clean up stale data.
- **Eviction Policy**: Simple eviction mechanism to maintain a memory limit.

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
    - `eviction.py`: Implements a basic eviction policy.
    - `RedisCmd.py`: Data structure representing a parsed Redis command.
    - `FDComm.py`: Helper for non-blocking file descriptor communication.

- **`server/`**:
    - `Server.py`: Contains both synchronous and asynchronous TCP server implementations.

- **`config.py`**: Centralized configuration for server parameters.

## Getting Started

### Prerequisites

- Python 3.8+
- Linux (for `epoll` support)

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
127.0.0.1:7379> BGREWRITEAOF
OK
```

### Pipelining

**runDB** supports Redis pipelining. You can test this using `netcat`:

```bash
(printf '*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n';) | nc localhost 7379
```

## Configuration

Modify `config.py` to adjust system limits:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `HOST` | `0.0.0.0` | Binding address |
| `PORT` | `7379` | Listening port |
| `KEY_LIMIT` | `20,000` | Maximum number of keys allowed in the store |
| `MAX_CLIENTS` | `10,000` | Maximum number of concurrent client connections |
| `EVICTION_STRATEGY` | `simple-first` | Strategy for memory reclamation |

## Supported Commands

| Command | Description |
|---------|-------------|
| `PING [message]` | Returns PONG or the provided message. |
| `SET key value [EX seconds]` | Sets a key-value pair in memory. |
| `GET key` | Retrieves the value associated with a key. |
| `DEL key [key ...]` | Deletes one or more keys. |
| `EXPIRE key seconds` | Sets a timeout on a key in seconds. |
| `TTL key` | Returns the remaining time-to-live of a key. |
| `INCR key` | Increments the integer value of a key by one. |
| `BGREWRITEAOF` | Triggers a background process to dump current state to AOF file. |

## License

This repository is protected under [MIT](LICENSE) License.

## Author

**Darshan Aguru**
- 📧 Email: agurudf@gmail.com
- 🌐 Website: [thisdarshiii.in](https://thisdarshiii.in)
- 🐙 GitHub: [@DarshanAguru](https://github.com/DarshanAguru)

---

If you find this useful, give it a ⭐ on [GitHub](https://github.com/DarshanAguru/runDB)!