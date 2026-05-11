# runDB

> [!NOTE]
> **runDB** is a learning-focused project created to explore and understand Redis internals in simpler terms. It is intended for educational purposes and is not meant to be a production database replacement.

A lightweight, simplified implementation of a Redis-like in-memory Key-Value store. It demonstrates core concepts such as the Redis Serialization Protocol (RESP), asynchronous networking with `epoll`, and internal memory management strategies.

## Learning Objectives

This project was built to gain hands-on experience with:
- **RESP Protocol**: Implementing the logic to parse and generate Redis-compatible messages.
- **Asynchronous I/O**: Understanding how `epoll` enables a single-threaded server to handle thousands of concurrent clients.
- **Data Eviction & Expiration**: Learning how Redis manages memory and cleans up stale keys using active and passive strategies.
- **Concurrency**: Handling multiple connections in an asynchronous event loop environment.

## Features

- **RESP Support**: Fully compatible with the Redis Serialization Protocol.
- **Asynchronous Server**: High-concurrency TCP server using Python's `select.epoll`.
- **Command Set**: Supports core Redis commands like `PING`, `SET`, `GET`, `DEL`, `EXPIRE`, and `TTL`.
- **Key Expiration**: Passive and active expiration strategies to clean up stale data.
- **Eviction Policy**: Simple eviction mechanism to maintain a memory limit.
- **Configurable**: Easily adjust host, port, key limits, and maximum client connections.

## Architecture

The project is structured into modular components:

- **`core/`**:
    - `resp.py`: Implementation of RESP protocol for decoding requests and encoding responses.
    - `evaluator.py`: The command processor that handles the logic for each supported operation.
    - `store.py`: Thread-safe (using class methods) in-memory storage for key-value pairs and metadata.
    - `expiration.py`: Manages active expiration sampling to delete keys that have timed out.
    - `eviction.py`: Implements a basic eviction policy when the `KEY_LIMIT` is reached.
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

You can also specify the host and port via command-line arguments:

```bash
python3 main.py --host 127.0.0.1 --port 7379
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
127.0.0.1:7379> SET mykey "hello world"
OK
127.0.0.1:7379> GET mykey
"hello world"
127.0.0.1:7379> EXPIRE mykey 10
(integer) 1
127.0.0.1:7379> TTL mykey
(integer) 9
```

### Pipelining

**runDB** supports Redis pipelining, allowing you to send multiple commands in a single request. You can test this using `netcat` (or `nc`):

```bash
(printf '*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n';) | nc localhost 7379
```

This will send `PING`, `SET k v`, and `GET k` in one go and return the results for all three commands.

## Configuration

Modify `config.py` to adjust system limits:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `HOST` | `0.0.0.0` | Binding address |
| `PORT` | `7379` | Listening port |
| `KEY_LIMIT` | `20,000` | Maximum number of keys allowed in the store |
| `MAX_CLIENTS` | `10,000` | Maximum number of concurrent client connections |

## Supported Commands

| Command | Description |
|---------|-------------|
| `PING [message]` | Returns PONG or the provided message. |
| `SET key value [EX seconds]` | Sets a key-value pair with an optional expiration. |
| `GET key` | Retrieves the value associated with a key. |
| `DEL key [key ...]` | Deletes one or more keys. |
| `EXPIRE key seconds` | Sets a timeout on a key in seconds. |
| `TTL key` | Returns the remaining time-to-live of a key. |

## License

This repository is protected under [MIT](LICENSE) License.

## Author

**Darshan Aguru**
- 📧 Email: agurudf@gmail.com
- 🌐 Website: [thisdarshiii.in](https://thisdarshiii.in)
- 🐙 GitHub: [@DarshanAguru](https://github.com/DarshanAguru)

---

If you find this useful, give it a ⭐ on [GitHub](https://github.com/DarshanAguru/runDB)!