# NexusKV ⚡

**High-Performance In-Memory Key-Value Store & Cache Server with Redis Wire Protocol (RESP), AOF Durability, and LRU/LFU Eviction**

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-26%20passed-brightgreen.svg)]()
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-success.svg)]()

---

## 📌 Overview

**NexusKV** is a lightweight, zero-dependency in-memory key-value database and cache server implementing the standard Redis Serialization Protocol (RESP v2). Built with modern Python standard library modules, it provides an embedded or standalone drop-in caching solution for development, CI/CD testing, edge environments, and microservices without the overhead of Docker containers or C compilation.

---

## ✨ Features

- **⚡ Zero External Dependencies:** Pure Python 3.10+ standard library (`socket`, `threading`, `time`, `struct`, `os`, `unittest`).
- **🔌 Full Redis Protocol (RESP v2):** Compatible with standard Redis clients (`redis-cli`, `redis-py`, `ioredis`, Go `go-redis`).
- **🗃️ Rich Data Structures:**
  - **Strings:** `GET`, `SET`, `SETEX`, `SETNX`, `MGET`, `MSET`, `INCR`, `INCRBY`, `DECR`, `APPEND`, `STRLEN`.
  - **Lists:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`.
  - **Sets:** `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SDIFF`, `SINTER`, `SUNION`.
  - **Hashes:** `HSET`, `HGET`, `HDEL`, `HEXISTS`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HINCRBY`.
  - **Sorted Sets (ZSets):** `ZADD`, `ZRANGE`, `ZSCORE`, `ZCARD`, `ZREM`.
- **⏱️ Key Lifecycle & Expiration:** `EXPIRE`, `EXPIREAT`, `TTL`, `PERSIST`, `KEYS`, `EXISTS`, `DEL`.
- **🛡️ Memory Eviction Policies:**
  - `noeviction`
  - `allkeys-lru` (Least Recently Used across all keys)
  - `volatile-lru` (Least Recently Used among keys with TTL)
  - `allkeys-lfu` (Least Frequently Used)
  - `volatile-lfu`
  - `volatile-ttl` (Shortest time-to-live)
- **💾 Append-Only File (AOF) Durability:**
  - Mutation logging in standard RESP format.
  - Automatic restart replay and state reconstruction.
  - Background AOF compaction and rewriting (`BGREWRITEAOF`).
- **🔒 Atomic Transactions:** `MULTI`, `EXEC`, `DISCARD` block atomicity.
- **📡 Pub/Sub Messaging:** `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` real-time channel delivery.
- **🖥️ Tooling & Benchmarking:** Built-in interactive REPL (`nexus-cli`) and concurrent load tester (`nexus-benchmark`).

---

## 🏗️ Architecture

```
nexus-kv/
├── nexus_kv/
│   ├── __init__.py           # Package exports and versioning
│   ├── benchmark.py          # Concurrent load tester and latency percentile profiler
│   ├── cli.py                # Interactive REPL client and single-command CLI
│   ├── commands.py           # Command dispatcher (Strings, Lists, Sets, Hashes, ZSets)
│   ├── persistence.py        # Append-Only File (AOF) durability & log compaction
│   ├── pubsub.py             # Pub/Sub channel broker
│   ├── resp.py               # RESP v2 streaming parser and serializer
│   ├── server.py             # Multi-threaded TCP server and client session manager
│   ├── storage.py            # In-memory store, TTL expiration, and eviction policies
│   └── transactions.py       # MULTI/EXEC transaction state engine
├── tests/
│   ├── test_commands.py      # Data structure operation tests
│   ├── test_persistence.py   # AOF durability & compaction tests
│   ├── test_resp.py          # RESP parsing and serialization tests
│   ├── test_server_e2e.py    # Live TCP socket client-server tests
│   ├── test_storage.py       # TTL and LRU/LFU eviction tests
│   └── test_transactions.py  # Transaction atomicity tests
├── pyproject.toml            # Packaging configuration
└── README.md                 # System documentation
```

---

## 🚀 Quick Start

### 1. Start the Server

```bash
python -m nexus_kv.server -p 6380 --policy allkeys-lru
```

### 2. Connect with CLI

```bash
python -m nexus_kv.cli -p 6380
```

```text
127.0.0.1:6380> SET user:100 "Alice"
OK
127.0.0.1:6380> GET user:100
"Alice"
127.0.0.1:6380> HSET profile name "Alice" role "Architect"
(integer) 2
127.0.0.1:6380> HGETALL profile
1) "name"
2) "Alice"
3) "role"
4) "Architect"
```

Or use the standard `redis-cli`:

```bash
redis-cli -p 6380 ping
# PONG
```

### 3. Run Benchmark

```bash
python -m nexus_kv.benchmark -p 6380 -n 5000 -c 10
```

---

## 🧪 Testing

Run the automated test suite:

```bash
python -m unittest discover -s tests -v
```

```text
Ran 26 tests in 0.450s
OK
```

---

## 📄 License

Distributed under the MIT License. See `LICENSE` for details.
