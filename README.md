# JCache — High-Performance In-Memory Cache Server

A production-grade, distributed in-memory cache library and server built in Java. JCache provides multiple eviction policies, full concurrency support, TTL-based expiry, persistence, consistent-hash clustering, and a TCP server accessible from any language.

[![Build Status](https://github.com/himanshmunjal/JCache/actions/workflows/ci.yml/badge.svg)](https://github.com/himanshmunjal/JCache/actions)
[![Java](https://img.shields.io/badge/Java-17-blue)](https://openjdk.org/projects/jdk/17/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-himanshmunjal%2Fjcache-blue)](https://hub.docker.com/r/himanshmunjal/jcache)

---

## Table of Contents

- [What Is JCache](#what-is-jcache)
- [Architecture Overview](#architecture-overview)
- [Module Structure](#module-structure)
- [Quick Start](#quick-start)
- [Docker Usage](#docker-usage)
- [Eviction Policies](#eviction-policies)
- [Concurrency Strategies](#concurrency-strategies)
- [TTL Support](#ttl-support)
- [Persistence](#persistence)
- [Cache Server](#cache-server)
- [Wire Protocol](#wire-protocol)
- [Consistent Hashing and Clustering](#consistent-hashing-and-clustering)
- [Java Client](#java-client)
- [Go Client Example](#go-client-example)
- [CLI Client](#cli-client)
- [Metrics and Monitoring](#metrics-and-monitoring)
- [Benchmarks](#benchmarks)
- [Configuration Reference](#configuration-reference)
- [Building from Source](#building-from-source)
- [Running Tests](#running-tests)
- [CI Pipeline](#ci-pipeline)
- [Design Decisions](#design-decisions)

---

## What Is JCache

JCache started as a cache library implementing LRU, LFU, and ARC eviction policies with O(1) complexity. It then grew into a full cache server — accepting concurrent TCP connections, serving GET/PUT/DELETE at sub-millisecond latency, supporting TTL-based key expiry, optional on-disk persistence, and consistent-hash clustering across multiple nodes.

**What makes it different from just using a HashMap:**

- Three eviction policies (LRU, LFU, ARC) — choose based on your access pattern
- O(1) LFU using Shan's frequency-bucket algorithm, not the naive O(log n) heap version
- Three concurrency strategies — benchmark-proven throughput tradeoffs
- TTL expiry with dual-mechanism (lazy + background sweep), same as Redis
- TCP server built on Netty — accepts clients from any language
- Consistent hashing for multi-node deployments — only 1/N keys remap on node change
- Append-only log and snapshot persistence — survives restarts
- Full JMH benchmark suite with Zipfian and uniform workload results

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Clients                                  │
│         Go · Java · Python · curl · telnet · CLI                │
└───────────────┬─────────────────────────────────────────────────┘
                │  TCP :6379  (plain-text wire protocol)
                ▼
┌─────────────────────────────────────────────────────────────────┐
│                     cache-server                                 │
│                                                                 │
│   Netty Boss Thread ──► Netty Worker Threads (N)               │
│         │                       │                               │
│   ConnectionManager      CacheServerHandler                     │
│   (max conn limit)       (parse → dispatch → respond)          │
│         │                       │                               │
│   ServerMetrics          ServerConfig                           │
│   (ops/sec, p99)         (policy, capacity, TTL, threads)      │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      cache-core  (the engine)                   │
│                                                                 │
│  TTLCache ──► SegmentedCache ──► LRUCache / LFUCache / ARCCache │
│  (expiry)     (16 segments,      (eviction policy,              │
│               per-segment lock)   O(1) operations)              │
│                                                                 │
│  PersistenceManager                                             │
│  (append-only log + snapshots)                                  │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                     cache-common                                 │
│                                                                 │
│   ConsistentHashRing    CommandParser    ResponseEncoder        │
│   NodeRouter            ProtocolException                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Module Structure

```
JCache/
├── cache-core/              ← Cache library: policies, concurrency, TTL, persistence
├── cache-server/            ← TCP server built on Netty
├── cache-client/            ← Java client library + CLI
├── cache-common/            ← Shared: wire protocol, consistent hashing
├── cache-benchmark/         ← JMH benchmark suite
├── cache-integration-test/  ← End-to-end tests (client to server)
└── distribution/            ← Dockerfile, docker-compose, start/stop scripts
```

### cache-core

The heart of the project. Everything else depends on it.

| Package | Contents |
|---|---|
| `api/` | `Cache<K,V>` interface, `EvictionPolicy`, `CacheStats`, `CachePolicyType` |
| `core/` | `Node<K,V>` (doubly linked list node), `DoublyLinkedList` |
| `policy/` | `LRUCache`, `LFUCache`, `ARCCache` — all O(1) |
| `concurrent/` | `CoarseGrainedCache`, `SegmentedCache`, `LockFreeCache` |
| `ttl/` | `TTLCache` — decorator adding TTL expiry to any policy |
| `stats/` | `CacheMetricsCollector` — hit rate, miss rate, eviction rate |
| `persistence/` | `PersistenceManager`, `SnapshotWriter`, `SnapshotLoader` |
| `CacheFactory` | Factory and strategy pattern — plug any policy via one call |

### cache-server

The TCP server layer. Takes a `Cache<String,String>` from core and exposes it over TCP.

| File | Responsibility |
|---|---|
| `CacheServer.java` | Netty bootstrap, lifecycle (start/shutdown/startAsync) |
| `ServerConfig.java` | All config: port, policy, capacity, threads, TTL, persistence |
| `handler/CacheServerHandler.java` | Receives commands, dispatches to cache, writes response |
| `handler/ConnectionManager.java` | Tracks active connections, enforces max limit |
| `metrics/ServerMetrics.java` | ops/sec counter, sliding-window p50/p99 latency |

### cache-client

Client library and interactive CLI.

| File | Responsibility |
|---|---|
| `CacheClient.java` | Single-node programmatic Java client |
| `ClusterCacheClient.java` | Multi-node client with automatic consistent-hash routing |
| `ConnectionPool.java` | Fixed pool of TCP connections, acquire/release |
| `routing/ClientRouter.java` | Client-side consistent hash ring — mirrors server topology |
| `cli/CacheCLI.java` | Interactive shell: get, put, delete, stats, nodes |

### cache-common

Shared code used by both server and client — no circular dependencies.

| File | Responsibility |
|---|---|
| `protocol/CommandParser.java` | Parses raw text into `ParsedCommand` objects |
| `protocol/ResponseEncoder.java` | Formats responses: `+OK`, `+value`, `-ERR message` |
| `protocol/Command.java` | Enum: GET PUT DELETE STATS FLUSH PING |
| `cluster/ConsistentHashRing.java` | Sorted ring with 150 virtual nodes per physical node |
| `cluster/NodeRouter.java` | Routes key to CacheNode via ring lookup |
| `cluster/CacheNode.java` | Represents one physical server: id, host, port |

### cache-benchmark

JMH benchmark suite. Run independently from the main build.

| File | What it measures |
|---|---|
| `LRUBenchmark` | LRU throughput and latency — Zipfian and uniform workloads |
| `LFUBenchmark` | LFU throughput and latency |
| `ARCBenchmark` | ARC throughput and latency |
| `PolicyComparisonBenchmark` | All three policies, same workload, same JVM |
| `ConcurrencyBenchmark` | Coarse vs segmented vs lock-free under N threads |
| `NetworkBenchmark` | TCP round-trip latency: single client and 8-client concurrent |
| `BenchmarkWorkload` | Zipfian and uniform key-distribution generators |

---

## Quick Start

### Option 1 — Docker (recommended)

```bash
# Start with defaults (LRU, 10k capacity, port 6379)
docker run -d -p 6379:6379 himanshmunjal/jcache:latest

# Verify it's running
docker logs <container-id>
```

### Option 2 — Build and run locally

```bash
git clone https://github.com/himanshmunjal/JCache.git
cd JCache
mvn clean package -DskipTests
java -jar cache-server/target/cache-server-*-jar-with-dependencies.jar
```

### Connect with telnet

```bash
telnet localhost 6379

PUT name Alice
+OK

GET name
+Alice

STATS
+hits:1 misses:0 evictions:0 connections:1

PING
+PONG
```

---

## Docker Usage

### Basic run

```bash
docker run -d -p 6379:6379 himanshmunjal/jcache:latest
```

### Configured run

All configuration is done via environment variables:

```bash
docker run -d -p 6379:6379 \
  -e JCACHE_POLICY=ARC \
  -e JCACHE_CAPACITY=50000 \
  -e JCACHE_DEFAULT_TTL=3600 \
  -e JCACHE_MAX_CONNECTIONS=1000 \
  -e JCACHE_THREADS=16 \
  himanshmunjal/jcache:latest
```

### docker-compose multi-node cluster

```bash
cd distribution
docker-compose up -d
```

Spins up three JCache nodes on ports 6379, 6380, 6381. `ClusterCacheClient` connects to all three and routes keys via consistent hashing automatically.

### Environment Variable Reference

| Variable | Default | Description |
|---|---|---|
| `JCACHE_PORT` | `6379` | TCP port the server listens on |
| `JCACHE_POLICY` | `LRU` | Eviction policy: LRU, LFU, or ARC |
| `JCACHE_CAPACITY` | `10000` | Maximum number of keys before eviction fires |
| `JCACHE_DEFAULT_TTL` | `0` | Default TTL in seconds. 0 means no expiry |
| `JCACHE_MAX_CONNECTIONS` | `1000` | Maximum concurrent TCP connections |
| `JCACHE_THREADS` | `8` | Netty worker thread count |
| `JCACHE_SEGMENTS` | `16` | Number of lock segments (higher = less contention) |
| `JCACHE_SWEEP_INTERVAL` | `500` | TTL sweeper interval in milliseconds |
| `JCACHE_VERBOSE` | `false` | Log every command to stdout |
| `JCACHE_PERSIST` | `false` | Enable persistence (snapshot + append log) |
| `JCACHE_SNAPSHOT_PATH` | `/data` | Directory for snapshots and append log |

### Startup Banner

```
==========================================
  JCache Server started successfully
==========================================
  Port      : 6379
  Policy    : ARC
  Capacity  : 50,000
  Workers   : 16
  Max Conn  : 1,000
  Verbose   : false
  Persist   : true
==========================================
  Ready to accept connections
  Use Ctrl+C or SIGTERM to stop
==========================================
```

---

## Eviction Policies

When the cache reaches capacity, one key must be evicted to make room. JCache implements three policies, all with O(1) complexity.

### LRU — Least Recently Used

Every key access moves that key to the front of a doubly linked list. When eviction is needed, the key at the tail (least recently touched) is removed.

**Data structure:** `HashMap<K, Node>` + doubly linked list. The map gives O(1) lookup. The list gives O(1) move-to-front and O(1) tail removal.

**Best for:** General-purpose caching. Session caches, page caches, API response caches. Access recency is usually a good proxy for future access.

**Weakness:** Suffers on sequential scan patterns. A one-time scan of 100k keys evicts your hot working set entirely.

```bash
docker run -e JCACHE_POLICY=LRU himanshmunjal/jcache:latest
```

### LFU — Least Frequently Used (O(1) via Shan's Algorithm)

JCache implements the O(1) LFU algorithm from the paper *"An O(1) algorithm for implementing the LFU cache eviction scheme"* by Shah, Mitra, and Matani. Most LFU implementations use a min-heap at O(log n). This one uses frequency buckets.

**Data structure:** Three maps and doubly linked lists per frequency bucket.
- `keyMap: K → (value, frequency)` — O(1) lookup
- `freqMap: frequency → DoublyLinkedList<K>` — all keys at this frequency, LRU-ordered within the bucket
- `minFreq: int` — tracks current minimum frequency for O(1) eviction targeting

When a key is accessed: its frequency increments and it moves from `freqMap[f]` to `freqMap[f+1]`. When eviction is needed: remove the LRU key from `freqMap[minFreq]`.

**Best for:** Workloads with a clear hot set — a small number of keys accessed far more than others. Rate-limit counters, feature flag caches, leaderboard data.

**Weakness:** Cold-start problem — new keys start at frequency 1 and may be evicted before they have a chance to accumulate accesses. Poor performance on uniform-distribution workloads.

```bash
docker run -e JCACHE_POLICY=LFU himanshmunjal/jcache:latest
```

### ARC — Adaptive Replacement Cache

Developed at IBM Research and used in ZFS and the macOS kernel. ARC maintains four lists:

- **T1** — keys seen exactly once recently (recency list)
- **T2** — keys seen more than once recently (frequency list)
- **B1** — ghost list: recently evicted keys from T1 (metadata only, no values)
- **B2** — ghost list: recently evicted keys from T2 (metadata only, no values)

A parameter `p` (the target size for T1) adapts dynamically. A cache miss that hits B1 means the workload is recency-favoring — increase `p`, give more space to T1. A cache miss that hits B2 means the workload is frequency-favoring — decrease `p`, give more space to T2. No manual tuning required.

**Best for:** Production workloads where the access pattern is unknown or mixed. ARC consistently matches or beats the best fixed policy in benchmarks.

**Weakness:** More memory overhead than LRU or LFU due to ghost lists — approximately 2x the metadata.

```bash
docker run -e JCACHE_POLICY=ARC himanshmunjal/jcache:latest
```

### Policy Comparison

| | LRU | LFU | ARC |
|---|---|---|---|
| Time complexity | O(1) | O(1) | O(1) |
| Handles recency | Yes | No | Yes |
| Handles frequency | No | Yes | Yes |
| Self-tuning | No | No | Yes |
| Memory overhead | Low | Medium | High (ghost lists) |
| Best workload | General | Hot-key heavy | Unknown or mixed |
| Cold-start problem | No | Yes | No |

---

## Concurrency Strategies

All three cache policies are single-threaded by design. The concurrency layer wraps them as a separate concern. This means policy correctness is verified without threading complexity, and threading correctness is verified without policy logic.

### Coarse-Grained Locking

A single `ReentrantReadWriteLock` wraps the entire cache. Read lock for `get()`, write lock for `put()` and `evict()`.

**When to use:** Single-threaded workloads, or as a correctness baseline for testing. Maximum simplicity, maximum contention.

### Segmented Locking (Default)

The cache is split into N segments (default 16). `hash(key) % N` selects the segment. Each segment has its own `ReentrantReadWriteLock` and its own underlying policy cache. Global `size()` sums all segments.

**When to use:** Multi-threaded production workloads. This is the default. Benchmark results show ~4.4x throughput improvement over coarse locking under 16 threads.

```bash
docker run -e JCACHE_SEGMENTS=32 himanshmunjal/jcache:latest
```

### Lock-Free Reads

`ConcurrentHashMap` backbone for key storage. `AtomicLong` for hit/miss counters. `AtomicInteger` for LFU frequency counters. Locking only on eviction where structural list modification is unavoidable.

**When to use:** Read-heavy workloads (80%+ reads) under very high thread counts. Highest throughput, most complex implementation.

---

## TTL Support

Every key can carry a time-to-live in seconds. After the TTL elapses, the key is treated as if it does not exist.

### Two-Mechanism Expiry

**Lazy eviction** checks expiry on every `get()`. Before returning a value, TTLCache checks if the key's deadline has passed. If so, the key is deleted and null is returned. Cost is O(1) per get with no background overhead.

**Active sweeper** is a background `ScheduledExecutorService` thread waking every `JCACHE_SWEEP_INTERVAL` milliseconds (default 500ms). It scans all keys with deadlines and deletes expired ones. Memory is reclaimed even for keys that are never read again.

Neither mechanism alone is sufficient: lazy alone leaves expired keys in memory forever if unread; sweeper alone adds unpredictable latency spikes on cleanup cycles. Together they are both correct and memory-efficient — this is exactly the dual-mechanism approach used by Redis.

### TTL Semantics

| TTL value | Meaning |
|---|---|
| `0` | No expiry — key lives until evicted by policy |
| Greater than 0 | Expires after N seconds |
| Less than 0 | IllegalArgumentException — rejected immediately |

### Setting TTL

Via Docker (server-wide default for all keys):
```bash
docker run -e JCACHE_DEFAULT_TTL=3600 himanshmunjal/jcache:latest
```

Via wire protocol (per-key, overrides server default):
```
PUT session:abc token123 3600      ← this key expires in 1 hour
PUT config:flags feature=true      ← no TTL (server default applies)
```

Priority: per-key TTL in PUT command takes precedence over server default TTL.

### TTL Reset on Re-put

Re-putting a key completely replaces its TTL:

```
PUT session:abc token1 60     ← expires in 60 seconds
PUT session:abc token2 3600   ← TTL reset: now expires in 1 hour
```

This enables session renewal, cache refresh, and sliding expiry patterns.

---

## Persistence

JCache can persist cache contents to disk so data survives server restarts.

### Enable Persistence

```bash
docker run -d -p 6379:6379 \
  -e JCACHE_PERSIST=true \
  -e JCACHE_SNAPSHOT_PATH=/data \
  -v /your/host/path:/data \
  himanshmunjal/jcache:latest
```

### How It Works

**Append-Only Log (AOF):** Every PUT and DELETE is immediately appended to a log file on disk. The log records the operation type, key, value, and TTL. This is the same design Redis uses in AOF mode.

**Snapshots:** Periodically, the full cache state is serialized to a binary snapshot file. Written atomically — JCache writes to a temp file and renames it, so a crash during snapshot write never corrupts the previous valid snapshot.

**Recovery on restart:**
1. Load the most recent snapshot (rebuilds the bulk of cache state instantly)
2. Replay the append log on top (applies all changes made since the last snapshot)
3. Skip any keys whose TTL elapsed during the downtime period

### Persistence Classes

| Class | Responsibility |
|---|---|
| `PersistenceManager` | Orchestrates AOF writes: logPut(), logDelete(), flush() |
| `SnapshotWriter` | Serializes full cache state atomically to binary file |
| `SnapshotLoader` | Reads snapshot and replays append log on server startup |
| `PersistenceConfig` | Snapshot path, flush interval, max log size before compaction |

---

## Cache Server

Built on **Netty** — an asynchronous, non-blocking TCP framework. One boss thread accepts incoming connections. N worker threads handle all reads and writes concurrently.

### Netty Pipeline

```
Raw TCP bytes
     ↓
RespDecoder          ← frames incoming bytes into complete command lines
     ↓
RespEncoder          ← formats response objects into wire bytes
     ↓
ConnectionManager    ← enforces max connection limit, tracks active count
     ↓
CacheServerHandler   ← parses command, dispatches to cache, writes response
```

### Connection Management

`ConnectionManager` uses Netty's `channelActive` and `channelInactive` callbacks to track open connections. When `maxConnections` is reached, new connections receive `-ERR server at connection limit` and are immediately closed. Active connection count is included in every `STATS` response.

### Graceful Shutdown

On SIGTERM or Ctrl+C, the shutdown hook executes in order:
1. Stop accepting new connections
2. Close all active channels
3. Stop the TTL sweeper thread
4. Gracefully shut down Netty event loop groups (200ms quiet period, 3s timeout)
5. Flush the persistence log if persistence is enabled

### startAsync() for Testing

`server.startAsync()` starts the server on a background daemon thread and blocks the calling thread until the server is ready to accept connections (timeout: 5 seconds). Used in integration tests to boot a real embedded server on a random port for each test class without hardcoding ports.

---

## Wire Protocol

Plain-text TCP protocol. Every command is a single line terminated by `\r\n`. Responses begin with `+` for success or `-ERR` for errors. Debuggable directly with telnet.

### Commands

```
PING
  → +PONG

GET <key>
  → +<value>
  → -ERR key not found

PUT <key> <value> [ttlSeconds]
  → +OK

DELETE <key>
  → +OK

STATS
  → +hits:<n> misses:<n> evictions:<n> connections:<n> uptime:<seconds>

FLUSH
  → +OK
```

### Example Session

```
telnet localhost 6379

PUT user:123 Alice 3600
+OK

GET user:123
+Alice

DELETE user:123
+OK

GET user:123
-ERR key not found

STATS
+hits:142 misses:31 evictions:8 connections:2 uptime:3601
```

### Value Encoding

Values are URL-encoded by the client before sending. This allows values to contain spaces, newlines, JSON, and any UTF-8 characters without breaking the line-delimited protocol. The server stores and returns the encoded form. Clients decode on receipt.

---

## Consistent Hashing and Clustering

When running multiple JCache nodes, consistent hashing determines which node owns which key.

### Why Not Modulo Hashing

Naive modulo: `hash(key) % N`. When N changes (node added or removed), virtually every key remaps to a different node — cache miss rate spikes to near 100% temporarily. With consistent hashing, only `1/N` of keys remap. Adding a fourth node to a three-node cluster remaps approximately 25% of keys; the other 75% remain on their current node.

### How the Ring Works

`ConsistentHashRing` maintains a `TreeMap<Integer, CacheNode>` — a sorted map of hash positions to nodes. Each physical node occupies 150 positions on the ring (virtual nodes). More virtual nodes means more even key distribution across nodes.

To route a key: hash the key, do a `tailMap(hash)` lookup to find the nearest clockwise position on the ring, return the `CacheNode` at that position. If `tailMap` is empty (hash is past the last node), wrap around to the first node.

### Adding and Removing Nodes

```java
ConsistentHashRing ring = new ConsistentHashRing();
ring.addNode(new CacheNode("node1", "localhost", 6379));
ring.addNode(new CacheNode("node2", "localhost", 6380));
ring.addNode(new CacheNode("node3", "localhost", 6381));

CacheNode owner = ring.getNode("user:123");

// Remove a node — only ~1/3 of keys remap
ring.removeNode(new CacheNode("node2", "localhost", 6380));
```

### Client-Side Routing

`ClusterCacheClient` mirrors the same ring on the client side. When you call `get("user:123")`, the client computes the same hash, finds the same node, and connects to it directly. No coordinator, no proxy, no extra network hop.

```java
ClusterCacheClient cluster = new ClusterCacheClient();
cluster.addServer("localhost", 6379);
cluster.addServer("localhost", 6380);
cluster.addServer("localhost", 6381);

cluster.put("user:123", "Alice", 3600);  // routes to whichever node owns user:123
cluster.get("user:123");                 // routes to the same node
```

---

## Java Client

### Single-Node Client

```java
import com.cache.client.CacheClient;

CacheClient client = new CacheClient("localhost", 6379);

// Put with TTL
client.put("session:abc", "user_data", 3600);   // expires in 1 hour

// Put without TTL
client.put("config:feature_x", "enabled", 0);

// Get
String value = client.get("session:abc");
if (value != null) {
    System.out.println(value);
}

// Delete
client.delete("session:abc");

// Stats
Map<String, String> stats = client.stats();

// Always close when done
client.close();
```

### Connection Pool

`ConnectionPool` maintains a fixed pool of pre-created TCP connections. Callers acquire a connection and release it when done. If the pool is exhausted, acquire blocks until one becomes available. Idle connections are health-checked via PING.

```java
// CacheClient uses the pool internally
ConnectionPool pool = new ConnectionPool("localhost", 6379, 10); // pool of 10
CacheClient client = new CacheClient(pool);
```

### Cluster Client

```java
import com.cache.client.ClusterCacheClient;

ClusterCacheClient cluster = new ClusterCacheClient();
cluster.addServer("node1.internal", 6379);
cluster.addServer("node2.internal", 6379);
cluster.addServer("node3.internal", 6379);

cluster.put("user:1", "Alice", 3600);
cluster.put("user:2", "Bob",   3600);

String alice = cluster.get("user:1"); // automatically routed to correct node
String bob   = cluster.get("user:2");

// Fan-out: collect stats from all nodes
Map<String, String> allStats = cluster.all();
```

---

## Go Client Example

JCache's plain-text protocol means any language can connect with a TCP socket and a few lines of code. Here is the Go client implementation used in a separate backend project:

```go
import cache "github.com/yourproject/jcache-go"

client, err := cache.NewJCacheClient("localhost", 6379)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Put with TTL
client.Put("session:abc123", "user_data_json", 3600)

// Put without TTL (permanent until policy eviction)
client.Put("config:flags", "feature_x=true", 0)

// Get — returns (value, found, error)
value, found, err := client.Get("session:abc123")
if found {
    fmt.Println(value)
}

// Delete
client.Delete("session:abc123")

// Health check
err = client.Ping()

// Server stats
stats, err := client.Stats()
```

The Go client handles URL-encoding of values (to support JSON and special characters), automatic reconnection on dropped connections, and per-operation deadlines via `SetDeadline`. No JVM required on the client side.

---

## CLI Client

An interactive shell for manual inspection, debugging, and one-off operations.

```bash
java -jar cache-client/target/cache-client-*-jar-with-dependencies.jar localhost 6379
```

```
jcache> get user:123
Alice

jcache> put session:abc token123 3600
OK

jcache> delete session:abc
OK

jcache> stats
hits: 142 | misses: 31 | evictions: 8 | connections: 1 | uptime: 3601s

jcache> nodes
node1 → localhost:6379 (this node)
node2 → localhost:6380
node3 → localhost:6381

jcache> ping
PONG

jcache> flush
OK

jcache> exit
```

| Command | Description |
|---|---|
| `get <key>` | Retrieve a value |
| `put <key> <value> [ttl]` | Store a value with optional TTL in seconds |
| `delete <key>` | Remove a key |
| `stats` | Show server metrics |
| `nodes` | List cluster nodes (cluster mode) |
| `ping` | Check server health |
| `flush` | Remove all keys |
| `exit` | Close the CLI |

---

## Metrics and Monitoring

### Server Metrics (via STATS command)

| Metric | Description |
|---|---|
| `hits` | Total successful GET responses |
| `misses` | Total GET responses for absent or expired keys |
| `evictions` | Total keys removed by policy (not TTL) |
| `ttl_evictions` | Total keys removed by TTL sweeper |
| `connections` | Current active TCP connections |
| `uptime` | Seconds since server started |
| `ops_per_sec` | Rolling ops/second over the last 10 seconds |
| `p50_latency` | Median operation latency in microseconds (sliding window of last 1000 ops) |
| `p99_latency` | 99th percentile latency in microseconds |

### CacheMetricsCollector

At the library level, `CacheMetricsCollector` tracks hit rate, miss rate, and eviction rate using atomic counters. Call `snapshot()` for an immutable point-in-time view:

```java
CacheMetricsCollector collector = new CacheMetricsCollector(cache);
MetricsSnapshot snap = collector.snapshot();
System.out.println("Hit rate: " + snap.getHitRate());
System.out.println("Eviction rate: " + snap.getEvictionRate());
```

---

## Benchmarks

All benchmarks use JMH (Java Microbenchmark Harness), 3 forks, 5 warmup iterations, 10 measurement iterations. Full raw output is in [`docs/benchmark-results.md`](docs/benchmark-results.md).

### Policy Comparison — Zipfian Workload (80/20 access pattern)

| Policy | Throughput (ops/sec) | Hit Rate | p99 Latency |
|---|---|---|---|
| LRU | ~4.2M | 76% | 890ns |
| LFU | ~3.8M | 84% | 920ns |
| ARC | ~3.5M | 87% | 980ns |

LFU and ARC both outperform LRU on hit rate with a Zipfian workload (which mimics real access patterns). ARC wins on hit rate without any tuning. LRU wins on raw throughput due to simpler data structure operations.

### Concurrency Strategy — 16 Threads, Mixed Read/Write

| Strategy | Throughput (ops/sec) | Notes |
|---|---|---|
| Coarse-grained | ~1.1M | Single lock — maximum contention |
| Segmented (16) | ~4.8M | 4.4x improvement over coarse |
| Lock-free | ~6.2M | Best throughput — most complex |

### Network Round-Trip — localhost

| Metric | Value |
|---|---|
| p50 latency | ~0.3ms |
| p99 latency | ~0.8ms |
| Throughput (1 client) | ~18k ops/sec |
| Throughput (8 clients) | ~95k ops/sec |

---

## Configuration Reference

### Priority Order

```
Command-line args (--port, --policy, --capacity)    ← highest priority
         ↓
Environment variables (JCACHE_PORT, JCACHE_POLICY)  ← Docker -e flags
         ↓
Properties file (jcache.properties)                 ← file-based config
         ↓
Defaults hardcoded in ServerConfig                   ← lowest priority
```

### jcache.properties

```properties
port=6379
policy=ARC
capacity=50000
default.ttl=1800
worker.threads=16
max.connections=1000
segments=32
sweep.interval.ms=500
verbose=false
persistence.enabled=false
snapshot.path=/data
```

Place `jcache.properties` in the same directory as the jar, or specify the path:

```bash
java -jar cache-server.jar --config /etc/jcache/jcache.properties
```

### Command-Line Args

```bash
java -jar cache-server.jar \
  --port 6379 \
  --policy ARC \
  --capacity 100000 \
  --verbose \
  --config /etc/jcache/jcache.properties
```

---

## Building from Source

### Requirements

- Java 17 or later
- Maven 3.8 or later
- Docker (optional, for containerized deployment)

### Build all modules

```bash
mvn clean package
```

### Build skipping tests

```bash
mvn clean package -DskipTests
```

### Build only the server fat jar

```bash
mvn clean package -pl cache-server -am -DskipTests
```

### Build the Docker image

```bash
mvn clean package -DskipTests
docker build -t himanshmunjal/jcache:latest -f distribution/Dockerfile .
```

### Run locally after build

```bash
# Default config
java -jar cache-server/target/cache-server-*-jar-with-dependencies.jar

# Custom config via args
java -jar cache-server/target/cache-server-*-jar-with-dependencies.jar \
  --policy LFU --capacity 50000 --port 6380
```

---

## Running Tests

### All tests

```bash
mvn test
```

### Specific module

```bash
mvn test -pl cache-core
mvn test -pl cache-server
mvn test -pl cache-common
```

### Specific test class

```bash
mvn test -pl cache-core -Dtest=LRUCacheTest
mvn test -pl cache-core -Dtest=TTLCacheTest
mvn test -pl cache-server -Dtest=ServerIntegrationTest
```

### Specific test method

```bash
mvn test -pl cache-core -Dtest=TTLCacheTest#testLazyEviction_afterExpiry
```

### Verbose output (useful when a test hangs)

```bash
mvn test -pl cache-core -Dsurefire.useFile=false
```

### Integration tests

```bash
mvn test -pl cache-integration-test
```

Integration tests use `server.startAsync()` to boot an embedded server on a random port. No manually running server is needed.

### JMH Benchmarks

```bash
mvn clean package -pl cache-benchmark -am
java -jar cache-benchmark/target/benchmarks.jar

# Run specific benchmark
java -jar cache-benchmark/target/benchmarks.jar PolicyComparisonBenchmark

# Quick CI-friendly run
java -jar cache-benchmark/target/benchmarks.jar -f 1 -wi 1 -i 3
```

### Code Coverage

JaCoCo generates coverage reports on every `mvn test` run:

```bash
mvn test
open cache-core/target/site/jacoco/index.html
```

CI enforces a minimum of 85% line coverage on `cache-core`. The build fails if coverage drops below this threshold.

---

## CI Pipeline

GitHub Actions runs on every push and pull request to main.

**ci.yml:**
1. Checkout code
2. Set up Java 17
3. `mvn clean verify` — compiles, tests, generates JaCoCo report
4. Fail if coverage is below 85%
5. Upload JaCoCo report as a build artifact

**benchmark.yml** (runs on push to main only):
1. Run JMH suite in fast mode (1 fork, 1 warmup, 3 iterations)
2. Upload benchmark JSON as an artifact
3. Post a benchmark summary comment on the commit

---

## Design Decisions

### Why Netty over raw Java NIO or an HTTP framework

Raw NIO requires manually managing selection keys, ByteBuffers, and partial reads — hundreds of lines of error-prone boilerplate. An HTTP framework adds JSON serialization overhead and HTTP framing (headers, status lines, content-type) that you pay on every single cache operation. Netty provides the NIO performance model with a clean pipeline abstraction, giving sub-millisecond latency with readable code.

### Why a custom text protocol over HTTP/REST

REST over HTTP adds at minimum 200–400 bytes of header overhead per request. For a cache serving millions of ops/sec, that overhead is measurable. A plain-text protocol (`GET key\r\n`) is roughly 10 bytes. It is debuggable with telnet. It is trivial to implement in any language without a library dependency. This is exactly why Redis chose a text protocol in 2009 and still uses it today.

### Why O(1) LFU specifically

Most LFU implementations use a min-heap: O(log n) to increment frequency and O(log n) to find the minimum-frequency eviction target. At millions of ops/sec, that logarithm compounds. Shan's frequency-bucket algorithm achieves O(1) for all operations by maintaining a `minFreq` integer pointer that updates in O(1) on every access. The implementation is more complex than LRU but the benchmark results show the throughput difference is negligible while the hit-rate improvement on skewed workloads is significant.

### Why segmented locking as the default rather than lock-free

Lock-free data structures are substantially harder to implement correctly. The CAS operations and Java memory model ordering requirements are subtle and difficult to test exhaustively. Segmented locking with 16 segments delivers approximately 94% of the throughput of the lock-free implementation (4.8M vs 6.2M ops/sec) with a fraction of the complexity. Lock-free is available for extreme read-heavy workloads, but segmented is the default because it is auditable.

### Why 150 virtual nodes in consistent hashing

Fewer virtual nodes (for example, 1 per physical node) produces uneven key distribution — some nodes handle 2x the load of others. More virtual nodes (for example, 1000) uses more memory and slows `addNode` and `removeNode` operations. 150 is the value used by Apache Cassandra and is empirically validated to produce key distribution within ±10% of ideal across typical cluster sizes of 3 to 20 nodes.

---

## Roadmap

- RESP protocol compatibility — wire-compatible with existing Redis clients
- Replication — primary/replica with async log shipping
- Raft consensus — strongly consistent cluster with leader election
- Prometheus metrics endpoint — HTTP `/metrics` for Grafana integration
- Key eviction callbacks — notify application code when a key is evicted or expires
- Lua scripting — atomic multi-key operations

---

*Built by [Himansh Munjal](https://github.com/himanshmunjal) — a systems engineering deep-dive into cache internals, JVM concurrency primitives, Netty networking, and distributed systems fundamentals.*
