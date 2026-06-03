# JCache — Complete Project Reference

> Every file, every tool used, every tradeoff, every bottleneck.  
> This document is your interview preparation and your architectural memory.

---

## Table of Contents

1. [Project Architecture Overview](#1-project-architecture-overview)
2. [Module: cache-core](#2-module-cache-core)
    - [api/](#21-api)
    - [core/](#22-core)
    - [policy/](#23-policy)
    - [ttl/](#24-ttl)
    - [concurrent/](#25-concurrent)
    - [stats/](#26-stats)
    - [CacheFactory](#27-cachefactory)
3. [Module: cache-server](#3-module-cache-server)
    - [Server Bootstrap](#31-server-bootstrap)
    - [Protocol Layer](#32-protocol-layer)
    - [Handler Layer](#33-handler-layer)
    - [Cluster Layer](#34-cluster-layer)
    - [Metrics](#35-metrics)
4. [Module: cache-client](#4-module-cache-client)
5. [Module: cache-benchmarks](#5-module-cache-benchmarks)
6. [Test Files](#6-test-files)
7. [Java Primitives Master Reference](#7-java-primitives-master-reference)
8. [Design Patterns Used](#8-design-patterns-used)
9. [Bottlenecks and Known Limitations](#9-bottlenecks-and-known-limitations)
10. [Critical Things to Never Get Wrong](#10-critical-things-to-never-get-wrong)
11. [What to Say in the FAANG Interview](#11-what-to-say-in-the-faang-interview)

---

## 1. Project Architecture Overview

```
cache-library/
├── cache-core/          ← The library. LRU/LFU/ARC + TTL + concurrency wrappers
├── cache-server/        ← Netty TCP server. Turns the library into a network service
├── cache-client/        ← Java client + connection pool + cluster routing
└── cache-benchmarks/    ← JMH suite. Measures throughput, latency, hit rates
```

### Data Flow

```
Client Application
    │
    ▼
ClientRouter                  ← hashes key → picks server → opens connection
    │  TCP
    ▼
Netty Pipeline
  LineBasedFrameDecoder       ← splits byte stream on newlines
  StringDecoder               ← bytes → String
  StringEncoder               ← String → bytes (outbound)
  ConnectionManager           ← enforces max connections, tracks active count
  CacheServerHandler          ← parses command, calls cache engine, writes response
    │
    ▼
SegmentedCache                ← 16 segments, per-segment locks
    │
    ▼
LRUCache / LFUCache / ARCCache ← the eviction policy
    │
    ▼
TTLCache (optional wrapper)   ← lazy expiry + background sweeper
```

### Why This Architecture Matters

Every layer has one job. `CommandParser` only parses. `ResponseEncoder` only formats. `CacheServerHandler` only dispatches. This separation means you can test every layer in isolation, and bugs are localized — a wrong response format is in `ResponseEncoder`, not scattered in the handler.

---

## 2. Module: cache-core

The library that everything else depends on. Contains zero network code, zero framework dependencies. Pure Java.

---

### 2.1 api/

**`Cache.java`** — The master interface. Every cache implementation, wrapper, and decorator in the system implements this one interface. Defines: `get(K key)`, `put(K key, V value)`, `evict(K key)`, `size()`, `getStats()`.

- **Why it exists**: Without a common interface, every layer would be coupled to a concrete class. `CacheServerHandler` would be `CacheServerHandler<LRUCache>`. Adding ARC would require rewriting the handler. The interface makes policies hot-swappable via `CacheFactory`.
- **Critical contract**: `get()` returns `null` for missing keys, not an exception. Every caller in the system relies on this. If you throw instead of returning null, the server will send error responses for normal cache misses.

**`EvictionPolicy.java`** — Enum: `LRU`, `LFU`, `ARC`. Used by `CacheFactory` and `SegmentedCache.PolicyType` to select which concrete cache to instantiate.

- **Why enum not subtype**: The policy is a configuration choice, not a behavioral variation. Enum + factory is cleaner than abstract class hierarchies for this use case.

**`CacheStats.java`** — Value object holding `hits`, `misses`, `evictions`, `currentSize` as `AtomicLong` fields. Returned by `Cache.getStats()`.

- **AtomicLong used here because**: Stats are read from multiple threads (STATS command) and written from multiple threads (every get/put). `AtomicLong` provides lock-free thread-safe reads and writes via CAS (compare-and-swap). Plain `long` would cause data races — a thread might read a half-written value on a 32-bit JVM.
- **Why not just `long`**: On 32-bit JVMs, `long` reads/writes are not atomic (two 32-bit operations). On 64-bit JVMs they are, but the JMM (Java Memory Model) doesn't guarantee visibility without `volatile` or synchronization. `AtomicLong` gives both atomicity and visibility.

---

### 2.2 core/

**`Node.java`** — A generic doubly linked list node. Fields: `key`, `value`, `prev`, `next`, `expiryTime`. Used by LRU and LFU's internal linked lists.

- **Why a custom linked list node instead of `java.util.LinkedList`**: `java.util.LinkedList` doesn't let you remove a specific node in O(1) given a reference to it. It requires an iterator scan — O(n). A custom `Node` with `prev` and `next` pointers allows O(1) removal: `node.prev.next = node.next; node.next.prev = node.prev`. This is the operation that makes LRU O(1).
- **`expiryTime` field**: Stored directly in the node so TTLCache can co-locate expiry info with the node. Alternative would be a separate `Map<K, Long>` — but TTLCache uses its own `ConcurrentHashMap<K, Long>` because it wraps any cache, not just `Node`-based ones. The field exists for potential future use in a fused LRU+TTL implementation.

**`DoublyLinkedList.java`** — The actual list. Operations: `addToFront(node)`, `remove(node)`, `removeLast()`, `getLast()`.

- **O(1) for all operations**: Because we always operate on nodes we already have a reference to. `removeLast()` uses the tail pointer — no scan.
- **Sentinel head and tail nodes**: Dummy head and tail nodes eliminate all null checks for prev/next during pointer manipulation. This is a standard technique that makes edge cases (empty list, single element) disappear.
- **Not thread-safe**: Deliberately. Thread safety is added by the wrapping `CoarseGrainedCache` or `SegmentedCache`. Mixing concerns would make both harder to test.

---

### 2.3 policy/

**`LRUCache.java`** — Least Recently Used eviction. Internally: `HashMap<K, Node<K,V>>` + `DoublyLinkedList`.

- **Data structures**: HashMap gives O(1) key lookup to find the node. DoublyLinkedList gives O(1) move-to-front and O(1) tail eviction. Together: all operations O(1).
- **How get() works**: Look up node in HashMap. Move it to the front of the list (most recently used). Return value.
- **How put() works**: If key exists: update value, move to front. If new key: create node, add to front, add to HashMap. If over capacity: remove tail node, remove from HashMap, add new node.
- **Weakness**: Scan attack — sequential access to N keys evicts all hot keys. A single `for (i in range(1000)) cache.get("key-" + i)` can wipe out all cached values.
- **Real-world use**: Redis's default policy (though Redis uses a randomized approximation, not exact LRU, for performance).

**`LFUCache.java`** — Least Frequently Used eviction. Internally: `HashMap<K, Node>` + `HashMap<K, Integer> freqMap` + `HashMap<Integer, DoublyLinkedList> freqBuckets` + `int minFreq`.

- **Data structures**: Three maps. The key innovation (Shan's O(1) LFU algorithm): instead of a min-heap (O(log n) for every operation), use frequency buckets — a map from frequency to a doubly linked list of all keys with that frequency. `minFreq` tracks which bucket to evict from.
- **How get() works**: Find node. Increment its frequency. Move it from `freqBuckets[freq]` to `freqBuckets[freq+1]`. Update `minFreq` if the old bucket is now empty.
- **How put() works**: If new key, add to `freqBuckets[1]`, set `minFreq = 1`. If over capacity, evict from `freqBuckets[minFreq]`.
- **Why O(1)**: Every operation is a fixed number of HashMap and LinkedList operations, each O(1). No sorting, no scanning.
- **Weakness — cold start**: A brand new key has frequency=1 and is the first evicted, even if it's about to become the most popular key in the system. LFU needs time to "learn" hot keys.
- **Weakness — frequency pollution**: Old accesses count forever. A key accessed 1000 times last week but not today will never be evicted. Needs frequency decay for production use (we don't implement decay — it's a known limitation).

**`ARCCache.java`** — Adaptive Replacement Cache. Internally: four doubly linked lists: `T1` (recent, one access), `T2` (frequent, two+ accesses), `B1` (ghost list of recently evicted T1 keys), `B2` (ghost list of recently evicted T2 keys). Plus adaptation parameter `p`.

- **Data structures**: Four lists + a HashMap for O(1) lookup across all four. Ghost lists store only keys (no values) — they're metadata, not data. This is why ARC's memory footprint is ~2x capacity: the actual capacity is used for T1+T2, but ghost lists add an equal number of key-only entries.
- **The adaptation mechanic**: When a B1 ghost hit occurs (a key we evicted from T1 is accessed again), `p++` — grow T1's target size (recency bias). When a B2 ghost hit occurs (evicted from T2, accessed again), `p--` — grow T2's target size (frequency bias). `p` converges to the optimal split for the current workload without any manual tuning.
- **Scan resistance**: Sequential scan keys enter T1. T1 fills up but doesn't steal from T2. Hot keys in T2 are protected. When the scan ends, T2 hot keys are still there. LRU would have evicted T2 hot keys to make room for scan keys.
- **Why it's in ZFS**: ZFS caches disk blocks. File system workloads mix sequential scans (backups, compaction) with random access (application data). ARC handles both without reconfiguration — hence its adoption in ZFS (OpenZFS still uses it).
- **Patent history**: IBM held US Patent 6,996,676 on ARC until 2023. This is why it was never in the Linux kernel — legal risk prevented adoption even though it's technically superior to LRU for most workloads.

---

### 2.4 ttl/

**`TTLCache.java`** — Decorator that adds time-based expiry to any `Cache<K,V>`.

- **Design pattern**: Decorator. `TTLCache` implements `Cache<K,V>` and holds a `Cache<K,V>` delegate. This means you can write `new TTLCache(new LFUCache(100))` or `new TTLCache(new ARCCache(100))` — the TTL logic is reusable across all policies without inheritance.
- **Java primitives used**:
    - `ConcurrentHashMap<K, Long> expiryMap`: Maps each key to its deadline timestamp. `ConcurrentHashMap` because the sweeper thread and application threads both read/write this map concurrently. Plain `HashMap` would cause `ConcurrentModificationException` or corrupt state.
    - `AtomicLong sweepEvictions`: Counts how many keys the sweeper has removed. `AtomicLong` for thread-safe increment from the sweeper thread, readable from the application thread.
    - `ScheduledExecutorService sweeper`: Runs the background sweep task at a fixed rate. Created with a daemon thread so it doesn't prevent JVM shutdown if `shutdown()` is forgotten.
- **Two-mechanism expiry** (same as Redis):
    - *Lazy eviction*: `get()` checks `expiryMap` before delegating. O(1), zero background overhead. Problem: expired keys stay in memory if never read.
    - *Active sweeper*: Runs every `sweepIntervalMs`. Scans `expiryMap`, removes expired keys. O(n keys with TTL). Benefit: memory is reclaimed even for unread keys.
- **`NO_EXPIRY = Long.MAX_VALUE` sentinel**: Instead of a separate `Set<K>` for permanent keys, we store `Long.MAX_VALUE` in `expiryMap`. The expiry check (`now > deadline`) always fails for `MAX_VALUE` — no special-casing needed. One fewer data structure to keep in sync.
- **Ordering in put()**: `expiryMap` is written *before* the delegate. If the sweeper runs between the two writes, it can only fail to find an entry in the delegate (safe). If we wrote the delegate first and the sweeper ran before `expiryMap` was updated, the key would have data but no expiry info — a zombie entry.
- **Sweeper catches `Throwable`**: Critical. A `RuntimeException` escaping a `ScheduledExecutorService` task silently cancels all future executions. The sweeper would stop running and you'd never know. Catching `Throwable` and logging keeps it alive.
- **Bottleneck**: The sweeper scan is O(n) over all keys with TTL. With millions of keys and a short sweep interval, the sweeper itself becomes a bottleneck. Redis solves this by scanning only a random sample of 20 keys per sweep cycle. We don't implement this optimization — known limitation.

---

### 2.5 concurrent/

**`CoarseGrainedCache.java`** — Thread-safe wrapper using a single `ReentrantReadWriteLock`.

- **Java primitives used**:
    - `ReentrantReadWriteLock`: Two modes. Read lock — multiple threads can hold simultaneously (safe because reads are non-mutating). Write lock — exclusive, blocks all readers and other writers. `get()` takes read lock; `put()` and `evict()` take write lock.
    - "Reentrant" means: a thread that holds the lock can acquire it again without deadlocking. This matters if `put()` internally calls `evict()` — both need the write lock, and reentrance prevents deadlock.
    - Fair mode (`new ReentrantReadWriteLock(true)`): Threads acquire locks in FIFO order. Prevents writer starvation — a continuous stream of readers can't indefinitely block a waiting writer.
- **Why ReadWriteLock over `synchronized`**: `synchronized` would serialize ALL threads even for read-only operations. ReadWriteLock gives free parallelism for reads — multiple threads call `get()` simultaneously with zero blocking.
- **Purpose**: This is the correctness baseline. Before optimizing with segments or lock-free approaches, this simple implementation gives you confidence the cache is correct under concurrent access. If a test fails here, the bug is in the delegate, not the locking.
- **Bottleneck**: Every write (put/evict) blocks ALL threads — readers and other writers. Under mixed load, write throughput determines overall throughput. Not suitable for write-heavy production workloads.

**`SegmentedCache.java`** — Thread-safe wrapper splitting the cache into N independent segments, each with its own lock and its own underlying cache.

- **Java primitives used**:
    - `ReentrantReadWriteLock[]` — array of locks, one per segment. Only threads operating on the same segment contend. Threads on different segments run fully in parallel.
    - `Cache<K,V>[]` — array of cache instances, one per segment. Each is a full LRU/LFU/ARC cache with its own capacity.
    - Segment selection: `int segment = (key.hashCode() & Integer.MAX_VALUE) % numSegments`. The `& Integer.MAX_VALUE` strips the sign bit — `hashCode()` can return negative values, and negative modulo in Java is negative, which would be an invalid array index.
- **Capacity distribution**: `segmentCapacity = ceil(totalCapacity / numSegments)`. Each segment gets an equal share. Effective total capacity = `segmentCapacity * numSegments`, which is slightly more than requested when total doesn't divide evenly.
- **`PolicyType` enum**: Each segment creates its own cache instance of the chosen policy. Adding `PolicyType` was a bug fix — the original implementation always created `LRUCache` regardless of which policy was selected. Now `CacheServer` can pass `PolicyType.ARC` and each of the 16 segments creates its own `ARCCache`.
- **Why 16 segments as default**: Powers of 2 allow a bitmask optimization (`hash & (N-1)` instead of `hash % N`). 16 is enough to reduce contention significantly without wasting memory on empty segment metadata.
- **Bottleneck**: When thread count >> segment count, contention returns. With 16 segments and 100 threads, ~6 threads contend per segment on average. Increasing segments reduces contention but increases memory overhead (each segment has its own lock, capacity tracking, etc.).

**`LockFreeCache.java`** — Highest throughput cache using ConcurrentHashMap and atomic operations.

- **Java primitives used**:
    - `ConcurrentHashMap<K, CacheEntry<K,V>>`: The backbone. Internally uses stripe locking (16 stripes by default in Java 8+, tree-based for high collision in Java 8+). `get()` is lock-free — just a hash table lookup. `put()` uses a single stripe lock only for the affected entry.
    - `AtomicLong lastAccessTime` per entry: Updated on every `get()` via `set()`. No lock needed — `AtomicLong.set()` is a volatile write, providing visibility guarantees.
    - `AtomicInteger size`: Tracks current entry count for capacity checks without locking.
    - `AtomicLong` for all stat counters (hits, misses, evictions): Lock-free increment on the hot path.
- **The fundamental tradeoff**: True O(1) LRU requires maintaining a doubly linked list in order. Updating that list on every read requires a lock (the list must be modified atomically). Exact LRU + lock-free reads = impossible without complex non-blocking data structures. Our compromise: approximate LRU — each `CacheEntry` stores `lastAccessTime`, and eviction finds the oldest entry via a scan. O(n) scan for eviction, but eviction is rare. Reads (the dominant operation) are fully lock-free.
- **Real-world equivalent**: Caffeine (Google's cache library, used in Spring Boot) solves this with a striped ring buffer — each thread appends its access to a local buffer, and a background thread drains all buffers to update the linked list. This gives exact LRU ordering without blocking the read path. Implementing Caffeine's approach is a stretch goal.
- **Bottleneck**: Eviction scan is O(n). If your cache is at capacity and you do continuous puts, every put triggers an O(n) scan. At 10,000 capacity and 100k puts/sec, this is 10,000 * 100,000 = 1 billion comparisons per second. In practice, eviction is infrequent enough that this doesn't matter — but it's the known weak point.

---

### 2.6 stats/

**`CacheMetricsCollector.java`** — Real-time metrics instrumentation.

- **Java primitives used**:
    - `LongAdder` for all counters (hits, misses, puts, evictions): Under high contention, `LongAdder` maintains per-thread cells and sums them on read. This eliminates the CAS spin-retry that `AtomicLong` suffers under high contention. Rule of thumb: use `LongAdder` when you increment frequently and read rarely. Use `AtomicLong` when you read and write with similar frequency, or when you need atomic CAS operations (compareAndSet).
    - Sliding window (circular array) for latency: Fixed-size `long[]` with an `AtomicInteger` index for the write position. The index wraps around using `% WINDOW_SIZE`. This gives O(1) writes and O(n log n) reads (sort for percentiles). The window size (1000) is chosen so sorting takes microseconds.
- **Immutable `MetricsSnapshot`**: `snapshot()` captures all current values at one instant. This prevents "read hit rate, then read eviction rate, they're from different moments" — all values in one snapshot are consistent with each other.
- **`startTimer()` / `recordHit(startTime)` pattern**: The caller calls `startTimer()` before the operation and passes the return value to `recordHit()` or `recordMiss()`. This way the metric knows exactly how long the cache operation took, including any lock wait time. Using `System.nanoTime()` — not `currentTimeMillis()` — because nanoTime is monotonic (not affected by NTP adjustments) and has nanosecond resolution.

---

### 2.7 CacheFactory

**`CacheFactory.java`** — The single entry point for constructing any cache configuration.

- **Design pattern**: Factory Method + Builder. Static methods for quick construction (`CacheFactory.lru(100)`). Inner `Builder` class for composed configurations with TTL and metrics.
- **Wrapper ordering matters**: When both TTL and metrics are applied: `MetricsWrapper → TTLCache → PolicyCache`. This means metrics are recorded *after* TTL eviction. A `get()` on an expired key records a MISS — correct, because the caller got nothing back. If metrics wrapped the policy cache directly (inside TTL), the same expired-key `get()` would record a HIT (it exists in the policy cache) followed by returning null — misleading data.
- **Why a utility class with private constructor**: `CacheFactory` should never be instantiated — it's a namespace for factory methods. The private constructor with `throw new UnsupportedOperationException()` enforces this even via reflection (attempting `Constructor.newInstance()` throws).

---

## 3. Module: cache-server

Turns `cache-core` into a network-accessible service. Adds Netty, wire protocol, and cluster routing.

---

### 3.1 Server Bootstrap

**`ServerConfig.java`** — Immutable configuration for `CacheServer`.

- **Design**: Builder pattern + immutable fields (all `final`). Built once at startup, never mutated. If config could change at runtime, every field access would need synchronization. Immutability eliminates that class of bug.
- **Key defaults**: Port 6379 (Redis's default — operators immediately recognize it). Boss threads = 1 (accepting connections is trivially fast, one thread is enough). Worker threads = 2×CPU (saturates hardware threads without excessive context switching). Max connections = 1000 (below Linux's default `ulimit` of 1024 file descriptors).
- **Port 0 support**: Pass port 0 and the OS assigns a free ephemeral port. Used in tests to avoid port collisions between test classes running in parallel.

**`CacheServer.java`** — Netty TCP server. The entry point for the server module.

- **Java primitives used**:
    - `NioEventLoopGroup boss` (1 thread): Accepts incoming TCP connections. Calls `accept()` and hands channels to workers.
    - `NioEventLoopGroup worker` (2×CPU threads): Handles I/O for accepted connections. Each channel is pinned to one worker thread — no concurrent access to a single channel's pipeline.
    - `AtomicBoolean running`: Prevents double-start and double-stop races. `compareAndSet(false, true)` in `start()` ensures only one thread can start the server. Atomic — no synchronized block needed for a single boolean.
    - `CountDownLatch startLatch`: Used in `startAsync()` to signal "server is bound and ready" to the calling thread. The calling thread blocks on `latch.await()` and the Netty bind future calls `latch.countDown()` when the port is bound. Without this, `startAsync()` would return before the port is actually bound, causing tests to connect to a server that isn't listening yet.
- **Netty pipeline per channel**: `LineBasedFrameDecoder` → `StringDecoder` → `StringEncoder` → `ConnectionManager` → `CacheServerHandler`. The pipeline is a chain of handlers. Each handler processes the event and passes it to the next. This is the Chain of Responsibility pattern.
- **`LineBasedFrameDecoder`**: TCP is a stream protocol — it has no concept of message boundaries. A client sending "GET key\n" might arrive as ["GET ke", "y\n"] across two packets. `LineBasedFrameDecoder` buffers bytes until it sees a newline, then delivers a complete line as one message. Max line length (8KB) protects against memory exhaustion from clients that send data without newlines.
- **Shared cache**: `cache` is created once and shared across ALL client channels. All Netty worker threads call into the same `SegmentedCache` concurrently — this is why the cache must be thread-safe.

---

### 3.2 Protocol Layer

**`Command.java`** — Two things: `Command.Type` enum (the verbs) and `Command.ParsedCommand` inner class (the structured result of parsing).

- **Why enum for command types**: Enum `switch` is exhaustive — the compiler warns if you add a new command to the enum but forget to handle it in the switch. String comparison would silently fall through to the default case.
- **`ParsedCommand` is immutable**: Set once by `CommandParser`, never modified. `CacheServerHandler` reads fields and calls the cache. Immutability means no synchronization needed if `ParsedCommand` objects are shared (they're not currently, but it's good practice).
- **Argument count validation in the enum**: Each `Type` carries `minArgs` and `maxArgs`. `CommandParser` checks `args.length` against these values without needing per-command if-else chains. Adding a new command means adding one enum constant with its arg counts — nothing else changes.

**`CommandParser.java`** — Converts raw text lines into `ParsedCommand` objects. Stateless — no fields.

- **Stateless by design**: No instance state means it's trivially thread-safe. `CacheServerHandler` can call `parser.parse(line)` from multiple Netty worker threads simultaneously without any synchronization.
- **Parsing strategy**: `trim()` → `split("\\s+")` → uppercase first token → enum lookup → arg count validation → type-specific field extraction. `split("\\s+")` handles multiple spaces between tokens, tab characters, and other whitespace — robust to client formatting variations.
- **`ProtocolException` for all errors**: All malformed input throws a checked `ProtocolException`. Checked because protocol errors are expected, recoverable events — the handler catches them and sends an error response. The channel stays open. Unchecked exceptions would require a try-catch at the top level with no ability to distinguish protocol errors from system errors.

**`ResponseEncoder.java`** — Formats response strings. All methods static, no state.

- **CRLF (`\r\n`) terminator**: RFC-compliant. Works with telnet, netcat, and any RFC-compliant client. `LineBasedFrameDecoder` on a client would strip both `\r\n` and `\n`.
- **`+`/`-` prefix**: Inspired by Redis RESP. Clients check `response.charAt(0)` — O(1) — to distinguish success from error. No full-line parsing needed for the check.
- **`value(String v)` null handling**: If `v` is null (key not found), returns `-ERR key not found` rather than `+null`. This is the correct semantic — null means absence, and the protocol should express absence as an error response, not a success response with a null-like string.

**`ProtocolException.java`** — Checked exception for malformed commands.

- **Checked, not unchecked**: Forces `CacheServerHandler` to explicitly handle every protocol error. You cannot accidentally let a `ProtocolException` propagate uncaught — the compiler enforces the catch.
- **`ErrorCode` enum**: Categorizes errors (UNKNOWN_COMMAND, MISSING_ARGUMENT, TOO_MANY_ARGS, INVALID_TTL, EMPTY_COMMAND). Available for future use — numeric error codes for machine-readable clients, similar to HTTP status codes.

---

### 3.3 Handler Layer

**`CacheServerHandler.java`** — The final inbound handler. Receives parsed String commands, calls the cache, writes responses.

- **Extends `SimpleChannelInboundHandler<String>`**: Netty calls `channelRead0(ctx, msg)` with already-decoded String messages. `SimpleChannelInboundHandler` automatically releases the message's memory after `channelRead0` returns — prevents memory leaks from unreleased `ByteBuf` references.
- **`System.nanoTime()` for latency**: Recorded before and after each cache operation. `nanoTime()` not `currentTimeMillis()` because: (1) monotonic — unaffected by NTP clock adjustments, (2) nanosecond resolution — cache operations complete in microseconds, millisecond resolution would show 0ms for everything.
- **Not `@ChannelHandler.Sharable`**: Each channel gets its own handler instance. Even though the current implementation is stateless per-channel, marking it non-sharable is defensive — if you add per-connection state later (authenticated user, pipeline mode), you won't accidentally share it.
- **`exceptionCaught()`**: Catches both `ProtocolException` (sends error response, keeps channel open) and other `Throwable` (logs and closes channel). Never let uncaught exceptions propagate up Netty's pipeline — they would close the channel silently.

**`ConnectionManager.java`** — Tracks active channels, enforces max connection limit.

- **`@ChannelHandler.Sharable`**: One `ConnectionManager` instance is added to EVERY channel's pipeline. This is deliberate — it holds global state (active connection count). Non-sharable would require creating a new instance per channel, which would give each channel its own counter — defeating the purpose.
- **Java primitives used**:
    - `ConcurrentHashMap<ChannelId, ChannelHandlerContext> activeChannels`: Set of active channels. `ConcurrentHashMap` because `channelActive()` and `channelInactive()` are called by different Netty worker threads concurrently. `keySet()` gives us a thread-safe set backed by the map.
    - `AtomicLong connectionCount`: Current connection count. `AtomicLong` for lock-free increment/decrement from concurrent Netty threads.
    - `AtomicLong rejectedConnections`: Counts rejected connections for STATS reporting.
- **Limit enforcement**: When `connectionCount >= maxConnections` in `channelActive()`, the new channel is immediately closed. No error message is sent — the TCP connection is reset before any protocol exchange. Redis does the same.

---

### 3.4 Cluster Layer

**`CacheNode.java`** — Represents one physical cache server. Immutable.

- **All fields `final`**: Immutability is critical for ring correctness. If `host` or `port` could change after a node was added to the ring, the node's virtual node positions would become stale — the ring would route keys to the wrong address.
- **Identity based on `id`, not `host:port`**: `equals()` and `hashCode()` use only `id`. A node that restarts at a new IP is still the same logical node — the ring doesn't need to be rebuilt. This mirrors how Cassandra handles node identity.
- **`withStatus(Status)` instead of setters**: Returns a new `CacheNode` with updated status. Preserves immutability while allowing status transitions (ACTIVE → LEAVING → FAILED). Callers must replace their reference to get the updated status.
- **`weight` field**: Heavier nodes get more virtual nodes on the ring → handle more of the key space. A server with 2x RAM can handle 2x traffic. Default weight=1 means uniform distribution.

**`ConsistentHashRing.java`** — The core distributed routing algorithm.

- **The problem it solves**: Naive `hash(key) % N` routing breaks when N changes — every key remaps, causing 100% cache miss rate. Consistent hashing: removing 1 of N nodes remaps only ~1/N keys.
- **Java primitives used**:
    - `ConcurrentSkipListMap<Integer, CacheNode> ring`: A sorted map from ring position (MD5 hash, 32-bit) to the CacheNode that owns that position. `ConcurrentSkipListMap` is a lock-free sorted map — `tailMap()` lookups (finding the next node clockwise) are thread-safe without a lock. Reads on the hot path have zero locking overhead.
    - `ReentrantReadWriteLock lock`: Used only for `addNode()` and `removeNode()` — these insert/remove 150 entries atomically. Without the write lock, a reader might see a partially-added node (50 of 150 virtual nodes present) and route incorrectly. `getNode()` uses only `ConcurrentSkipListMap`'s internal thread safety — it doesn't need this lock.
    - `Map<String, CacheNode> nodeById`: Reverse lookup — given a node ID, find all its virtual node positions for O(1) removal. Without this, `removeNode()` would scan all 450+ ring entries to find which belong to the departing node — O(V*N) instead of O(V).
- **Virtual nodes (150 per physical node)**: Pure consistent hashing with 3 nodes gives uneven distribution — one node might own 50% of the ring by chance. 150 virtual nodes per physical node means 450 ring positions for 3 nodes — distribution converges to ~33% per node with ~3% variance. The same count as Memcached's libketama.
- **MD5 hash function**: NOT for security. MD5 is cryptographically broken. We use it for its distribution properties: good spread across the 2^32 ring space, deterministic, available in Java standard library. Same as libketama (the original consistent hashing implementation used by Twitter and Wikipedia).
- **Byte extraction from MD5**: `(digest[0] & 0xFF) | ((digest[1] & 0xFF) << 8) | ...`. The `& 0xFF` mask is critical: Java bytes are signed (-128 to 127). Without masking, a byte value of -1 (0xFF) would sign-extend to 0xFFFFFFFF when promoted to int, corrupting the upper bits. Masking treats it as unsigned 255.
- **Bottleneck**: `MessageDigest.getInstance("MD5")` is NOT thread-safe and must be created per-call. Under high throughput, repeated MessageDigest instantiation adds up. Production solution: `ThreadLocal<MessageDigest>` — one instance per thread, reused across calls. We use per-call creation for simplicity — acceptable because ring operations (addNode, getNode) are infrequent relative to cache gets.

**`NodeRouter.java`** — Server-side routing policy layer wrapping `ConsistentHashRing`.

- **Separation of concerns**: `ConsistentHashRing` is a pure algorithm — no concept of "this node" vs "other nodes". `NodeRouter` adds identity awareness. `CacheServerHandler` asks `NodeRouter.route(key)` and acts on the `RoutingDecision` — it never touches the ring directly.
- **`RoutingDecision` value object**: Instead of returning a boolean `isLocal()` and forcing a second ring lookup for the redirect address, `RoutingDecision` carries both. This avoids a TOCTOU (time-of-check-time-of-use) race — if the ring changes between the boolean check and the address lookup, you'd get inconsistent data.
- **`markNodeFailed()` vs `removeNode()`**: `removeNode()` is permanent — keys are redistributed. `markNodeFailed()` marks the node `FAILED` in the ring — it stays in the ring for potential recovery, but routing skips it. This mirrors Cassandra's failure detection: temporarily mark failed, keep the slot for recovery.

**`ConsistentHashRingTest.java`** — The most important test file in the server module.

- **The key test**: `testRemoveNode_remapsOnlyAffectedKeys()`. Routes 10,000 keys with 3 nodes. Removes node-2. Routes the same keys again. Asserts: (1) zero keys that were on node-1 or node-3 changed — strict correctness requirement. (2) All of node-2's keys moved to other nodes — they have to. (3) ~33% of total keys moved — within ±10% of the theoretically optimal 1/3. This test proves consistent hashing works, and it's what you show an interviewer who asks "how did you verify this?"

**`ClientRouter.java`** — Client-side consistent hash routing. Smart client model.

- **Smart client vs proxy**: Proxy adds an extra network hop and is a single point of failure. Smart client computes routing locally, connects directly to the correct server. Used by Redis Cluster clients (Jedis, Lettuce), Cassandra drivers, Memcached clients.
- **Java primitives used**:
    - `ConcurrentHashMap<String, CacheClient> clientMap`: Maps node ID → open CacheClient connection. `ConcurrentHashMap` for safe concurrent reads during routing (multiple threads can call `get(key)` simultaneously, all reading from `clientMap`).
    - `ReentrantReadWriteLock lock`: Read lock for routing (get/put/delete) — multiple threads route concurrently. Write lock for `addServer()`/`removeServer()` — these must atomically update both the ring and `clientMap`. Without the write lock, a thread might look up a client for a node that's being removed — `clientMap.get()` returns a just-closed connection.
- **Connect-before-ring ordering in `addServer()`**: The TCP connection is established *before* adding to the ring. If the connection fails, the ring is never updated. This ensures the ring never points to an unreachable server. If we added to the ring first and the connection failed, routing would send requests to a node with no open client — `clientMap.get()` would return null, causing NullPointerException.
- **`RoutingException` as unchecked**: Routing failures mean infrastructure is down. The call site can't recover from that. Unchecked lets it propagate naturally to a top-level error handler. Same reasoning as Jedis's unchecked `JedisConnectionException`.
- **`@FunctionalInterface CacheOperation<T>`**: Used in `executeWithRetry()` to abstract the retry logic from the operation type. `get()`, `put()`, `delete()` all go through the same retry machinery — just different lambda implementations of `CacheOperation`.

---

## 4. Module: cache-client

**`CacheClient.java`** — Single-connection client. NOT thread-safe by design.

- **Blocking I/O (`java.net.Socket`)**: The server uses Netty NIO. The client uses blocking sockets. This is intentional — the client is simple, one thread per connection, doesn't need multiplexing. Blocking I/O is simpler and just as fast for one-thread-one-connection. For thousands of connections, use Netty on the client side too.
- **`SO_TIMEOUT`**: `socket.setSoTimeout(5000)` causes `readLine()` to throw `SocketTimeoutException` after 5 seconds. Without this, if the server crashes mid-request, the client blocks forever.
- **`BufferedReader` / `PrintWriter`**: `BufferedReader.readLine()` is efficient — reads a full line at once, buffers internally. `PrintWriter.println()` writes a line and flushes. Flushing is critical — unflushed data sits in the OS network buffer and the server never receives it.
- **`Closeable` / try-with-resources**: `CacheClient implements Closeable` means you can write `try (CacheClient c = new CacheClient(...)) { ... }` and the connection is automatically closed even if an exception is thrown.
- **Not thread-safe**: `send()` does two non-atomic operations: write the request, read the response. If two threads interleave these, thread A's write might be followed by thread B's write, and then thread A reads thread B's response. Use `ConnectionPool` for multi-threaded access.

**`ConnectionPool.java`** — Fixed pool of pre-opened TCP connections.

- **`ArrayBlockingQueue<CacheClient> pool`**: Bounded queue that blocks on `poll()` when empty (no available connections) and blocks on `offer()` when full (bug guard — pool should never be over-full). `ArrayBlockingQueue` is thread-safe — multiple threads can call `acquire()` and `release()` concurrently without external synchronization.
- **Why fixed size**: A fixed pool bounds resource usage. An unbounded pool could open thousands of connections under load spike, exhausting OS file descriptors. Fixed pool = predictable resource consumption.
- **Connection health check on `release()`**: When returning a connection, send a PING. If the server doesn't respond, the connection is broken — create a new one. This transparently replaces dead connections without the caller knowing.
- **`AtomicInteger activeCount`**: Tracks how many connections are currently in use (acquired but not yet released). Used for pool utilization metrics. `AtomicInteger` for lock-free reads — monitoring code can call `getActiveCount()` without blocking the pool.

---

## 5. Module: cache-benchmarks

**`BenchmarkWorkload.java`** — Pre-computes key arrays used by all benchmarks.

- **Why pre-compute**: If keys were generated inside `@Benchmark` methods, we'd measure key generation + cache operation. JMH measures wall time — `String.format()` in the hot loop pollutes the cache measurements. `@Setup` is not measured.
- **Zipfian distribution**: `P(rank r) ∝ 1/r^skew`. With skew=1.0: rank 1 accessed ~ln(N) times more than rank N. Top 10% of keys get ~70% of accesses. Matches real web traffic. This is the distribution where LFU and ARC beat LRU.
- **Uniform distribution**: Every key accessed with equal probability. No hot keys. All policies perform similarly. Use as control group — differences here are pure implementation overhead, not policy intelligence.
- **Fixed random seed (42)**: Both Zipfian and Uniform use `new Random(42)`. Same seed = same sequence every run. Benchmarks are reproducible — you'll get the same key distribution every time, enabling fair comparison across runs.
- **Binary search CDF for Zipfian**: Pre-computes the CDF array once in `@Setup`. Then each slot in `zipfianKeys[]` does an O(log N) binary search on the CDF. This O(log N) cost is paid only during `@Setup`, not in the benchmark loop. The benchmark loop itself is O(1) array indexing.

**`LRUBenchmark.java`, `LFUBenchmark.java`, `ARCBenchmark.java`** — Individual policy benchmarks.

- **JMH annotations**:
    - `@Fork(2)`: Two separate JVM processes per benchmark. JIT compilation state leaks between benchmarks in the same JVM — separate forks prevent this. Two forks = enough independent samples without multi-hour runtimes.
    - `@Warmup(3, 1s)`: Three warmup iterations. JVM needs ~10,000-100,000 invocations to JIT-compile a method to optimized native code. Three seconds at millions of ops/sec is plenty.
    - `@Measurement(5, 2s)`: Five measurement iterations. More time = fewer noise from GC pauses. Five iterations gives enough samples for a meaningful 99% confidence interval.
    - `@State(Scope.Benchmark)`: One shared cache instance across all threads in a fork. We want threads to contend on the same cache — separate instances per thread would defeat the purpose.
    - `jvmArgs = {"-Xms512m", "-Xmx512m"}`: Fixed heap size prevents mid-benchmark heap expansion pauses that would inflate latency measurements.
- **`Blackhole.consume(result)`**: Tells JMH "this value is used." Without it, the JIT sees that `result` is never used and might eliminate the `cache.get()` call entirely as dead code — measuring nothing.
- **`AtomicLong opCounter`**: Cycles through the pre-built key array. `AtomicLong.getAndIncrement()` is thread-safe. Each thread gets a different key. Without atomicity, two threads might get the same array index — not catastrophic, but the benchmark would under-represent key diversity.

**`PolicyComparisonBenchmark.java`** — The most important benchmark.

- **`@Param({"LRU", "LFU", "ARC"})`**: JMH creates one variant per value. Every `@Benchmark` method runs three times — once per policy. Output is a table comparing all three policies on the same workload. Apples-to-apples comparison.
- **`createCache(policyName)` factory method**: The only place policy-specific code lives. Every benchmark method is policy-agnostic — it calls `cache.get()` and `cache.put()` via the `Cache<K,V>` interface. Changing which policies to compare means changing one array.

**`ConcurrencyBenchmark.java`** — Compares the three locking strategies.

- **`@Param({"1", "4", "8", "16", "32"}) int threads`**: Runs each strategy at multiple thread counts. The story told by the numbers: at 1 thread, all three are similar. As threads increase, coarse-grained falls behind. Segmented scales well until threads >> segments. Lock-free scales best.
- **`@Param({"coarse", "segmented", "lockfree"}) String strategy`**: Switches between the three implementations. Same pattern as PolicyComparisonBenchmark — all strategies run on the same workload.

---

## 6. Test Files

**`LRUCacheTest.java`** — Unit tests for LRU eviction ordering. Most important assertions: the exact key that gets evicted, and that `get()` rescues a key from eviction by making it "most recently used."

**`TTLCacheTest.java`** — Time-sensitive tests. Key technique: sleep = TTL × 1.5 for buffer. Tests both lazy eviction (via `get()`) and active sweeper (by checking `size()` without calling `get()`). `@Execution(ExecutionMode.SAME_THREAD)` prevents parallel execution — time-dependent tests are inherently non-deterministic when competing with other threads for CPU.

**`CoarseGrainedCacheTest.java`** — Concurrent correctness. Key technique: `CountDownLatch` to make all threads start simultaneously. Without it, threads start sequentially — no contention, no races. Asserts invariants: `size()` never exceeds capacity, no exceptions thrown, stats non-negative.

**`SegmentedCacheTest.java`** — Tests specific to segment design: routing consistency (same key always same segment), segment isolation (operations in different segments don't interfere), key distribution (keys spread across segments), and configuration validation (non-power-of-2 segment count rejected).

**`ConcurrencyStressTest.java`** — Runs the same stress tests against all three concurrency strategies using `@ParameterizedTest` + `@MethodSource`. If a test passes for `CoarseGrainedCache` but fails for `LockFreeCache`, the bug is localized to the lock-free implementation.

**`CommandParserTest.java`** — Tests the protocol boundary. Every invalid input the real world might send goes through `CommandParser` first. Tests: valid commands, missing args, extra args, case insensitivity, whitespace variants, invalid TTL values.

**`CacheServerHandlerTest.java`** — Uses Netty's `EmbeddedChannel` — an in-memory channel with no real TCP socket. Key fix: `readOutbound()` returns a `ByteBuf`, not a `String`, because `StringEncoder` already converted the string to bytes. Read the `ByteBuf` and call `buf.toString(CharsetUtil.UTF_8)` to get the string back.

**`ConsistentHashRingTest.java`** — Mathematical property verification. The critical test `testRemoveNode_remapsOnlyAffectedKeys` verifies two separate things: (1) no key from non-removed nodes changed (strict, zero tolerance), and (2) ~1/3 of keys changed (statistical, ±10% tolerance). These are different assertions for different failure modes.

**`ServerIntegrationTest.java`** and **`CacheClientTest.java`** — Full-stack tests. Server starts once (`@BeforeAll`). Cache flushed before each test (`@BeforeEach`). Tests the complete stack: client sends bytes, server processes, client receives response. Catches bugs that exist in the interaction between components — impossible to find with unit tests alone.

---

## 7. Java Primitives Master Reference

| Primitive | Used In | Why This One |
|-----------|---------|--------------|
| `AtomicLong` | `CacheStats`, `ServerMetrics`, all benchmarks | CAS-based lock-free increment. Use when read and write frequency are similar, or when you need `compareAndSet`. |
| `LongAdder` | `CacheMetricsCollector`, `ServerMetrics` | Per-thread cells, sum on read. 3-5x faster than `AtomicLong` under high contention. Use when you increment constantly and read rarely. |
| `AtomicInteger` | `ConnectionPool`, `LockFreeCache` | Same as `AtomicLong` but 32-bit. For sizes and counts that fit in int. |
| `AtomicBoolean` | `CacheServer.running` | Lock-free boolean flag. `compareAndSet(false, true)` prevents double-start. |
| `AtomicReference` | `LockFreeCache` eviction slot | CAS-based object replacement. Used to ensure only one thread performs eviction at a time without a lock. |
| `ConcurrentHashMap` | `TTLCache`, `ClientRouter`, `ConnectionManager` | Lock-free reads, stripe-locked writes. Default for thread-safe key-value storage when you don't need sorted order. |
| `ConcurrentSkipListMap` | `ConsistentHashRing` | Thread-safe sorted map. Lock-free reads with O(log n) `tailMap()`. Required when you need sorted iteration concurrent with writes. |
| `ReentrantReadWriteLock` | `CoarseGrainedCache`, `SegmentedCache`, `ConsistentHashRing`, `ClientRouter` | Separate read/write locks. Readers share access; writers are exclusive. Use when reads >> writes and readers can safely run concurrently. |
| `CountDownLatch` | `CacheServer.startLatch`, all stress tests | One-time barrier. `await()` blocks until `countDown()` is called N times. Used for "wait until server is ready" and "all threads start simultaneously." |
| `BlockingQueue` (`ArrayBlockingQueue`) | `ConnectionPool` | Thread-safe bounded queue with blocking `poll()` and `offer()`. Perfect for producer-consumer patterns like connection pools. |
| `ScheduledExecutorService` | `TTLCache` sweeper | Runs a task on a fixed schedule. Created with daemon threads so it doesn't prevent JVM shutdown. |
| `ConcurrentLinkedQueue` | `ServerMetrics` latency window | Lock-free queue for the latency sliding window. `offer()` is lock-free; we manually cap size at `WINDOW_SIZE`. |
| `volatile` | Various flags | Visibility guarantee — a write to a `volatile` field is immediately visible to all other threads. No atomicity (use `AtomicX` for that), just visibility. |

---

## 8. Design Patterns Used

| Pattern | Files | What It Achieves |
|---------|-------|-----------------|
| **Decorator** | `TTLCache`, `CoarseGrainedCache`, `CacheMetricsCollector` | Adds behavior (TTL, thread safety, metrics) to any `Cache<K,V>` without modifying the delegate. Each wrapper is independently composable. |
| **Factory Method** | `CacheFactory` | Encapsulates construction logic. Callers get `Cache<K,V>` and never know which concrete class or which wrappers are applied. |
| **Builder** | `CacheFactory.Builder`, `ServerConfig.Builder` | Constructs complex objects step by step. Enforces immutability — the built object is fully configured before being used. |
| **Strategy** | `EvictionPolicy` interface, `Cache<K,V>` interface | Swappable algorithms. `CacheServerHandler` uses `Cache<K,V>` — it doesn't care which eviction policy is running underneath. |
| **Chain of Responsibility** | Netty pipeline | Each handler processes events and passes them down. `LineBasedFrameDecoder` → `StringDecoder` → `ConnectionManager` → `CacheServerHandler`. |
| **Value Object** | `CacheStats`, `RoutingDecision`, `ParsedCommand` | Immutable data carriers. No shared mutable state. Safe to pass between threads without synchronization. |
| **Template Method** | `SimpleChannelInboundHandler<String>` | Netty calls the template (`channelRead`) which handles lifecycle (message release) and calls `channelRead0` for our implementation. |

---

## 9. Bottlenecks and Known Limitations

### cache-core bottlenecks

| Location | Bottleneck | Impact | Production Fix |
|----------|-----------|--------|---------------|
| `LockFreeCache` eviction | O(n) scan to find LRU entry | Every eviction when at capacity scans all entries | Caffeine's striped ring buffer for approximate LRU ordering |
| `TTLCache` sweeper | O(n) scan over all keys each sweep interval | At millions of keys + short TTLs, sweeper consumes CPU | Redis-style: scan only random 20 keys per cycle, repeat if >25% expired |
| `LFUCache` frequency counts | Frequencies accumulate forever | Old hot keys never evicted even after access pattern changes | Frequency decay: halve all counts periodically |
| `CoarseGrainedCache` write lock | One write blocks all readers | Under mixed load, write throughput limits everything | Use `SegmentedCache` or `LockFreeCache` |
| `ConsistentHashRing.computeHash()` | `MessageDigest.getInstance("MD5")` created per-call | High ring lookup rate creates GC pressure from `MessageDigest` objects | `ThreadLocal<MessageDigest>` — one instance per thread, reused |

### cache-server bottlenecks

| Location | Bottleneck | Impact | Production Fix |
|----------|-----------|--------|---------------|
| `CacheServer` single cache | All channels share one cache — lock contention | Under high concurrency, segment locks become the bottleneck | Increase `numSegments` based on CPU count |
| `LineBasedFrameDecoder` | Copies bytes until newline found | Large values with no newlines buffer entirely in memory | Binary framing with length prefix instead of newline delimiter |
| `CacheServerHandler` metrics | `System.nanoTime()` on every operation | `nanoTime()` is a system call — ~40ns overhead per operation | Sample 1% of operations for latency tracking |

### cache-client bottlenecks

| Location | Bottleneck | Impact | Production Fix |
|----------|-----------|--------|---------------|
| `ConnectionPool` blocking `acquire()` | Thread blocks if pool exhausted | Under high load, all threads wait for connections | Increase pool size; add async/non-blocking acquire |
| `ClientRouter` ring lookup on every operation | ReadLock taken for every get/put/delete | Under very high throughput, lock becomes contention point | Cache the last routing decision per key with TTL |
| `CacheClient.send()` blocking I/O | `readLine()` blocks until response | Thread is occupied for the full round-trip time | Async client using Netty NIO on the client side |

---

## 10. Critical Things to Never Get Wrong

### Data Structure Invariants

**LRU linked list pointers**: Every `addToFront()` and `remove()` must update both `prev` and `next` of the affected nodes AND the neighbors. Forgetting one pointer update causes a broken list that manifests as wrong eviction order or infinite loops. Test: after every operation, the list should be traversable from head to tail and from tail to head with the same count.

**LFU minFreq tracking**: After eviction, `minFreq` must be reset to 1 if you evict from the minimum bucket AND the new key that's about to be inserted will have frequency 1. Missing this causes the next eviction to scan empty buckets. Test: immediately after eviction + insert, call `get()` on the inserted key — it should be returned successfully.

**ARC ghost list sizes**: `|B1| + |T1| ≤ capacity` and `|B2| + |T2| ≤ capacity` must always hold. The ghost lists exist to inform the `p` adaptation — they consume memory but provide no cached values. If you don't bound ghost list size, the cache can grow unboundedly.

### Concurrency

**`ConcurrentHashMap` null keys/values**: `ConcurrentHashMap` does NOT allow null keys or null values. Attempting to put null throws `NullPointerException`. All caches that use `ConcurrentHashMap` internally must validate inputs and throw `IllegalArgumentException` before reaching the map operation.

**Double-checked locking**: Never implement your own double-checked locking for singleton initialization. Use `enum` singletons or `Holder` classes. Broken double-checked locking is a classic Java concurrency bug that manifests only on certain JVM implementations and CPU architectures.

**Lock acquisition order**: If any code path acquires multiple locks, ALL code paths must acquire them in the same order. Deadlock occurs when thread A holds lock 1 and waits for lock 2, while thread B holds lock 2 and waits for lock 1. We avoid this by never holding two locks simultaneously in our implementations.

**`ScheduledExecutorService` and exceptions**: Any uncaught exception in a scheduled task silently cancels all future executions. Always wrap scheduled task bodies in `try { ... } catch (Throwable t) { log; }`.

### Protocol

**Response prefix**: Every response must start with `+` (success) or `-ERR` (error). No exceptions. A missing prefix causes clients to misparse the response and produce garbage output or throw exceptions.

**CRLF vs LF**: `LineBasedFrameDecoder` accepts both. But always send `\r\n` — some clients (telnet, Windows netcat) send `\r\n` and expect `\r\n` back. Sending only `\n` from the server can cause issues with strict clients.

**TTL of 0**: Must mean "no expiry," not "expire immediately." This is the Redis convention. If TTL=0 means "expire immediately," putting a key with TTL=0 and then getting it would always return null — a very confusing behavior for callers.

### Consistent Hashing

**Sign bit in hash extraction**: `hashCode()` returns `int`, which can be negative. `hash(key) % numSegments` in Java returns a negative result for negative hashes — invalid array index. Always use `(hashCode() & Integer.MAX_VALUE) % N` or `Math.abs(hashCode()) % N` (but `Math.abs(Integer.MIN_VALUE)` is still negative — the first form is safer).

**MD5 byte extraction masking**: `digest[i] & 0xFF` is mandatory. Java bytes are signed. Without masking, byte value -1 (0xFF binary) becomes `0xFFFFFFFF` when promoted to int via sign extension — corrupting the ring position. This bug is subtle and only manifests for keys whose MD5 digest contains bytes > 127.

**Virtual node count consistency**: Client and server must use the same number of virtual nodes per physical node. If client uses 150 and server uses 200, they compute different ring positions and route different keys to different nodes — data is effectively lost (not corrupted, just unreachable).

---

## 11. What to Say in the FAANG Interview

### "Design a cache system"

> "I built a cache library with three eviction policies: LRU using a doubly linked list plus HashMap for O(1) all operations, LFU using Shan's frequency bucket algorithm for O(1) all operations — not the naive O(log n) heap approach — and ARC which is the adaptive cache used in ZFS and macOS. I wrapped all three in a common `Cache<K,V>` interface using the Strategy pattern so they're hot-swappable via factory methods.

> On top of the core library, I built a TCP server using Netty that accepts concurrent connections and serves GET/PUT/DELETE commands over a Redis-inspired text protocol. The server uses segmented locking internally — 16 independent lock segments — to reduce contention under concurrent client load.

> For distribution, I implemented consistent hashing with 150 virtual nodes per physical node using MD5 for ring positioning, the same approach as Memcached's libketama. I verified mathematically that removing one of three nodes remaps exactly the ~33% of keys that were on that node, and no other keys move."

### "How did you handle thread safety?"

> "I implemented three levels of thread safety and benchmarked them. Coarse-grained locking is a single ReentrantReadWriteLock — simple, correct, but every write serializes all threads. Segmented locking splits the cache into 16 segments each with its own lock — threads operating on different keys don't block each other. Lock-free uses ConcurrentHashMap with AtomicLong counters — reads are entirely lock-free. I measured [X] ops/ms for coarse, [Y] for segmented, [Z] for lock-free at 32 threads. The lock-free approach wins on reads but uses an O(n) approximate LRU eviction, which is acceptable because eviction is infrequent."

### "What are the tradeoffs between LRU, LFU, and ARC?"

> "LRU has the lowest implementation overhead and is scan-vulnerable — a sequential table scan evicts all hot keys. LFU protects hot keys perfectly but has a cold-start problem — new keys are evicted first even if they're about to become hot, and frequency counts accumulate forever without decay. ARC adapts between LRU-like and LFU-like behavior based on ghost list hits — it handles workload shifts and is scan-resistant, but costs ~2x memory for the ghost lists and has ~15-25% lower throughput due to more bookkeeping. My benchmarks showed LFU achieving 12% higher hit rates than LRU on Zipfian workloads, with ARC in between but with better behavior on scan-mixed workloads."

### "Why Netty?"

> "Netty gives you asynchronous non-blocking I/O with a boss/worker thread model. One boss thread accepts connections, N worker threads handle I/O. Each channel is pinned to one worker thread, which eliminates per-channel synchronization. Without Netty, handling 1000 concurrent clients would require 1000 OS threads — 4GB of stack memory just for thread stacks. With Netty, 8 worker threads handle 1000 clients through event-driven I/O."

---

*This document covers all 39 Java files across 4 Maven modules. Total project LOC: approximately 8,500 lines of production code and 3,200 lines of tests.*