# JCache — Architecture Deep Dive

This document explains how every component in JCache works, why each design decision was made, and how the pieces connect to each other. Read this before reading source code.

---

## Table of Contents

- [System Overview](#system-overview)
- [Layer 1 — Core Data Structures](#layer-1--core-data-structures)
- [Layer 2 — Eviction Policies](#layer-2--eviction-policies)
- [Layer 3 — TTL and Expiry](#layer-3--ttl-and-expiry)
- [Layer 4 — Thread Safety](#layer-4--thread-safety)
- [Layer 5 — The Network Server](#layer-5--the-network-server)
- [Layer 6 — Consistent Hashing and Clustering](#layer-6--consistent-hashing-and-clustering)
- [Layer 7 — The Client and Connection Pool](#layer-7--the-client-and-connection-pool)
- [Layer 8 — Metrics and Observability](#layer-8--metrics-and-observability)
- [Design Patterns Used](#design-patterns-used)
- [Data Flow — Request Lifecycle](#data-flow--request-lifecycle)
- [Memory Layout](#memory-layout)
- [Failure Modes and How They Are Handled](#failure-modes-and-how-they-are-handled)

---

## System Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        CLIENT APPLICATIONS                           │
│  Java App   Python Script   curl / telnet   Any TCP Client           │
└──────────────────┬───────────────────────────────────────────────────┘
                   │  TCP (port 6379)  plain-text protocol
                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    CACHE-SERVER MODULE                               │
│                                                                      │
│  CacheServer (Netty Bootstrap)                                       │
│    │                                                                 │
│    ├── NioEventLoopGroup (boss, 1 thread) ── accepts connections     │
│    └── NioEventLoopGroup (worker, N threads) ── handles I/O         │
│                                                                      │
│  Per-connection ChannelPipeline:                                     │
│    LineBasedFrameDecoder → StringDecoder → StringEncoder             │
│    → ConnectionManager → CacheServerHandler                          │
│                                   │                                  │
│                             calls cache.get/put/evict                │
│                             records ServerMetrics                    │
└──────────────────────────────────┬───────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      CACHE-CORE MODULE                               │
│                                                                      │
│  CacheFactory                                                        │
│    │                                                                 │
│    ├── LRUCache ──────────────────┐                                  │
│    ├── LFUCache ──────────────────┤── EvictionPolicy interface       │
│    └── ARCCache ──────────────────┘                                  │
│                                                                      │
│  Concurrency wrappers (Decorator pattern):                           │
│    CoarseGrainedCache (single RW lock)                               │
│    SegmentedCache     (16 independent locks)                         │
│    LockFreeCache      (ConcurrentHashMap + AtomicLong)               │
│                                                                      │
│  TTLCache (Decorator):                                               │
│    lazy eviction on get() + ScheduledExecutorService sweeper         │
│                                                                      │
│  Core data structures:                                               │
│    Node<K,V> ← doubly linked list node                              │
│    DoublyLinkedList<K,V> ← O(1) addToFront, remove, removeLast     │
│    HashMap<K, Node<K,V>> ← O(1) lookup by key                       │
└──────────────────────────────────────────────────────────────────────┘

┌─────────────────────┐         ┌─────────────────────────────────────┐
│  CACHE-CLIENT       │         │  CACHE-BENCHMARKS                   │
│                     │         │                                      │
│  CacheClient        │         │  BenchmarkWorkload (Zipfian/Uniform) │
│  ConnectionPool     │ ─TCP──▶ │  PolicyComparisonBenchmark           │
│  ClusterCacheClient │         │  ConcurrencyBenchmark               │
│  CacheCLI           │         │  NetworkBenchmark                   │
└─────────────────────┘         └─────────────────────────────────────┘
```

---

## Layer 1 — Core Data Structures

Every cache policy in JCache is built on the same two components: a `HashMap` and a `DoublyLinkedList`. Understanding these is mandatory before understanding the policies.

### HashMap — O(1) Lookup

```
HashMap<K, Node<K,V>>

Key "Alice" ──hash──▶ bucket[42] ──▶ Node("Alice", "Engineer", prev, next)
Key "Bob"   ──hash──▶ bucket[91] ──▶ Node("Bob", "Designer", prev, next)
```

Given any key, we can find its node in O(1) average time. Without this, every get() would be O(N) — scanning all entries.

### DoublyLinkedList — O(1) Ordering

```
HEAD ←→ Node(MRU) ←→ Node ←→ Node ←→ Node(LRU) ←→ TAIL

addToFront(node):  head.next = node, node.prev = head    → O(1)
remove(node):      node.prev.next = node.next            → O(1)
removeLast():      return tail.prev, unlink it           → O(1)
```

The doubly linked list maintains order — most recently used at the front, least recently used at the tail — without any sorting. Sorting would be O(N log N). Every operation is O(1) because we always have direct pointer access to the node we want to move.

### Why Both Together?

- HashMap alone: O(1) lookup, but no ordering — can't find LRU entry efficiently
- LinkedList alone: O(1) order maintenance, but O(N) lookup by key
- HashMap + LinkedList: O(1) everything — lookup by key AND instant LRU tail access

This is the fundamental insight that makes all three eviction policies work.

### Node Structure

```java
class Node<K, V> {
    K key;
    V value;
    Node<K,V> prev;      // pointer to previous node in list
    Node<K,V> next;      // pointer to next node in list
    long expiryTime;     // System.currentTimeMillis() + TTL, or Long.MAX_VALUE
    int frequency;       // used by LFU only
}
```

The `key` is stored in the node (not just the value) because when evicting the tail, we need to know which key to remove from the HashMap. Without the key in the node, we would have to do a reverse lookup — O(N).

---

## Layer 2 — Eviction Policies

### LRU — Least Recently Used

**Core idea:** When the cache is full, evict the key that was accessed least recently.

```
Initial state (capacity=3):
  HEAD ←→ [C:30] ←→ [B:20] ←→ [A:10] ←→ TAIL
  (C is MRU, A is LRU)

get("B"):
  1. HashMap.get("B") → Node B       O(1)
  2. list.remove(Node B)             O(1)  (unlink from current position)
  3. list.addToFront(Node B)         O(1)  (move to MRU position)

  HEAD ←→ [B:20] ←→ [C:30] ←→ [A:10] ←→ TAIL

put("D", 40) — cache full:
  1. list.removeLast() → Node A      O(1)  (evict LRU)
  2. HashMap.remove("A")             O(1)
  3. create Node D, add to front     O(1)
  4. HashMap.put("D", Node D)        O(1)

  HEAD ←→ [D:40] ←→ [B:20] ←→ [C:30] ←→ TAIL
```

**Time complexity:** O(1) for get() and put().
**Space complexity:** O(capacity) — one node per cached entry.

**LRU weakness — the scan problem:**
```
Before scan: hot keys [A, B, C] are in cache
Sequential scan: access [key0, key1, key2, ..., key999]
After scan: cold scan keys fill cache, hot keys [A, B, C] are evicted
Next request for A: miss. Cold start all over again.
```

### LFU — Least Frequently Used (Shan's O(1) Algorithm)

**Core idea:** Evict the key that has been accessed the fewest times. Among ties, evict the least recently used.

**Naive approach (O(log N)):** A min-heap ordered by frequency. Problem: heap operations are O(log N), and with millions of cache operations per second, that adds up.

**Shan's O(1) approach:** Frequency buckets.

```
FrequencyMap: {
  freq=1: DoublyLinkedList [D, E]     ← candidates for eviction (min freq)
  freq=3: DoublyLinkedList [B, C]
  freq=7: DoublyLinkedList [A]        ← most accessed
}

minFreq = 1  ← always tracks the minimum, updated on every operation

get("D"):  
  1. HashMap.get("D") → Node D, freq=1       O(1)
  2. Remove D from freq=1 bucket             O(1)
  3. Increment D.freq to 2                  O(1)
  4. Add D to freq=2 bucket (create if new) O(1)
  5. If freq=1 bucket is now empty:
       minFreq = 2                           O(1)

Eviction (put when full):
  1. Get FrequencyMap[minFreq] bucket        O(1)
  2. Remove tail of that bucket (LRU among equals) O(1)
  3. Remove from HashMap                     O(1)
```

**Why minFreq tracking works:**
- On get(): new freq = old freq + 1. minFreq can only be minFreq or minFreq+1.
- On put() of new key: new key always starts at freq=1, so minFreq = 1.
- We never need to search for the minimum — it's always tracked.

**LFU cold start problem:**
A new key starts at frequency 1. Even if it's about to become very popular (a viral post), it is the first candidate for eviction. LFU is blind to future access patterns.

### ARC — Adaptive Replacement Cache

**Core idea:** Maintain two LRUs (T1 for recent, T2 for frequent) and ghost lists (B1, B2) of recently evicted keys. Use ghost hits to adapt the split between T1 and T2.

```
┌─────────────────────────────────────────┐
│  T1 (recent, seen once)    p entries    │
│  T2 (frequent, seen 2+) cap-p entries   │
├─────────────────────────────────────────┤
│  B1 (ghost of T1, keys only)            │
│  B2 (ghost of T2, keys only)            │
└─────────────────────────────────────────┘

p = target size for T1 (adaptation parameter, 0 ≤ p ≤ capacity)
```

**The adaptation logic:**
```
B1 hit (we evicted a recently-seen key that got accessed again):
  → We evicted too aggressively from T1 (recent list)
  → Increase p (give T1 more room)
  → p = min(p + max(1, |B2|/|B1|), capacity)

B2 hit (we evicted a frequently-seen key that got accessed again):
  → We evicted too aggressively from T2 (frequent list)
  → Decrease p (give T2 more room)
  → p = max(p - max(1, |B1|/|B2|), 0)
```

**Why ghost lists?**
Ghost lists hold keys without values — just enough to detect "we recently evicted this and now it's being accessed again." The insight is that a ghost hit is evidence of a wrong eviction decision, and we use it to correct the policy.

**Why ZFS uses ARC:**
File system access patterns shift — a database might do occasional full scans while serving random queries. ARC's scan resistance (scans fill T1 but don't evict T2) and adaptation (p adjusts after ghost hits) make it robust to these shifts without operator tuning.

---

## Layer 3 — TTL and Expiry

TTLCache is a **Decorator** around any `Cache<K,V>`. It adds time-based expiry without touching the policy implementations.

### Two-Mechanism Expiry

```
Put with TTL:
  expiryMap.put(key, System.currentTimeMillis() + ttl * 1000)
  delegate.put(key, value)

Get:
  if (expiryMap.get(key) < System.currentTimeMillis()):
    deleteKey(key)        ← LAZY EVICTION
    return null
  return delegate.get(key)

Background sweeper (every 500ms):
  for each entry in expiryMap:       ← ACTIVE EVICTION
    if expired: deleteKey(key)
```

**Why lazy eviction alone is insufficient:**
If a key is put with TTL and never read again, lazy eviction never fires. The key occupies memory forever. The background sweeper catches these dead keys.

**Why the sweeper alone is insufficient:**
The sweeper runs every 500ms. Between sweeper runs, an expired key could be returned to a caller. Lazy eviction on get() guarantees no expired value is ever returned.

**Ordering of expiryMap write vs delegate.put():**
We write to expiryMap **before** delegate.put(). If we did it after, there is a window where the key exists in the delegate but not in expiryMap. If the sweeper runs in that window, it would see no expiry entry and leave the key alone — a zombie that never expires.

---

## Layer 4 — Thread Safety

Three strategies, increasing complexity and throughput.

### Strategy 1 — Coarse-Grained Locking

```
ReentrantReadWriteLock lock

get():  readLock.lock()  → cache.get() → readLock.unlock()
put():  writeLock.lock() → cache.put() → writeLock.unlock()
```

Multiple readers can proceed simultaneously (readLock is shared). Any writer is exclusive — it blocks all readers and other writers. Under high write load, the single write lock becomes the bottleneck. Every thread queues up waiting for `writeLock`.

### Strategy 2 — Segmented Locking

```
16 segments, each a complete cache with its own lock

hash(key) % 16 → segment index

get(key):  segment = segments[hash(key) % 16]
           segment.lock.readLock()
           segment.cache.get(key)
           segment.lock.readLock.unlock()
```

A write to key "Alice" (segment 3) does not block a read of key "Bob" (segment 7). Contention is divided by the number of segments. With 16 segments and 32 threads, on average 2 threads compete per segment — far less than 32 threads competing for 1 lock.

**Why 16 segments?**
- Power of 2: `hash % 16` = `hash & 15` (bitwise AND, faster than modulo)
- Covers typical thread pool sizes (8–16 threads) with minimal collision
- Small enough that per-segment overhead is negligible

### Strategy 3 — Lock-Free

```
ConcurrentHashMap<K, Node<K,V>> store

get():  store.get(key)          ← ConcurrentHashMap.get() is non-blocking
        AtomicLong.increment()  ← frequency counter, no locks

put():  store.compute(key, (k, existing) -> newNode)  ← atomic CAS
        if (store.size() > capacity): evict()  ← only eviction needs coordination
```

`ConcurrentHashMap.get()` uses volatile reads and no locks. Under pure read load, lock-free scales linearly with thread count. The cost: eviction still requires coordination (you must atomically remove the LRU entry), so under heavy write load the gap with segmented locking narrows.

---

## Layer 5 — The Network Server

### Netty Reactor Pattern

```
Boss Thread (1 thread)
  │  
  │  accept() in a loop
  │  for each new TCP connection:
  │    register channel with a worker thread
  │
  ▼
Worker Thread Pool (N threads, default 2×CPU)
  Each worker manages multiple channels using Java NIO Selectors
  
  select() → [channel1 readable, channel3 readable]
              ↓
  Read bytes from channel1, pass through pipeline
  Read bytes from channel3, pass through pipeline
```

**Why non-blocking I/O?**
In blocking I/O, one thread per connection means 1000 clients = 1000 threads. Each thread uses ~512KB stack = 512MB RAM just for stacks. Plus context switching overhead. Non-blocking I/O with Netty: 8 worker threads can handle 10,000+ connections.

### Channel Pipeline — How Commands Are Processed

```
INBOUND (client → server):

Raw bytes: "GET foo\n"
     │
     ▼
LineBasedFrameDecoder
  Purpose: TCP is a stream — bytes can arrive in any chunk size.
           "GET " might arrive, then "foo\n" in the next read.
  Action:  Buffer bytes until \n is found, then emit one complete line.
  Output:  ByteBuf containing "GET foo" (no \n)
     │
     ▼
StringDecoder  
  Purpose: Convert ByteBuf to String for handler convenience.
  Output:  String "GET foo"
     │
     ▼
ConnectionManager
  Purpose: Enforce maxConnections limit.
           If limit exceeded: close channel, return.
           Otherwise: increment counter, propagate.
  Output:  String "GET foo" (unchanged, just passes through)
     │
     ▼
CacheServerHandler
  Purpose: Parse command, dispatch to cache, write response.
  Action:  tokens = "GET foo".split(" ") → ["GET", "foo"]
           value = cache.get("foo")
           ctx.writeAndFlush("+bar\r\n")

OUTBOUND (server → client):

String "+bar\r\n"
     │
     ▼
StringEncoder
  Purpose: Convert String to ByteBuf for sending.
  Output:  ByteBuf containing bytes [43, 98, 97, 114, 13, 10]
     │
     ▼
TCP socket sends bytes to client
```

### Why `ctx.writeAndFlush()` not `ctx.write()`?

`write()` places data in an outbound buffer. `flush()` sends the buffer contents over the socket. If you `write()` without `flush()`, the response stays buffered and the client hangs waiting. `writeAndFlush()` combines both. For a cache server where every command has exactly one immediate response, always flush.

---

## Layer 6 — Consistent Hashing and Clustering

### The Problem With Naive Sharding

```
3 servers. Naive approach: hash(key) % 3 → server index

Server 0 has keys: {0, 3, 6, 9, ...}
Server 1 has keys: {1, 4, 7, 10, ...}
Server 2 has keys: {2, 5, 8, 11, ...}

Add a 4th server:
  hash(key) % 4 → completely different distribution
  Almost every key is now on the wrong server
  Required: move ~75% of all keys → massive cache invalidation
```

### Consistent Hashing

```
Hash ring: 0 ──────── 2^32-1 (wraps around)

Physical servers placed at multiple points (virtual nodes):
  Server A → 150 positions spread around ring
  Server B → 150 positions
  Server C → 150 positions

Find server for key "foo":
  h = hash("foo") → some position on the ring
  Walk clockwise until first server node → that server owns "foo"

Add Server D:
  Server D → 150 new positions
  Only keys between D's positions and their previous clockwise neighbor move
  → ~25% of keys move (1/N where N=4)
  → 75% of keys stay on their original server ✓
```

**Why 150 virtual nodes per physical server?**
With few virtual nodes, the ring has gaps — one server might own 40% of the key space while another owns 10%. 150 virtual nodes gives statistically even distribution. The number is an empirical sweet spot from industry experience (Amazon Dynamo used 100-200).

**Virtual node implementation:**

```java
// For server "cache1:6379", create 150 virtual nodes
for (int i = 0; i < 150; i++) {
    String virtualKey = "cache1:6379#" + i;
    int position = hash(virtualKey);
    ring.put(position, cacheNode);  // TreeMap<Integer, CacheNode>
}

// Lookup: find smallest position >= hash(key)
int keyHash = hash(key);
Map.Entry<Integer, CacheNode> entry = ring.tailMap(keyHash).firstEntry();
if (entry == null) entry = ring.firstEntry();  // wrap around
return entry.getValue();
```

---

## Layer 7 — The Client and Connection Pool

### Why a Connection Pool?

Every TCP connection requires a handshake (SYN, SYN-ACK, ACK) — ~1ms on localhost, ~10ms+ over a network. If your application creates a new connection for every cache operation, you pay that cost every time.

```
Without pool:
  for each cache.get():
    TCP connect  (1ms)
    send GET     (0.1ms)
    recv response (0.1ms)
    TCP close    (1ms)
    Total: ~2.2ms — 99% overhead is connection management

With pool (10 pre-created connections):
  for each cache.get():
    pool.acquire()  (microseconds — just a queue poll)
    send GET        (0.1ms)
    recv response   (0.1ms)
    pool.release()  (microseconds)
    Total: ~0.2ms — nearly all time is actual cache work
```

### Pool Implementation

```
ConnectionPool:
  ArrayBlockingQueue<Socket> pool  ← thread-safe, bounded

  acquire():
    conn = pool.poll(timeout)  ← blocks if all connections in use
    if (!isHealthy(conn)):     ← PING to verify connection
        conn = createNew()
    return conn

  release(conn):
    pool.offer(conn)           ← returns to pool for next caller

  isHealthy(conn):
    conn.send("PING\n")
    return conn.read().startsWith("+PONG")
```

---

## Layer 8 — Metrics and Observability

### LongAdder vs AtomicLong for Counters

```
AtomicLong.incrementAndGet() under 32 threads:
  Thread 1: CAS(0→1) ✓
  Thread 2: CAS(0→1) ✗, retry CAS(1→2) ✓
  Thread 3: CAS(0→1) ✗, CAS(1→2) ✗, retry CAS(2→3) ✓
  ... high contention = many retries = wasted CPU

LongAdder.increment() under 32 threads:
  Thread 1: cells[0]++    (no contention — own cell)
  Thread 2: cells[1]++    (no contention — own cell)
  ...
  sum() = cells[0] + cells[1] + ... + cells[N]
```

LongAdder writes are ~3-5× faster than AtomicLong under high concurrency. The tradeoff: `sum()` is slightly more expensive than `AtomicLong.get()`. For metrics (written on every operation, read rarely for STATS), LongAdder is the correct choice.

### Sliding Window p99

```
Last 1000 latency samples in ConcurrentLinkedQueue<Long>:
  [0.08ms, 0.11ms, 0.07ms, 0.31ms, 0.09ms, ...]

On STATS request:
  1. Drain queue to List (snapshot — queue continues receiving)
  2. Collections.sort(samples)
  3. p50 = samples.get(samples.size() * 0.50)
  4. p99 = samples.get(samples.size() * 0.99)
```

The expensive sort (O(N log N), N=1000) only runs when STATS is explicitly requested, not on every operation. This is "compute on read" — pay the aggregation cost only when someone needs the data.

---

## Design Patterns Used

| Pattern | Where | Why |
|---|---|---|
| **Decorator** | TTLCache, CoarseGrainedCache, SegmentedCache, LockFreeCache | Add behaviour (TTL, thread safety) without modifying policy classes |
| **Strategy** | EvictionPolicy interface, LRU/LFU/ARC | Swap policies without changing the surrounding code |
| **Factory** | CacheFactory | Hide construction complexity, single creation point |
| **Builder** | ServerConfig.Builder | Readable construction of objects with many optional parameters |
| **Reactor** | Netty boss/worker groups | Non-blocking I/O — one thread handles many connections |
| **Chain of Responsibility** | Netty ChannelPipeline | Each handler processes then passes to next (framing → decoding → logic) |
| **Object Pool** | ConnectionPool | Reuse expensive TCP connections across multiple cache operations |

---

## Data Flow — Request Lifecycle

A complete trace of what happens when a client sends `PUT name Alice 60`:

```
1. Client sends bytes: "PUT name Alice 60\n"

2. NIO Selector detects data available on the channel
   Worker thread is assigned to handle the read

3. LineBasedFrameDecoder buffers bytes until \n
   Emits ByteBuf: "PUT name Alice 60"

4. StringDecoder converts ByteBuf → String "PUT name Alice 60"
   ByteBuf is released (reference count → 0, returned to pool)

5. ConnectionManager.channelRead() propagates (does not intercept data)

6. CacheServerHandler.channelRead0() called with "PUT name Alice 60"
   tokens = ["PUT", "name", "Alice", "60"]
   verb = "PUT"

7. handlePut(tokens, original):
   key = "name"
   value = "Alice"  (reconstructed from original string)
   ttl = 60 seconds

8. startNs = System.nanoTime()
   
9. cache.put("name", "Alice")   ← enters SegmentedCache
   segment = segments[hash("name") % 16]
   segment.writeLock.lock()
   delegate.put("name", "Alice")    ← enters LRUCache
     node = new Node("name", "Alice")
     hashMap.put("name", node)
     list.addToFront(node)
     if (size > capacity): evictLRU()
   segment.writeLock.unlock()

10. durationNs = System.nanoTime() - startNs

11. metrics.recordPut(durationNs)
    totalPuts.increment()
    latencyWindow.offer(durationNs)

12. ctx.writeAndFlush("+OK\r\n")
    StringEncoder converts "+OK\r\n" → ByteBuf
    ByteBuf sent over TCP socket to client

13. Client receives "+OK\r\n"

Total time: ~0.05-0.3ms on localhost
```

---

## Memory Layout

### Per-entry memory cost (LRU)

```
Node<String, String> object:
  Object header:  16 bytes
  key (String ref): 8 bytes  + String object: 40+ bytes + char[] data
  value (String ref): 8 bytes + String object: 40+ bytes + char[] data
  prev pointer: 8 bytes
  next pointer: 8 bytes
  expiryTime (long): 8 bytes
  frequency (int): 4 bytes
  padding: ~4 bytes
  
  Total per node: ~150 bytes + key and value string data

HashMap entry:
  ~32 bytes additional overhead

Total per cached key-value pair: ~200 bytes + string data
At 10,000 entries with avg 50-byte key+value: ~200 * 10,000 = 2MB
```

### Ghost list memory (ARC only)

Ghost lists store keys only — no values. Memory per ghost entry: ~100 bytes. With capacity=10,000, ghost lists hold up to 10,000 entries = ~1MB additional overhead. Total ARC memory footprint: ~2× LRU for the same capacity.

---

## Failure Modes and How They Are Handled

| Failure | Detection | Handling |
|---|---|---|
| Client disconnects abruptly | IOException in channelRead | ConnectionManager.exceptionCaught() closes channel, decrements count |
| Client sends malformed command | CommandParser throws ProtocolException | Handler returns -ERR response, connection stays open |
| Cache capacity exceeded | size() > capacity check on every put() | Eviction fires — LRU/LFU/ARC removes appropriate entry |
| TTL key never read (memory leak) | Background sweeper every 500ms | Sweeper scans expiryMap, deletes expired keys |
| maxConnections reached | ConnectionManager.channelActive() check | New connection closed immediately, rejectedConnections counter increments |
| Server port in use | ServerBootstrap.bind().sync() throws | Process exits with error message showing which port failed |
| JVM receives SIGTERM | Runtime.addShutdownHook() | Graceful shutdown: stop accepting, wait 2s for in-flight requests, close all connections |
| Netty worker thread exception | exceptionCaught() on every handler | Log + close channel — other channels on other worker threads unaffected |
| Benchmark JIT not warmed up | JMH @Warmup iterations | 3 warmup iterations (3 seconds) run before measurement begins |