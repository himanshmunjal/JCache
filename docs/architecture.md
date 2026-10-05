# Architecture

This document describes how the pieces of JCache fit together and why they
are built the way they are. For the command reference see
[wire-protocol.md](wire-protocol.md).

## Modules

```
cache-core      no dependencies    policies, concurrency wrappers, TTL, persistence, metrics
cache-common    netty-codec        text protocol parser, RESP codec, consistent hash ring
cache-server    core, common       Netty server, configuration, request handler
cache-client    common             blocking client, connection pool, cluster client, CLI
cache-benchmark all of the above   JMH benchmarks and a hit-rate simulation
cache-integration-tests            end-to-end tests against real servers
```

`cache-core` can be used on its own as an in-process cache library.

## The cache stack

The server builds its cache as a stack of decorators, each adding one concern:

```
TTLCache            expiry: lazy check on read + background sweeper
  SegmentedCache    thread safety: N independently locked segments
    LRU/LFU/ARC     eviction policy, one instance per segment, not thread-safe
```

Every layer implements the same `Cache<K, V>` interface, so the layers can be
combined differently. `CacheFactory` builds the common combinations for
library users.

### Eviction policies

All three policies keep a `HashMap` from key to a node in an intrusive doubly
linked list (`core/DoublyLinkedList`), which makes every operation O(1).

- **LRU** keeps one list in access order and evicts from the tail.
- **LFU** keeps one list per access count plus the lowest count in use, as in
  Shah, Mitra and Matani (2010). Ties are broken by recency. Every 10,000
  operations all counts are halved, so keys that were hot in the past do not
  stay in the cache forever.
- **ARC** follows Megiddo and Modha (2003): two resident lists (seen once,
  seen at least twice) and two ghost lists of recently evicted keys. A miss
  on a ghost key shifts the target size of the "seen once" list towards
  whichever list it came from. Ghost lists are bounded so total metadata
  never exceeds twice the capacity.

Hit rates for the three policies on the benchmark workloads are in
[benchmark-results.md](benchmark-results.md).

### Thread safety

The policy classes are deliberately single-threaded, so that their logic can
be tested without concurrency and the locking strategy can be swapped.

| Wrapper | Locking | Notes |
|---|---|---|
| `CoarseGrainedCache` | one `ReentrantLock` | Simplest and always correct. A read-write lock would not help: `get` reorders lists, so it is a write. |
| `SegmentedCache` | one lock per segment | The key's hash picks the segment. Capacity is split exactly across segments, so eviction is per segment. Used by the server. |
| `LockFreeCache` | none on reads | `ConcurrentHashMap` plus a last-access timestamp per entry. Eviction scans for the oldest timestamp, so it is approximate LRU with O(n) eviction. |

### Expiry

`TTLCache` stores an absolute deadline per key next to the delegate cache.

- A read checks the deadline first; an expired key is removed and the read
  counts as a miss.
- A sweeper thread runs every `JCACHE_SWEEP_INTERVAL` ms (500 by default) and
  removes expired keys nobody reads. When the delegate has evicted keys on its
  own, the sweeper also drops their leftover deadlines.
- Operations on the same key are serialised with a striped lock (64 stripes),
  so a concurrent `PUT` cannot be undone by an expiry check that read the old
  deadline.

## Persistence

Persistence is modelled on Redis's RDB + AOF combination and is off by default.

- **Append-only file** (`jcache.aof`): every write is appended as one line,
  `SET <key> <value> <expiryMs>`, `DEL <key>` or `FLUSH`. Keys and values are
  Base64-encoded so any content survives. Expiry is stored as an absolute
  timestamp, so a key that expires while the server is down is not restored.
- **Snapshot** (`jcache.snapshot`): every `JCACHE_SNAPSHOT_INTERVAL_MS`
  (five minutes by default) and on shutdown, the whole cache is written to a
  binary file, which is then atomically renamed over the previous one, and
  the AOF is truncated.
- **Recovery**: on start-up the snapshot is loaded and the AOF replayed on top.
  Corrupt input is skipped and reported rather than stopping the server.

A snapshot holds the AOF lock while it runs, and each request applies its
change and appends its AOF record under that same lock. That ordering is what
guarantees no write is lost between taking a snapshot and truncating the AOF,
and that concurrent writes to one key are replayed in the order they were
applied. The cost is that writes pause while a snapshot is written.

AOF writes are flushed to the operating system after every record but not
`fsync`ed, so a process crash loses nothing while a power failure can lose
the last few writes.

## Server

The server is a Netty application with one acceptor thread and
`JCACHE_THREADS` I/O threads. Each connection gets this pipeline:

```
LineBasedFrameDecoder  split the byte stream into lines (max 1 MiB)
StringDecoder/Encoder  UTF-8
ConnectionManager      shared; enforces JCACHE_MAX_CONNECTIONS and tracks open channels
CacheServerHandler     per connection; parses with CommandParser and runs the command
```

Commands are executed directly on the I/O thread. That is safe because every
cache operation is short and non-blocking, apart from the brief pause while a
snapshot is written.

On `SIGTERM` (`docker stop`) a shutdown hook stops accepting connections,
closes the open ones, stops the sweeper and writes a final snapshot.

Configuration is resolved in this order, later sources winning: built-in
defaults, a properties file given with `--config`, `JCACHE_*` environment
variables, command-line flags.

## Clients and clustering

`CacheClient` is a blocking client over one socket. `ConnectionPool` keeps a
fixed number of them and checks connections that have been idle for more than
five seconds with a `PING` before handing them out.

`ClusterCacheClient` spreads keys over independent servers using a
`ConsistentHashRing`. Each server occupies 150 positions on the ring, based on
an MD5 hash, so adding or removing one of N servers moves only about 1/N of
the keys. There is no coordinator: every client computes the owner of a key
itself.

If the owning server is unreachable, the request is retried once on another
server. A value written during such a failover lives on that other server and
reads as a miss once the owner is back, which is acceptable for a cache. With
health checks enabled, a server that fails three checks in a row is removed
from the ring.

There is no replication: each key lives on exactly one server.

## Known limitations

- No replication or rebalancing; losing a node loses its keys.
- Writes pause while a snapshot is being written, which matters for very
  large caches.
- `SegmentedCache` evicts per segment, so the evicted entry is the least
  recently used in its segment, not necessarily in the whole cache.
- Values cannot contain line breaks over the text protocol.
- There is no authentication or TLS; run the server on a trusted network.
