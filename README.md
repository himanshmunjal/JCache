# JCache

[![CI](https://github.com/himanshmunjal/JCache/actions/workflows/ci.yml/badge.svg)](https://github.com/himanshmunjal/JCache/actions/workflows/ci.yml)
![Java 17+](https://img.shields.io/badge/Java-17%2B-blue)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-himanshmunjal%2Fjcache-blue)](https://hub.docker.com/r/himanshmunjal/jcache)

An in-memory cache written from scratch in Java: a library with LRU, LFU and
ARC eviction, and a Netty-based TCP server with TTLs, persistence and a
consistent-hashing cluster client.

Not related to JSR-107 (`javax.cache`), the Java caching API also called JCache.

The project builds, from first principles, the parts that make up a cache
like Redis or Caffeine: O(1) eviction data structures, locking strategies,
expiry, crash recovery and client-side sharding.

- **Three eviction policies**, all O(1): LRU, LFU with frequency decay, and ARC.
- **Three thread-safety strategies**: one global lock, lock striping by segment, and lock-free reads.
- **TTLs** with lazy expiry on read plus a background sweeper.
- **Persistence**: append-only file plus periodic snapshots, replayed on start-up.
- **TCP server** on Netty with a text protocol you can drive with `nc`, and RESP so `redis-cli` and Redis client libraries work too.
- **Cluster client** that shards keys over several servers with a consistent hash ring.
- **JMH benchmarks** and a hit-rate simulation.

## Quick start

With Docker:

```bash
docker run -d --name jcache -p 6379:6379 himanshmunjal/jcache
```

From source (JDK 17 or newer, Maven 3.8+):

```bash
git clone https://github.com/himanshmunjal/JCache.git
cd JCache
mvn package -DskipTests
java -jar cache-server/target/cache-server-1.0.4.jar
```

Then talk to it:

```
$ nc localhost 6379
PUT user:1 Alice Smith
+OK
GET user:1
+Alice Smith
PUT session:42 abc123 60
+OK
TTL session:42
+59
STATS
+hits:1 misses:0 evictions:0 size:2 gets:1 puts:2 ...
```

or use the interactive client:

```bash
java -jar cache-client/target/cache-client-1.0.4.jar localhost 6379
```

or any Redis client, on the same port (JCache speaks the Redis protocol; no
Redis server is involved):

```
$ redis-cli -p 6379 SET user:1 "Alice Smith" EX 3600
OK
$ redis-cli -p 6379 GET user:1
"Alice Smith"
```

## Using it as a library

`cache-core` has no dependencies and can be used in-process:

```java
Cache<String, User> users = CacheFactory.arc(10_000);

Cache<String, Session> sessions = CacheFactory
        .<String, Session>builder(CachePolicyType.LRU, 50_000)
        .withTTL(Duration.ofMinutes(30))
        .withMetrics("sessions")
        .build();
```

The returned caches are not thread-safe. Wrap them for concurrent use:

```java
Cache<String, String> shared = new SegmentedCache<>(100_000, 16, CachePolicyType.LFU);
Cache<String, String> simple = new CoarseGrainedCache<>(CacheFactory.lru(1_000));
```

## Java client

```java
try (CacheClient client = new CacheClient("localhost", 6379)) {
    client.put("greeting", "hello world");
    client.put("session:1", "token", 3600);   // expires in an hour
    String value = client.get("greeting");    // null if absent
    long ttl = client.ttl("session:1");       // seconds left, -1 = no expiry, -2 = absent
}
```

For concurrent use, share a pool:

```java
ConnectionPool pool = new ConnectionPool("localhost", 6379, 8);
CacheClient conn = pool.acquire();
try {
    conn.put("k", "v");
} finally {
    pool.release(conn);
}
```

To spread keys over several servers:

```java
try (ClusterCacheClient cluster = ClusterCacheClient.builder()
        .addServer("node-1", "localhost", 6379)
        .addServer("node-2", "localhost", 6380)
        .addServer("node-3", "localhost", 6381)
        .withHealthCheck(5_000)
        .build()) {
    cluster.put("user:1", "Alice");
    cluster.get("user:1");                     // goes to the same node
    cluster.getRoutingTarget("user:1");        // which node owns the key
}
```

A runnable starter project with session-store, cache-aside and cluster
examples is in [starter/](starter/README.md).

## Protocol

One command per line, one reply per line: `+payload` or `-ERR message`.

| Command | Description |
|---|---|
| `PING` | Liveness check. |
| `GET key` | Read a value. |
| `PUT key value [ttl]` | Store a value, optionally with a TTL in seconds. Alias `SET`. |
| `DELETE key` | Remove a key. Alias `DEL`. |
| `EXPIRE key seconds` | Set a TTL on an existing key; 0 deletes it. |
| `TTL key` | Seconds until the key expires, or -1. |
| `PERSIST key` | Remove a key's TTL. |
| `STATS` | Hit rate, request counts, latency percentiles. |
| `FLUSH` | Remove every key. |
| `QUIT` | Close the connection. |

Values may contain spaces. The full rules, including how a trailing number is
read as a TTL, are in [docs/wire-protocol.md](docs/wire-protocol.md).

The same port speaks RESP2, the Redis protocol, chosen per connection from its
first byte. Redis clients can use `GET`, `SET` (with `EX`/`PX`), `SETEX`,
`DEL`, `EXISTS`, `EXPIRE`, `TTL`, `PERSIST`, `DBSIZE`, `FLUSHALL`, `INFO`
and `PING`; the replies follow Redis (`(nil)` for a missing key, `-2` from
`TTL`, counts from `DEL`). See [RESP](docs/wire-protocol.md#resp) for the
details and the differences from Redis.

## Configuration

Settings come from built-in defaults, then an optional properties file
(`--config file`), then environment variables, then command-line flags; later
sources win.

| Environment variable | Flag | Default | Description |
|---|---|---|---|
| `JCACHE_PORT` | `--port` | `6379` | TCP port |
| `JCACHE_CAPACITY` | `--capacity` | `10000` | Maximum number of keys |
| `JCACHE_POLICY` | `--policy` | `LRU` | `LRU`, `LFU` or `ARC` |
| `JCACHE_SEGMENTS` | `--segments` | `16` | Lock segments, a power of two |
| `JCACHE_DEFAULT_TTL` | `--default-ttl` | `0` | TTL in seconds for writes without one; 0 = never expire |
| `JCACHE_SWEEP_INTERVAL` | | `500` | Expiry sweeper interval, ms |
| `JCACHE_THREADS` | | `8` | Netty I/O threads |
| `JCACHE_BOSS_THREADS` | | `1` | Netty acceptor threads |
| `JCACHE_MAX_CONNECTIONS` | | `1000` | Connection limit |
| `JCACHE_RATE_LIMIT` | `--rate-limit` | `0` | Commands per second per connection; 0 = no limit. `PING` and `QUIT` are not counted |
| `JCACHE_RATE_LIMIT_BURST` | `--rate-limit-burst` | `0` | Commands a connection may send at once; 0 = same as the rate |
| `JCACHE_METRICS_PORT` | `--metrics-port` | off | Serve Prometheus metrics at `http://host:<port>/metrics` |
| `JCACHE_PERSISTENCE_ENABLED` | `--persist` | `false` | Enable snapshot + AOF persistence |
| `JCACHE_SNAPSHOT_PATH` | `--data-dir` | `./jcache-data` (`/data` in Docker) | Persistence directory |
| `JCACHE_SNAPSHOT_INTERVAL_MS` | | `300000` | Interval between snapshots |
| `JCACHE_VERBOSE` | `--verbose` | `false` | Log every command |

The matching properties-file keys are listed in the Javadoc of `ServerConfig`.
In Docker, JVM options go in `JAVA_OPTS` (default
`-XX:MaxRAMPercentage=75 -XX:+UseG1GC`).

### Prometheus metrics

With `--metrics-port 9100` the server answers `GET /metrics` on that port with
the `STATS` counters in the Prometheus text format: `jcache_commands_total`,
`jcache_hits_total`, `jcache_misses_total`, `jcache_evictions_total`,
`jcache_errors_total`, `jcache_rate_limited_total`, `jcache_connections_total`,
`jcache_entries`, `jcache_capacity`, `jcache_connections_active`,
`jcache_uptime_seconds` and `jcache_latency_seconds{quantile}`. Hit rate is
`rate(jcache_hits_total[5m]) / ignoring(command) rate(jcache_commands_total{command="get"}[5m])`.

`docker compose -f distribution/docker-compose.yml --profile monitoring up -d`
starts three nodes with Prometheus on port 9090 and a Grafana dashboard on
<http://localhost:3000>.

Settings are read once at startup; restart the server (or recreate the
container) to change them.

## Docker

```bash
# Persistent, ARC, 100k keys
docker run -d --name jcache -p 6379:6379 \
  -e JCACHE_POLICY=ARC \
  -e JCACHE_CAPACITY=100000 \
  -e JCACHE_PERSISTENCE_ENABLED=true \
  -v jcache-data:/data \
  himanshmunjal/jcache

# Flags after the image name go to the server
docker run -d -p 6379:6379 himanshmunjal/jcache --policy LFU --capacity 50000
docker run --rm himanshmunjal/jcache --help

# Settings from a file
docker run -d -p 6379:6379 --env-file jcache.env himanshmunjal/jcache
docker run -d -p 6379:6379 -v "$PWD/jcache.properties:/app/jcache.properties" \
  himanshmunjal/jcache --config /app/jcache.properties

# Three-node cluster for trying ClusterCacheClient (ports 6379-6381)
./distribution/scripts/start.sh --build
./distribution/scripts/stop.sh
```

The image runs as a non-root user, keeps data in `/data`, has a health check
based on `PING`, and writes a final snapshot when stopped with `docker stop`.
Mount a volume at `/data` when persistence is on, or the data is lost with the
container. To use another host port, change only the host side of the mapping
(`-p 6390:6379`); if you set `JCACHE_PORT`, map that port instead.

## How it works

The server's cache is a stack of decorators:

```
TTLCache  ->  SegmentedCache (16 locked segments)  ->  LRU / LFU / ARC per segment
```

- **LRU**: hash map + doubly linked list in access order.
- **LFU**: one list per access count and a pointer to the lowest count, so
  eviction is O(1). Counts are halved every 10,000 operations so old hot keys
  can age out.
- **ARC**: two LRU lists (seen once, seen twice) plus two ghost lists of
  recently evicted keys that steer how much space each list gets.
- **Expiry**: checked on every read, plus a sweeper thread for keys nobody reads.
- **Persistence**: each write is appended to `jcache.aof`; a periodic
  snapshot replaces the AOF; start-up loads the snapshot and replays the AOF.

[docs/architecture.md](docs/architecture.md) covers the design in detail,
including the trade-offs and known limitations. The API reference is
published at <https://himanshmunjal.github.io/JCache/>.

## Benchmarks

Read-through hit rate (on a miss, load and insert), cache of 256 entries over
1,000 keys, one million requests:

| Workload | LRU | LFU | ARC |
|---|---|---|---|
| Zipf (s = 1.0) | 74.2% | 81.0% | 76.6% |
| Uniform | 25.5% | 26.1% | 25.4% |
| Sequential scan + Zipf | 51.0% | 61.7% | 53.7% |

On a stable skewed workload LFU wins, as expected. A uniform workload is a
sanity check: every policy should land at capacity / keys = 25.6%.

Throughput numbers and how to reproduce them are in
[docs/benchmark-results.md](docs/benchmark-results.md).

## Building and testing

```bash
mvn verify                                  # build, run all tests, coverage in */target/site/jacoco
mvn javadoc:aggregate                       # API docs in target/reports/apidocs
mvn test -pl cache-core -Dtest=ARCCacheTest # one test class
```

The integration tests start real servers on free ports, so nothing needs to
be running beforehand.

```bash
mvn package -pl cache-benchmark -am -DskipTests
java -jar cache-benchmark/target/benchmarks.jar PolicyComparison
java -cp cache-benchmark/target/benchmarks.jar com.cache.bench.HitRateSimulation
```

## Project layout

```
cache-core/              policies, concurrency wrappers, TTL, persistence, metrics
cache-common/            protocol parser, RESP codec, consistent hash ring
cache-server/            Netty server and configuration
cache-client/            Java client, connection pool, cluster client, CLI
cache-benchmark/         JMH benchmarks
cache-integration-test/  end-to-end tests
starter/                 example project for client applications
distribution/            Dockerfile, docker-compose, scripts, Homebrew formula
docs/                    architecture, protocol, benchmark results
```

## Roadmap

- Replication between nodes.

## License

[MIT](LICENSE)
