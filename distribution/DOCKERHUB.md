# JCache

An in-memory cache server written from scratch in Java: LRU, LFU and ARC
eviction, TTLs, persistence, and a TCP server built on Netty that speaks a
simple text protocol and the Redis protocol (RESP).

Source, docs and Java client: https://github.com/himanshmunjal/JCache

## Tags

- `latest`: the most recent release
- `1.0.3`: an exact release
- `1.0`, `1`: the newest release in that line

Images are built for `linux/amd64` and `linux/arm64`.

## Quick start

```bash
docker run -d --name jcache -p 6379:6379 himanshmunjal/jcache
```

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
```

The same port also speaks RESP, the protocol Redis uses, so existing Redis
tools such as `redis-cli` and client libraries can talk to JCache directly.
Redis itself is not involved:

```
$ redis-cli -p 6379 SET user:1 "Alice Smith" EX 3600
OK
$ redis-cli -p 6379 GET user:1
"Alice Smith"
```

See [RESP](https://github.com/himanshmunjal/JCache/blob/main/docs/wire-protocol.md#resp)
for the supported commands.

## With persistence

```bash
docker run -d --name jcache -p 6379:6379 \
  -e JCACHE_POLICY=ARC \
  -e JCACHE_CAPACITY=100000 \
  -e JCACHE_PERSISTENCE_ENABLED=true \
  -v jcache-data:/data \
  himanshmunjal/jcache
```

Snapshots and the append-only file are written to `/data` and replayed on start-up.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `JCACHE_PORT` | `6379` | TCP port |
| `JCACHE_CAPACITY` | `10000` | Maximum number of keys |
| `JCACHE_POLICY` | `LRU` | `LRU`, `LFU` or `ARC` |
| `JCACHE_SEGMENTS` | `16` | Lock segments, a power of two |
| `JCACHE_DEFAULT_TTL` | `0` | TTL in seconds for writes without one; 0 = never expire |
| `JCACHE_SWEEP_INTERVAL` | `500` | Expiry sweeper interval, ms |
| `JCACHE_THREADS` | `8` | Netty I/O threads |
| `JCACHE_BOSS_THREADS` | `1` | Netty acceptor threads |
| `JCACHE_MAX_CONNECTIONS` | `1000` | Connection limit |
| `JCACHE_RATE_LIMIT` | `0` | Commands per second per connection; 0 = no limit. `PING` and `QUIT` are not counted |
| `JCACHE_RATE_LIMIT_BURST` | `0` | Commands a connection may send at once; 0 = same as the rate |
| `JCACHE_PERSISTENCE_ENABLED` | `false` | Enable snapshot + AOF persistence |
| `JCACHE_SNAPSHOT_PATH` | `/data` | Persistence directory |
| `JCACHE_SNAPSHOT_INTERVAL_MS` | `300000` | Interval between snapshots |
| `JCACHE_VERBOSE` | `false` | Log every command |
| `JAVA_OPTS` | `-XX:MaxRAMPercentage=75 -XX:+UseG1GC` | JVM flags |

If you change `JCACHE_PORT`, map that port instead of 6379.

## Commands

| Command | Description |
|---|---|
| `PING` | Liveness check |
| `GET key` | Read a value |
| `PUT key value [ttl]` | Store a value, optionally with a TTL in seconds. Alias `SET` |
| `DELETE key` | Remove a key. Alias `DEL` |
| `EXPIRE key seconds` | Set a TTL on an existing key; 0 deletes it |
| `TTL key` | Seconds until the key expires, or -1 |
| `PERSIST key` | Remove a key's TTL |
| `STATS` | Hit rate, request counts, latency percentiles |
| `FLUSH` | Remove every key |
| `QUIT` | Close the connection |

The full protocol is in [docs/wire-protocol.md](https://github.com/himanshmunjal/JCache/blob/main/docs/wire-protocol.md).

## Using it from Java

The [starter project](https://github.com/himanshmunjal/JCache/tree/main/starter)
connects to this image with the Java client and has examples for sessions with
TTLs, cache-aside reads through a connection pool, and the cluster client.

## Three-node cluster

```bash
git clone https://github.com/himanshmunjal/JCache.git
cd JCache
docker compose -f distribution/docker-compose.yml up -d
```

This starts nodes on ports 6379, 6380 and 6381. `ClusterCacheClient`
shards keys across them with a consistent hash ring.

## Image details

- Base image: `eclipse-temurin:21-jre`
- Runs as the non-root user `jcache`
- `HEALTHCHECK` sends `PING` and expects `+PONG`
- Volume `/data`, port `6379`

## License

MIT
