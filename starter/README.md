# JCache starter

A small Maven project that talks to a running JCache server, with the JCache
Java client and with Jedis over RESP. Copy this directory to start your own
project.

## 1. Start a server

```bash
docker run -d --name jcache -p 6379:6379 himanshmunjal/jcache
```

If you run the server on another port with `JCACHE_PORT`, map that port and
export the same value for the examples:

```bash
export JCACHE_PORT=7000
docker run -d --name jcache -e JCACHE_PORT=$JCACHE_PORT -p $JCACHE_PORT:$JCACHE_PORT himanshmunjal/jcache
```

`RateLimitDemo` needs a server with a rate limit. Start a second one so the
other examples are not limited:

```bash
docker run -d --name jcache-limited -p 6390:6379 \
  -e JCACHE_RATE_LIMIT=20 -e JCACHE_RATE_LIMIT_BURST=5 himanshmunjal/jcache
```

## 2. Install the client

The client is not on Maven Central yet. Install it into your local Maven
repository in one of two ways.

From a release (JDK 17+ and Maven 3.8+):

```bash
curl -LO https://github.com/himanshmunjal/JCache/releases/download/v1.0.4/jcache-client-1.0.4.jar
mvn install:install-file -Dfile=jcache-client-1.0.4.jar \
  -DgroupId=com.cache -DartifactId=cache-client -Dversion=1.0.4 \
  -Dpackaging=jar -DgeneratePom=true
```

The release jar already contains its dependencies, so `-DgeneratePom=true`
is needed to stop Maven looking for them.

From source:

```bash
git clone https://github.com/himanshmunjal/JCache.git
cd JCache
mvn install -pl cache-client -am -DskipTests
```

## 3. Run the examples

```bash
mvn compile exec:java                                                     # QuickStart
mvn compile exec:java -Dexec.mainClass=com.example.jcache.SessionStore
mvn compile exec:java -Dexec.mainClass=com.example.jcache.CacheAside
mvn compile exec:java -Dexec.mainClass=com.example.jcache.ClusterExample
mvn compile exec:java -Dexec.mainClass=com.example.jcache.RedisClientExample
JCACHE_PORT=6390 mvn compile exec:java -Dexec.mainClass=com.example.jcache.RateLimitDemo
```

| Class | Shows |
|---|---|
| `QuickStart` | PING, PUT, GET, TTL, PERSIST, DELETE, STATS on one connection |
| `SessionStore` | Sessions that expire after 30 minutes of inactivity, using `EXPIRE` to extend them |
| `CacheAside` | Cache-aside reads from 8 threads through a `ConnectionPool` |
| `ClusterExample` | Sharding keys over three servers with `ClusterCacheClient` |
| `RedisClientExample` | Jedis over RESP: `SET ... EX`, `SETEX`, `EXISTS`, multi-key `DEL`, `DBSIZE`, `INFO`, values with line breaks |
| `RateLimitDemo` | Commands rejected by the per-connection rate limit, then the same commands through `RateLimitRetry` |

`ClusterExample` needs three servers. From a clone of the JCache repository,
`docker compose -f distribution/docker-compose.yml up -d` starts them on ports
6379, 6380 and 6381.

## Settings

| Variable | Default | Used by |
|---|---|---|
| `JCACHE_HOST` | `localhost` | all single-server examples |
| `JCACHE_PORT` | `6379` | all single-server examples |
| `JCACHE_NODES` | `localhost:6379,localhost:6380,localhost:6381` | `ClusterExample` |

## Rate limiting

`JCACHE_RATE_LIMIT` gives each connection a token bucket of that many commands
per second, holding up to `JCACHE_RATE_LIMIT_BURST`. A command that finds the
bucket empty is not run and not queued. The connection stays open and the
client sees:

| Client | Error |
|---|---|
| `CacheClient` | `CacheClientException` with message `Server error: rate limit exceeded` |
| Jedis | `JedisDataException` with message `ERR rate limit exceeded` |

`PING` and `QUIT` are not counted. `STATS` (or `INFO` over RESP) reports the
server-wide count as `rateLimited`. `RateLimitRetry` wraps a call and retries
only this error, doubling the wait each time.

The limit is per connection, so a `ConnectionPool` of 8 can send up to 8 times
the rate.

## Things to know

- With `CacheClient`, keys cannot contain whitespace and values cannot contain
  line breaks. Over RESP, keys and values are binary-safe.
- `CacheClient` is one TCP connection and sends one request at a time. Share a
  `ConnectionPool` between threads instead.
- `get` returns `null` for a missing key; `ttl` returns `-1` for a key without
  expiry and `-2` for a missing key. Jedis behaves the same way.
- Values are strings. Serialize objects yourself, for example as JSON.

The full command list, including the RESP commands, is in the
[main README](https://github.com/himanshmunjal/JCache#protocol).
