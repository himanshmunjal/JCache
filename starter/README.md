# JCache starter

A small Maven project that talks to a running JCache server with the Java
client. Copy this directory to start your own project.

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

## 2. Install the client

The client is not on Maven Central yet. Install it into your local Maven
repository in one of two ways.

From a release (JDK 17+ and Maven 3.8+):

```bash
curl -LO https://github.com/himanshmunjal/JCache/releases/download/v1.0.3/jcache-client-1.0.3.jar
mvn install:install-file -Dfile=jcache-client-1.0.3.jar \
  -DgroupId=com.cache -DartifactId=cache-client -Dversion=1.0.3 \
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
```

| Class | Shows |
|---|---|
| `QuickStart` | PING, PUT, GET, TTL, PERSIST, DELETE, STATS on one connection |
| `SessionStore` | Sessions that expire after 30 minutes of inactivity, using `EXPIRE` to extend them |
| `CacheAside` | Cache-aside reads from 8 threads through a `ConnectionPool` |
| `ClusterExample` | Sharding keys over three servers with `ClusterCacheClient` |

`ClusterExample` needs three servers. From a clone of the JCache repository,
`docker compose -f distribution/docker-compose.yml up -d` starts them on ports
6379, 6380 and 6381.

## Settings

| Variable | Default | Used by |
|---|---|---|
| `JCACHE_HOST` | `localhost` | all single-server examples |
| `JCACHE_PORT` | `6379` | all single-server examples |
| `JCACHE_NODES` | `localhost:6379,localhost:6380,localhost:6381` | `ClusterExample` |

## Things to know

- Keys cannot contain whitespace. Values can contain spaces but not line breaks.
- `CacheClient` is one TCP connection and sends one request at a time. Share a
  `ConnectionPool` between threads instead.
- `get` returns `null` for a missing key; `ttl` returns `-1` for a key without
  expiry and `-2` for a missing key.
- Values are strings. Serialize objects yourself, for example as JSON.

The full command list is in the
[main README](https://github.com/himanshmunjal/JCache#protocol).
