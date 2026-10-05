# Benchmark results

Measured on an Apple M1 (8 cores, 8 GB) with JDK 23, using the code in
`cache-benchmark`. These were short runs on a laptop (1 fork, 2 warm-up and
3 measurement iterations), so treat the throughput figures as indicative.
The error column is JMH's 99.9% confidence interval; where it is larger than
the differences between rows, the comparison is inconclusive.

## Hit rate

Read-through access (on a miss the value is loaded and inserted), a cache of
256 entries over 1,000 distinct keys, one million requests. Produced by
`HitRateSimulation`; the JMH `readThrough_*` benchmarks report the same rates.

| Workload | LRU | LFU | ARC |
|---|---|---|---|
| Zipf, s = 1.0 | 74.2% | 81.0% | 76.6% |
| Uniform | 25.5% | 26.1% | 25.4% |
| 1,000-key sequential scan, then 2,000 Zipf requests, repeated | 51.0% | 61.7% | 53.7% |

- With a fixed popularity distribution, counting accesses (LFU) beats
  tracking recency. ARC sits between the two.
- With uniform access no policy can do better than capacity / keys = 25.6%,
  which is a useful check that the simulation is right.
- The scan floods LRU with keys that are never used again. LFU keeps its hot
  keys because scanned keys only reach a count of one. ARC recovers part of
  the gap because scanned keys stay in its "seen once" list.

## Single-threaded throughput

`PolicyComparisonBenchmark`, operations per microsecond (higher is better):

| Benchmark | LRU | LFU | ARC |
|---|---|---|---|
| `get_zipf` | 36.2 ± 0.7 | 11.2 ± 13.5 | 39.1 ± 76.5 |
| `put_zipf` | 16.3 ± 23.4 | 9.7 ± 15.4 | 11.0 ± 6.2 |
| `readThrough_zipf` | 18.2 ± 2.3 | 9.5 ± 0.7 | 11.3 ± 8.2 |

LRU is the cheapest per operation: a hit is one hash lookup and one list
move. LFU does more work per hit (moving the entry between frequency
buckets and the occasional decay pass), which shows in the tighter
`readThrough_zipf` numbers.

## Network round trip

`NetworkBenchmark`: four client threads using a pooled `CacheClient` against
the real server on localhost, keys drawn from a Zipf distribution.

| Operation | p50 | p99 | Throughput |
|---|---|---|---|
| `PING` | 0.059 ms | 0.200 ms | ~69 ops/ms |
| `GET` | 0.067 ms | 0.214 ms | ~67 ops/ms |
| `PUT` | 0.062 ms | 0.199 ms | ~59 ops/ms |

The latency is dominated by the TCP round trip and thread wake-ups rather
than by the cache itself, which `STATS` reports in single-digit microseconds.

## Concurrency

`ConcurrencyBenchmark` with 8 threads produced error bars wider than the
differences between `CoarseGrainedCache`, `SegmentedCache` and
`LockFreeCache` on this machine, so no ranking is claimed here. To measure
it properly, use more forks and longer iterations on a machine with nothing
else running:

```bash
java -jar cache-benchmark/target/benchmarks.jar Concurrency -f 3 -wi 5 -i 10 -r 5s -t 8
```

## Reproducing

```bash
mvn package -pl cache-benchmark -am -DskipTests

java -cp cache-benchmark/target/benchmarks.jar com.cache.bench.HitRateSimulation
java -jar cache-benchmark/target/benchmarks.jar PolicyComparison
java -jar cache-benchmark/target/benchmarks.jar Network
```
