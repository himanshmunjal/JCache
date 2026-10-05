# Wire protocol

JCache speaks a line-based text protocol over TCP, similar to Redis's inline
commands. You can use it with `nc` or `telnet`; no client library is needed.

- One command per line, terminated by `\n` or `\r\n`. Lines are UTF-8.
- Tokens are separated by whitespace. Verbs are case-insensitive, keys are not.
- Every reply is one line ending in `\r\n`: `+payload` on success,
  `-ERR message` on failure.
- Lines longer than 1 MiB are rejected with `-ERR line too long`; the
  connection stays open.

The parser lives in `cache-common` (`CommandParser`) and is the same code the
server runs, so the rules below are exactly what is enforced.

## Commands

| Command | Reply | Notes |
|---|---|---|
| `PING` | `+PONG` | |
| `GET key` | `+value` or `-ERR key not found` | |
| `PUT key value [ttl]` | `+OK` | Alias `SET`. See [values and TTLs](#values-and-ttls). |
| `DELETE key` | `+OK` | Alias `DEL`. Succeeds whether or not the key existed. |
| `EXPIRE key seconds` | `+OK` or `-ERR key not found` | `EXPIRE key 0` deletes the key. |
| `TTL key` | `+seconds`, `+-1`, or `-ERR key not found` | `-1` means the key never expires. |
| `PERSIST key` | `+OK` or `-ERR key not found` | Removes the key's TTL. |
| `STATS` | `+name:value name:value ...` | See [STATS](#stats). |
| `FLUSH` | `+OK` | Removes every key. |
| `QUIT` | `+BYE`, then the server closes the connection | Alias `EXIT`. |

`TTL key seconds` (two arguments) is accepted as a synonym for `EXPIRE`, for
clients written against earlier versions.

## Values and TTLs

The value of a `PUT` is everything after the key, with inner spacing
preserved and the ends trimmed:

```
PUT greeting hello   world      -> value "hello   world"
```

If the value has more than one token and the last one is a non-negative
integer, that integer is the TTL in seconds:

```
PUT session abc123 3600         -> value "abc123", expires in one hour
PUT answer 42                   -> value "42", no TTL (only one token)
PUT order item 42               -> value "item", TTL 42 s
PUT order item 42 0             -> value "item 42", no TTL
```

A TTL of 0 means "use the server's default", which is "never expire" unless
the server was started with `JCACHE_DEFAULT_TTL`. The Java client always sends
the TTL explicitly, so values ending in a number are never misread.

Because the protocol is line-based, values cannot contain line breaks.

## Errors

| Situation | Reply |
|---|---|
| Unknown verb | `-ERR unknown command 'FOO'` |
| Wrong number of arguments | `-ERR wrong number of arguments for GET: expected 1 argument, got 0` |
| Bad TTL in `EXPIRE` | `-ERR TTL must be a non-negative integer, got 'soon'` |
| Server full when connecting | `-ERR max connections reached`, then the connection is closed |

## STATS

`STATS` returns space-separated `name:value` pairs:

```
+hits:142 misses:31 evictions:8 size:870 gets:173 puts:900 deletes:12 errors:0
 hitRate:82.08% opsPerSec:310.5 activeConn:2 totalConn:9 uptime:3601000ms
 p50:0.004ms p99:0.031ms mean:0.006ms
```

(Shown wrapped; the reply is a single line.)

| Field | Meaning |
|---|---|
| `hits`, `misses`, `gets` | `GET` requests and their outcome |
| `puts`, `deletes`, `errors` | other request counts |
| `evictions` | entries removed by the eviction policy, expiry or `DELETE` |
| `size` | entries currently stored |
| `hitRate` | `hits / gets` as a percentage |
| `opsPerSec` | average since start-up |
| `activeConn`, `totalConn` | open connections, and all connections accepted |
| `uptime` | milliseconds since start-up |
| `p50`, `p99`, `mean` | time spent inside the cache over the last 1000 operations; network time is not included |

## Example session

```
$ nc localhost 6379
PUT user:1 Alice Smith
+OK
GET user:1
+Alice Smith
PUT session:9 token 60
+OK
TTL session:9
+59
PERSIST session:9
+OK
TTL session:9
+-1
DEL user:1
+OK
GET user:1
-ERR key not found
QUIT
+BYE
```

## RESP

`cache-common` also contains a RESP (Redis protocol) decoder and encoder.
They are tested but not yet wired into the server; see the roadmap in the
README.
