# JCache Wire Protocol Specification

**Version:** 1.0  
**Transport:** TCP (plain text, newline-delimited)  
**Default Port:** 6379  
**Encoding:** UTF-8

---

## Overview

JCache uses a plain-text, newline-delimited protocol inspired by Redis's inline command format. Commands are human-readable ASCII strings terminated by `\r\n` (CRLF). This makes the protocol trivially debuggable with `telnet` or `nc` without any special tools.

Every response begins with a single-character status prefix:
- `+` — success
- `-` — error (always prefixed with `-ERR`)

---

## Connection

Connect to the server via TCP on the configured port (default 6379).

```bash
telnet localhost 6379
```

No authentication handshake is required. The server accepts commands immediately after the TCP connection is established.

The server does not send a greeting message. The first bytes from the server will be a response to the client's first command.

---

## Command Format

```
VERB [arg1] [arg2] [arg3]\r\n
```

- All tokens are separated by one or more whitespace characters (space or tab).
- Command verbs are **case-insensitive** (`GET`, `get`, and `Get` are identical).
- Keys and values are **case-sensitive** and treated as opaque strings.
- Values **cannot contain spaces** (space is the argument delimiter). Use Base64 encoding for values with spaces or binary data.
- Lines are terminated with `\r\n` (CRLF). Servers also accept `\n` (LF) for compatibility with clients that don't send CR.
- Maximum line length: **8192 bytes**. Lines exceeding this are rejected with `-ERR`.

---

## Response Format

### Success Response

```
+{data}\r\n
```

`{data}` is either a literal value (`OK`, `PONG`) or the retrieved cache value.

### Error Response

```
-ERR {human-readable message}\r\n
```

Error messages are human-readable strings. They are not machine-parseable codes — clients should check only the `-ERR` prefix, not the full message text (which may change between server versions).

---

## Commands

### PING

Health check. Verifies the server is alive and responding to commands.

**Syntax:**
```
PING\r\n
```

**Response:**
```
+PONG\r\n
```

**Example:**
```
> PING
+PONG
```

**Use case:** Connection health checks, latency measurement (`time echo "PING" | nc localhost 6379`).

---

### GET

Retrieve a value by key.

**Syntax:**
```
GET key\r\n
```

**Arguments:**
| Argument | Required | Description |
|----------|----------|-------------|
| key      | Yes      | The cache key to look up |

**Response — key exists:**
```
+{value}\r\n
```

**Response — key not found (or expired):**
```
-ERR key not found: {key}\r\n
```

**Example:**
```
> GET username
+alice

> GET nonexistent
-ERR key not found: nonexistent
```

**Notes:**
- If the key has a TTL and it has elapsed, the server returns `-ERR key not found` and removes the key. This is **lazy eviction** — the key is cleaned up at read time.
- GET does not reset TTL. The key's expiry timestamp is unchanged by a GET.

---

### PUT

Store a key-value pair, optionally with a time-to-live.

**Syntax:**
```
PUT key value\r\n
PUT key value ttlSeconds\r\n
```

**Arguments:**
| Argument   | Required | Description |
|------------|----------|-------------|
| key        | Yes      | The cache key. Cannot contain spaces. |
| value      | Yes      | The value to store. Cannot contain spaces. |
| ttlSeconds | No       | Time-to-live in seconds. 0 = no expiry. Must be a non-negative integer. |

**Response:**
```
+OK\r\n
```

**Examples:**
```
> PUT username alice
+OK

> PUT session_token abc123 3600
+OK

> PUT counter 0 0
+OK
```

**Notes:**
- If the key already exists, the value is **overwritten** and the TTL is **replaced**.
- If `ttlSeconds` is omitted, the key has no expiry and persists until evicted by the eviction policy (LRU/LFU/ARC).
- If `ttlSeconds` is 0, the key has no expiry (equivalent to omitting TTL).
- If PUT causes the cache to exceed capacity, the eviction policy selects a key to remove before inserting the new entry.

**Error cases:**
```
> PUT key
-ERR 'PUT' requires 2 to 3 argument(s), but got 1

> PUT key value badttl
-ERR TTL must be a non-negative integer (seconds), got: 'badttl'

> PUT key value -1
-ERR TTL must be non-negative, got: -1
```

---

### DELETE

Remove a key from the cache.

**Syntax:**
```
DELETE key\r\n
```

**Arguments:**
| Argument | Required | Description |
|----------|----------|-------------|
| key      | Yes      | The key to remove. |

**Response:**
```
+OK\r\n
```

**Example:**
```
> DELETE username
+OK

> DELETE nonexistent_key
+OK
```

**Notes:**
- DELETE is **idempotent**. Deleting a key that doesn't exist returns `+OK`, not an error. This is safe for clients that retry on network failures.
- Deleting a key also removes its TTL entry.

---

### STATS

Return current cache statistics.

**Syntax:**
```
STATS\r\n
```

**Response:**
```
+hits:{N} misses:{N} evictions:{N} size:{N} hitRate:{N.NN}%\r\n
```

**Response fields:**
| Field      | Description |
|------------|-------------|
| hits       | Total successful GET operations (key found, not expired) |
| misses     | Total failed GET operations (key not found or expired) |
| evictions  | Total keys removed by the eviction policy (not by DELETE or TTL) |
| size       | Current number of entries in the cache |
| hitRate    | hits / (hits + misses) × 100, formatted to 2 decimal places |

**Example:**
```
> STATS
+hits:1420 misses:231 evictions:88 size:256 hitRate:86.00%
```

**Notes:**
- Stats are cumulative from server start. There is no reset mechanism in v1.0.
- `size` is an approximation — it may include keys that are expired but not yet swept by the background sweeper. The next GET on those keys will trigger lazy eviction.
- `evictions` counts only policy-driven evictions (capacity overflow), not TTL-based expirations or explicit DELETE calls.

---

### FLUSH

Remove all entries from the cache.

**Syntax:**
```
FLUSH\r\n
```

**Response:**
```
+OK\r\n
```

**Example:**
```
> FLUSH
+OK
```

**Notes:**
- FLUSH is **destructive and immediate**. There is no confirmation prompt.
- Stats counters (hits, misses, evictions) are NOT reset by FLUSH — only the key-value data is cleared.
- Use with caution in production. Intended for development and testing.

---

## Error Reference

All errors follow the format `-ERR {message}\r\n`.

| Error Message Pattern                         | Cause |
|-----------------------------------------------|-------|
| `Unknown command '{verb}'. Valid commands: ...`| Sent an unrecognized command verb |
| `'GET' requires exactly 1 argument(s), but got 0` | Sent `GET` with no key |
| `'PUT' accepts 2 to 3 argument(s), but got 4` | Sent too many arguments |
| `TTL must be a non-negative integer, got: 'abc'` | Non-numeric TTL |
| `TTL must be non-negative, got: -5`           | Negative TTL |
| `key not found: {key}`                        | GET on missing or expired key |
| `internal server error`                       | Unexpected server error (check server logs) |
| `server at capacity, try again later`         | Too many concurrent connections |
| `Empty command`                               | Sent blank line or whitespace only |

---

## Session Example

A complete client session demonstrating all commands:

```
$ telnet localhost 6379

> PING
+PONG

> PUT user:1:name Alice 300
+OK

> PUT user:1:age 30
+OK

> GET user:1:name
+Alice

> GET user:1:age
+30

> STATS
+hits:2 misses:0 evictions:0 size:2 hitRate:100.00%

> DELETE user:1:age
+OK

> GET user:1:age
-ERR key not found: user:1:age

> STATS
+hits:2 misses:1 evictions:0 size:1 hitRate:66.67%

> FLUSH
+OK

> GET user:1:name
-ERR key not found: user:1:name

> STATS
+hits:2 misses:2 evictions:0 size:0 hitRate:50.00%
```

---

## Protocol Limitations

These are known limitations of v1.0. Potential improvements are noted.

| Limitation | Detail | Potential Fix |
|------------|--------|---------------|
| No binary values | Values cannot contain `\r\n` — they would be interpreted as line endings | Use Base64 encoding, or implement RESP bulk strings |
| No spaces in values | Space is the argument delimiter | Quote-delimited values, or length-prefixed binary protocol |
| No pipelining | Each command is fully processed before the next is read | Connection is synchronous — one outstanding request per connection |
| No authentication | Any client can connect and read/write all keys | Add `AUTH password` command |
| No key namespaces | All keys share one flat namespace | Add key prefixes or logical databases (`SELECT 1`) |
| Stats not resettable | Cumulative only | Add `RESET STATS` command |
| No persistence | Data lost on server restart | See Day 19: AOF persistence layer |

---

## Comparison with Redis Inline Commands

JCache's protocol is a subset of Redis's inline command format. The main differences:

| Feature | JCache v1.0 | Redis |
|---------|-------------|-------|
| Line terminator | `\r\n` (also accepts `\n`) | `\r\n` |
| Success prefix | `+` | `+` |
| Error prefix | `-ERR` | `-ERR` |
| Binary values | Not supported | Supported via bulk strings |
| Pipelining | Not supported | Supported |
| Authentication | Not supported | Optional (requirepass) |
| Multiple databases | Not supported | 16 databases (SELECT 0-15) |

A Redis client library **cannot** be used with JCache without modification — this protocol is inspired by Redis, not compatible with it.

---

## Implementation Notes for Client Authors

**Connecting:**
```python
import socket
s = socket.socket()
s.connect(("localhost", 6379))
```

**Sending a command:**
```python
s.sendall(b"PUT name Alice\r\n")
```

**Reading a response:**
```python
response = b""
while not response.endswith(b"\r\n"):
    response += s.recv(1024)
line = response.decode("utf-8").strip()  # strips \r\n
```

**Checking success:**
```python
if line.startswith("+"):
    value = line[1:]   # strip "+" prefix
elif line.startswith("-"):
    raise Exception(line[5:])  # strip "-ERR " prefix
```

**Complete Python example:**
```python
import socket

class JCacheClient:
    def __init__(self, host="localhost", port=6379):
        self.sock = socket.socket()
        self.sock.connect((host, port))
        self.file = self.sock.makefile("rb")

    def _send(self, command):
        self.sock.sendall((command + "\r\n").encode("utf-8"))
        line = self.file.readline().decode("utf-8").strip()
        if line.startswith("+"):
            return line[1:]
        elif line.startswith("-"):
            raise Exception(line[5:])  # strip "-ERR "

    def ping(self):          return self._send("PING")
    def get(self, key):      return self._send(f"GET {key}")
    def put(self, k, v, t=0):return self._send(f"PUT {k} {v} {t}")
    def delete(self, key):   return self._send(f"DELETE {key}")
    def stats(self):         return self._send("STATS")
    def flush(self):         return self._send("FLUSH")
    def close(self):         self.sock.close()

# Usage
client = JCacheClient()
client.put("name", "Alice", 60)
print(client.get("name"))   # Alice
print(client.stats())       # hits:1 misses:0 ...
client.close()
```