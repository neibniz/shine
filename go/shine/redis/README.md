# Redis-like transport

This optional transport wraps a Shine byte stream in RESP2 `AUTH`, `SUBSCRIBE`,
`PUBLISH`, subscription acknowledgements and message pushes. A client opens two
connections: one subscribes for downloads, the other publishes uploads. Both
connections authenticate separately and are paired using two related random
channels (one for each direction); if credentials are configured, both must authenticate as the same user.

It is a **Redis-like carrier**, not a Redis server or a transport through an
existing Redis instance. It also simulates a limited set of RESP2 commands on
connections before they are paired:

- `AUTH`, `HELLO 2`, `PING`, `ECHO`, `QUIT`, `SELECT`, and `INFO`;
- `CLIENT ID`, `CLIENT GETNAME`, `CLIENT SETNAME` and `CLIENT SETINFO`;
- `GET`, `SET` with `NX`/`XX` and `EX`/`PX`, `DEL`, `EXISTS`, `TYPE`,
  `DBSIZE`, `MGET`, `EXPIRE`, `PEXPIRE`, `TTL` and `PTTL`;
- `PUBLISH` to an unrelated channel returns zero subscribers.

The simulated database is **temporary**, shared by simulated command
connections on one acceptor, and lost on restart. It supports databases 0–15
and is limited to 1,024 entries and 4 MiB of key/value data. `MGET` response
content is also limited to 4 MiB. Expired entries
are cleaned lazily. It is not suitable for real application storage. `INFO`
returns a limited synthetic description whose reported Redis version is 7.2.10;
RESP3 and unsupported commands or options are not implemented. Simulated
command connections have a two-minute idle timeout by default. After a carrier
connection is paired, only its transport-specific commands are supported.

The pairing message and payload envelope are Shine-specific; unimplemented
commands, payload contents, timing, and traffic patterns can reveal the
protocol. This mode does not encrypt traffic. In particular, `AUTH` and
uncompressed payloads are visible on a plaintext connection; compression is
not encryption.

Each logical write is split into messages up to 2 MiB by default. With outgoing
compression enabled, zlib is used only when the write is at least 128 bytes and
saves at least 16 bytes; otherwise the payload is sent uncompressed. Every
receiver supports either form, so each endpoint can enable compression
independently. Decompression is bounded by `MaxPayload`, which must be the
same at both ends or set high enough for the other end's messages. Compressed,
already-compressed or small data may not save bandwidth; framing, responses and
the second connection add overhead. `PUBLISH` waits for its Redis integer reply,
so latency and throughput should be benchmarked on the intended network.

The subscriber pairing wait and initial handshake have separate timeouts; the
server also bounds the number of waiting pairs. Closing the acceptor closes
pending pairs and active paired streams. `AcceptConn` owns each accepted
connection: the publisher invocation returns the paired stream, while the
subscriber invocation waits for it to close and returns `(nil, nil)`.

## Go integration

Client: provide a dial function that opens a new connection on each call. The
result is an `io.ReadWriteCloser` that can be passed to `shine.Client`:

```go
carrier, err := redis.Client(ctx, func(ctx context.Context) (io.ReadWriteCloser, error) {
    return (&net.Dialer{}).DialContext(ctx, "tcp", serverAddress)
}, redis.Options{Password: password, Compression: true})
if err != nil { return err }
link, err := shine.Client(ctx, carrier, shine.Options{Password: password})
if err != nil { return err }
defer link.Close()
```

On the server, construct one shared `redis.Acceptor` and call `AcceptConn` in a
separate goroutine for **each** accepted physical connection. For the one call
returning a non-nil carrier, pass it to `shine.Server`; the other call remains
active until the carrier closes. The acceptor's authenticator must validate the
same credentials as `shine.Server`. Xray implements this wiring in the adapter.

Optional integration tests can compare supported responses against Redis 7.2
and exercise the real CLI by setting `SHINE_REDIS_SERVER` and
`SHINE_REDIS_CLI` to their executable paths when running `go test`.

Both sides must use the same transport. This package is opt-in for standalone Go
callers; the Xray adapter selects it by default when `transport` is omitted.
The previous TCP mode is not wire-compatible with it, and the existing C++
implementation does not support it.
