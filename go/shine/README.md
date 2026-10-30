# Shine protocol for Go

A standalone implementation of the Shine v1 wire protocol in Go. It uses the Go
standard library only and does not depend on Xray. It implements the RESP2
subset, address encoding, client/server handshake, authentication, multiplexed
TCP sessions, per-session flow control, UDP datagrams, heartbeats and shutdown.

The module path is `github.com/hhhflow2020/shine/go/shine`. Until a suitable
version is published, another module can use a local `replace` pointing to this
directory. Go 1.22 or newer is required. For example, while working from a
local checkout, the consuming project's `go.mod` can include:

```go
require github.com/hhhflow2020/shine/go/shine v0.0.0
replace github.com/hhhflow2020/shine/go/shine => /absolute/path/to/shine/go/shine
```

## Client example

```go
conn, err := net.Dial("tcp", "127.0.0.1:7011")
if err != nil { return err }
link, err := shine.Client(ctx, conn, shine.Options{Password: "secret"})
if err != nil { return err } // Client closes conn on failure too.
defer link.Close()

target, err := shine.ParseAddress("example.com:443")
if err != nil { return err }
stream, err := link.Open(ctx, target)
if err != nil { return err }
defer stream.Close()
_, err = stream.Write([]byte("hello"))
// stream.Read reads the response; stream.CloseWrite sends EOF without closing
// the read side. Keep link alive until the session has finished.
return err
```

`OpenUDP(ctx)` creates a UDP session. `SendDatagram(ctx, payload, target)` and
`ReceiveDatagram(ctx)` preserve packet boundaries and carry the address of each
packet. UDP payloads are limited to 65,535 bytes; packets are dropped when the
per-session receive queue is full.

## Server example

Call `Server` once for each accepted transport, then `Accept` repeatedly:

```go
link, err := shine.Server(ctx, conn, shine.Options{Password: "secret"})
if err != nil { return err }
defer link.Close()
for {
    session, err := link.Accept(ctx)
    if err != nil { return err }
    go handle(session) // route session.Target() for TCP; inspect each UDP packet.
}
```

For UDP, `Target()` is empty because `NEW_UDP` does not carry a destination;
use `ReceiveDatagram` to obtain it. The C++ proxy chooses a UDP route based on
the first datagram and keeps that route for the association.

## User credentials

The server can authenticate several users, each with one `Auth` secret:

```go
opts := shine.Options{
    Users: []shine.User{
        {Auth: "alice-secret", Email: "alice@example.com"},
        {Auth: "bob-secret", Email: "bob@example.com"},
    },
}
link, err := shine.Server(ctx, conn, opts)
if err != nil { return err }
identity, authenticated := link.AuthenticatedUser()
// identity contains the configured email and level, never the auth secret.
```

`Options.Password` remains the client credential and supports the legacy single
server password; it cannot be combined with `Users` on a server. A user's `Auth`
must be nonempty and unique; nonempty email values must also be unique. Email
and level are optional. `ValidateServerCredentials` checks these constraints.
`AuthenticatedUser` returns `ok=false` for an unauthenticated server or a client.
For a legacy plain password, `ok=true` and `identity.User=false`; a configured
user has `identity.User=true`, even if its email is empty.

The v1 wire format transmits only the auth secret. The server maps it to its
local email and level, so email alone is not a credential. Existing Shine peers
can authenticate without a wire-format change.

## Redis-like transport

The optional [Redis-like transport](redis/README.md) wraps the Shine byte stream
in RESP2 publish/subscribe-style messages. It uses two connections and can apply
zlib compression when that reduces a message's size. It is opt-in, does not add
encryption, and requires support on both sides; the ordinary Go library TCP
transport is unchanged. The Xray adapter selects the Redis-style transport by
default when its `transport` setting is omitted.

## Embedding and lifecycle

- `Client` and `Server` accept an established `io.ReadWriteCloser` and take
  ownership even if the handshake fails. The transport must support concurrent
  reading and writing, and `Close` must unblock both. A `net.Conn` or an
  already-established TLS connection satisfies this contract.
- Session implements `io.Reader`, `io.Writer` and `io.Closer`, plus `CloseWrite`.
  It does not implement `net.Conn`. TCP reads/writes are serialized per direction;
  they may run concurrently with each other. `ReadContext` and `WriteContext`
  cancel waits for data, credit, the send queue, or another operation in the
  same direction without closing the session. If a read has already consumed
  bytes when its window update is canceled, it returns those bytes with the
  context error; the next read retries the pending update.
- A successful write means the frame was queued, not that the peer received it.
  Keep the link open while sessions are in use. `Link.Close` immediately closes
  the transport, aborts sessions and may discard queued frames. `Link.Done` and
  `Link.Err` report termination. `Session.Close` releases a session, whereas
  `CloseWrite` supports TCP half-close and keeps the read side available.
- `MaxSessions` bounds active TCP and UDP sessions per link (default 256). A
  local `Open` at capacity returns `ErrSessionLimit`; an excess peer-created
  session is reset without closing the link. `AcceptQueue` also bounds sessions
  awaiting acceptance. To share a total limit across links, create a
  `SessionLimiter` with `NewSessionLimiter` and pass the same pointer in each
  link's options. Links and sessions release their slots when closed.
- TCP data is buffered up to `InitialWindow` per session; a peer exceeding it
  has its session reset. UDP receive queues allocate on demand and are bounded
  by both `DatagramQueue` (default 1,024 packets) and `DatagramQueueBytes`
  (default 256 KiB, including the payload, address and 64 bytes per packet).
  Packets exceeding either bound are dropped. These are per-session limits;
  embedders must also limit the number of physical connections.
- `Open` sends NEW and returns without waiting for ACK, matching the existing C++
  implementation. It does not confirm the target was successfully connected.
- Set the same `InitialWindow` on both ends when interoperating with the C++
  implementation: its ACK does not advertise the receiving peer's window.
- Defaults match C++: 256 KiB window and DATA payload, 512 MiB maximum RESP
  bulk, 5 second handshake timeout, 15 second ping and 10 second pong timeout.
  `MaxFrameSize` additionally limits the **sum** of array contents and must be
  at least `MaxDataSize + 9`. Frame bodies are allocated incrementally as data
  arrives. Before authentication, handshake frames are limited to 64 KiB, or
  the configured authentication secret's size if larger, up to `MaxFrameSize`.
  Authenticated peers can still send bodies up to `MaxFrameSize`, so lower it
  for deployments that do not need large frames. `PongTimeout` includes time
  waiting to queue the heartbeat; a stalled send queue therefore terminates
  the link instead of leaving operations such as `Close` blocked indefinitely.
- Authentication sends a password over the transport; it does not encrypt or
  authenticate the transport itself. Use a secure transport such as TLS when
  confidentiality and peer authentication are required.

The separate [Xray integration](../xray/README.md) implements inbound, outbound,
and JSON configuration registration for a pinned Xray-core revision.

## Testing

```sh
go test -race ./...
# Optional integration tests against the existing C++ executable:
SHINE_CPP_BIN=/absolute/path/to/shine go test -race ./...
```

The integration tests start the supplied binary with temporary configuration and
exercise Go client → C++ server TCP/UDP and C++ client → Go server TCP/UDP. The
binary should be built from the intended C++ revision before relying on the
result. The tests use local sockets and are skipped if `SHINE_CPP_BIN` is unset.
