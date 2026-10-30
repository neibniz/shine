package shine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestUnfinishedLargeFrameDoesNotPreallocate(t *testing.T) {
	// Before authentication, a peer can declare a large body and never send it.
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := readFrame(bufio.NewReader(strings.NewReader("$67108864\r\n")), maxBulk)
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Fatal("accepted a frame without its body")
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 8<<20 {
		t.Fatalf("declared-only frame allocated %d bytes", allocated)
	}
}

func TestReadBulkIncrementalAndTruncated(t *testing.T) {
	payload := bytes.Repeat([]byte("fragmented-payload"), 20000)
	wire := []byte(fmt.Sprintf("$%d\r\n", len(payload)))
	wire = append(wire, payload...)
	wire = append(wire, '\r', '\n')
	got, err := readBulk(bufio.NewReaderSize(bytes.NewReader(wire), 31), len(payload))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("large frame: %v, len=%d", err, len(got))
	}
	for _, input := range []string{"$4\r\nab", "$2\r\nab\r", "$2\r\nab!!"} {
		if _, err := readBulk(bufio.NewReader(strings.NewReader(input)), 8); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
}

func waitGateHeld(t *testing.T, gate chan struct{}) {
	t.Helper()
	deadline := time.After(time.Second)
	for len(gate) != 0 {
		select {
		case <-deadline:
			t.Fatal("operation did not acquire gate")
		default:
			runtime.Gosched()
		}
	}
}
func expectDeadline(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v, want deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("operation did not observe deadline")
	}
}

func TestConcurrentReadWriteCancellation(t *testing.T) {
	client, server := pair(t, Options{InitialWindow: 1, MaxDataSize: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a, err := client.Open(ctx, Address{"example.com", 443})
	if err != nil {
		t.Fatal(err)
	}
	b, err := server.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	firstRead := make(chan error, 1)
	go func() { _, err := b.Read(make([]byte, 1)); firstRead <- err }()
	waitGateHeld(t, b.readGate)
	readCtx, stopRead := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stopRead()
	secondRead := make(chan error, 1)
	go func() { _, err := b.ReadContext(readCtx, make([]byte, 1)); secondRead <- err }()
	expectDeadline(t, secondRead)
	if _, err := a.Write([]byte("a")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-firstRead:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	c, s := pair(t, Options{InitialWindow: 1, MaxDataSize: 1})
	writer, err := c.Open(ctx, Address{"example.com", 443})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan error, 1)
	go func() { _, err := writer.Write([]byte("ab")); blocked <- err }()
	waitGateHeld(t, writer.writeGate)
	writeCtx, stopWrite := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stopWrite()
	secondWrite := make(chan error, 1)
	go func() { _, err := writer.WriteContext(writeCtx, []byte("x")); secondWrite <- err }()
	expectDeadline(t, secondWrite)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("blocked write succeeded")
		}
	case <-ctx.Done():
		t.Fatal("blocked writer remained stuck")
	}
}

func TestDatagramOperationsCancelWhileWaitingForGate(t *testing.T) {
	c, s := pair(t, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	a, err := c.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { _, err := b.ReceiveDatagram(ctx); first <- err }()
	waitGateHeld(t, b.readGate)
	deadline, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stop()
	second := make(chan error, 1)
	go func() { _, err := b.ReceiveDatagram(deadline); second <- err }()
	expectDeadline(t, second)
	if err = a.SendDatagram(ctx, []byte("ok"), Address{"127.0.0.1", 53}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestCanceledWindowUpdateIsRetried(t *testing.T) {
	opts, err := (Options{InitialWindow: 2}).defaults()
	if err != nil {
		t.Fatal(err)
	}
	l := &Link{opts: opts, send: make(chan []byte, 2), done: make(chan struct{}), sessions: make(map[uint64]*Session)}
	l.send <- []byte("occupied1")
	l.send <- []byte("occupied2")
	s := newSession(l, 42, Address{}, false, 2)
	if !s.deliver([]byte("ab")) {
		t.Fatal("data rejected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	b := make([]byte, 1)
	n, err := s.ReadContext(ctx, b)
	if n != 1 || string(b) != "a" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("read %d %q: %v", n, b, err)
	}
	<-l.send
	<-l.send
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if n, err = s.ReadContext(ctx2, b); n != 1 || string(b) != "b" || err != nil {
		t.Fatalf("second read %d %q: %v", n, b, err)
	}
	for i := 0; i < 2; i++ {
		f, err := readFrame(bufio.NewReader(bytes.NewReader(<-l.send)), 1024)
		if err != nil || f.cmd != "WINDOW" || binary.BigEndian.Uint32(f.args[1]) != 1 {
			t.Fatalf("window %+v: %v", f, err)
		}
	}
}

func TestDatagramQueueIsLazyAndBounded(t *testing.T) {
	opts, err := (Options{DatagramQueue: 3, DatagramQueueBytes: 140}).defaults()
	if err != nil {
		t.Fatal(err)
	}
	l := &Link{opts: opts, done: make(chan struct{}), send: make(chan []byte, 4), sessions: make(map[uint64]*Session)}
	s := newSession(l, 1, Address{}, true, 0)
	if s.datagrams != nil {
		t.Fatal("empty queue allocated storage")
	}
	s.deliverDatagram(Datagram{Data: []byte("a")}) // 65 accounted bytes
	s.deliverDatagram(Datagram{Data: []byte("b")})
	s.deliverDatagram(Datagram{Data: []byte("c")}) // dropped by the byte limit
	if got := len(s.datagrams) - s.datagramHead; got != 2 {
		t.Fatalf("queued %d packets", got)
	}
	if got, err := s.ReceiveDatagram(context.Background()); err != nil || string(got.Data) != "a" {
		t.Fatalf("first packet %+v: %v", got, err)
	}
	s.deliverDatagram(Datagram{Data: []byte("d")})
	for _, want := range []string{"b", "d"} {
		got, err := s.ReceiveDatagram(context.Background())
		if err != nil || string(got.Data) != want {
			t.Fatalf("packet %+v: %v", got, err)
		}
	}
	for i := 0; i < 10; i++ {
		s.deliverDatagram(Datagram{})
	}
	if got := len(s.datagrams) - s.datagramHead; got != 2 {
		t.Fatalf("empty packets not byte bounded: %d", got)
	}
	s.abort(io.ErrClosedPipe)
	if s.datagrams != nil || s.datagramBytes != 0 {
		t.Fatal("abort retained queued datagrams")
	}
}

func pairOptions(t *testing.T, clientOpts, serverOpts Options) (*Link, *Link) {
	t.Helper()
	a, b := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type result struct {
		link *Link
		err  error
	}
	ready := make(chan result, 1)
	go func() { l, e := Server(ctx, b, serverOpts); ready <- result{l, e} }()
	client, err := Client(ctx, a, clientOpts)
	if err != nil {
		t.Fatal(err)
	}
	r := <-ready
	if r.err != nil {
		client.Close()
		t.Fatal(r.err)
	}
	t.Cleanup(func() { client.Close(); r.link.Close() })
	return client, r.link
}

func TestPerLinkLimitAndSlotReuse(t *testing.T) {
	client, server := pair(t, Options{MaxSessions: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	one, err := client.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	peerOne, err := server.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = server.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.OpenUDP(ctx); !errors.Is(err, ErrSessionLimit) {
		t.Fatalf("expected limit: %v", err)
	}
	if err = one.Close(); err != nil {
		t.Fatal(err)
	}
	if err = peerOne.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := client.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if _, err = server.Accept(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestServerRejectsExcessWithoutClosingLink(t *testing.T) {
	c, s := pairOptions(t, Options{MaxSessions: 3}, Options{MaxSessions: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first, err := c.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := s.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	excess, err := c.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = excess.ReceiveDatagram(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("excess was not reset: %v", err)
	}
	if err = peer.Close(); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	again, err := c.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if _, err = s.Accept(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSharedSessionLimitAcrossLinksAndCleanup(t *testing.T) {
	limiter, err := NewSessionLimiter(2)
	if err != nil {
		t.Fatal(err)
	}
	c1, s1 := pairOptions(t, Options{}, Options{SessionLimiter: limiter})
	c2, s2 := pairOptions(t, Options{}, Options{SessionLimiter: limiter})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err = c1.OpenUDP(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := s1.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c2.OpenUDP(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := s2.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	excess, err := c2.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = excess.ReceiveDatagram(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared limit did not reset session: %v", err)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := c2.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if _, err = s2.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	if err = second.Close(); err != nil {
		t.Fatal(err)
	}
	s2.Close()
	s1.Close()
	if n := limiter.active.Load(); n != 0 {
		t.Fatalf("limiter leaked %d slots", n)
	}
}

func TestHandshakeRejectsLargeFrameBeforeBody(t *testing.T) {
	a, b := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer a.Close()
	result := make(chan error, 1)
	go func() { _, err := Server(ctx, b, Options{Password: "secret"}); result <- err }()
	// The peer does not send the declared body. The handshake limit must
	// reject the length immediately instead of waiting or allocating it.
	if _, err := io.WriteString(a, "*3\r\n$5\r\nHELLO\r\n$67108864\r\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("server waited for oversized handshake body")
	}
}

func TestLargeConfiguredSecretCanAuthenticate(t *testing.T) {
	secret := strings.Repeat("a", 80<<10)
	client, server := pair(t, Options{Password: secret})
	if client == nil || server == nil {
		t.Fatal("long configured credential did not authenticate")
	}
}

func TestBlockedDatagramWriteCancellation(t *testing.T) {
	opts, err := (Options{}).defaults()
	if err != nil {
		t.Fatal(err)
	}
	l := &Link{opts: opts, done: make(chan struct{}), send: make(chan []byte, 1), sessions: make(map[uint64]*Session)}
	l.send <- []byte("blocked")
	s := newSession(l, 1, Address{}, true, 0)
	first := make(chan error, 1)
	go func() { first <- s.SendDatagram(context.Background(), []byte("a"), Address{"127.0.0.1", 53}) }()
	waitGateHeld(t, s.writeGate)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- s.SendDatagram(ctx, []byte("b"), Address{"127.0.0.1", 53}) }()
	expectDeadline(t, second)
	s.abort(io.ErrClosedPipe)
	select {
	case err := <-first:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("first send error %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked send was not aborted")
	}
}

func TestSharedLimitReleasedDuringConcurrentClose(t *testing.T) {
	limiter, err := NewSessionLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		c, s := pairOptions(t, Options{}, Options{SessionLimiter: limiter})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		local, err := c.OpenUDP(ctx)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		remote, err := s.Accept(ctx)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		finished := make(chan struct{}, 4)
		for _, closeFn := range []func() error{c.Close, s.Close, local.Close, remote.Close} {
			go func(closeFn func() error) { _ = closeFn(); finished <- struct{}{} }(closeFn)
		}
		for j := 0; j < 4; j++ {
			select {
			case <-finished:
			case <-ctx.Done():
				cancel()
				t.Fatal("concurrent close stuck")
			}
		}
		if got := limiter.active.Load(); got != 0 {
			cancel()
			t.Fatalf("round %d leaked %d slots", i, got)
		}
		cancel()
	}
}

func TestCanceledWriteRestoresFlowCredit(t *testing.T) {
	opts, err := (Options{InitialWindow: 1, MaxDataSize: 1}).defaults()
	if err != nil {
		t.Fatal(err)
	}
	l := &Link{opts: opts, done: make(chan struct{}), send: make(chan []byte, 1), sessions: make(map[uint64]*Session)}
	l.send <- []byte("occupied")
	s := newSession(l, 11, Address{}, false, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	n, err := s.WriteContext(ctx, []byte("a"))
	if n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first write %d: %v", n, err)
	}
	<-l.send
	if n, err = s.WriteContext(context.Background(), []byte("b")); n != 1 || err != nil {
		t.Fatalf("retry %d: %v", n, err)
	}
	frame, err := readFrame(bufio.NewReader(bytes.NewReader(<-l.send)), 1024)
	if err != nil || frame.cmd != "DATA" || string(frame.data) != "b" {
		t.Fatalf("retry frame %+v: %v", frame, err)
	}
}

func TestResourceOptionValidation(t *testing.T) {
	for _, opts := range []Options{{MaxSessions: -1}, {MaxSessions: maxSessionLimit + 1}, {DatagramQueueBytes: -1}, {DatagramQueueBytes: maxDatagramQueueBytes + 1}, {SessionLimiter: &SessionLimiter{}}} {
		if _, err := opts.defaults(); err == nil {
			t.Fatalf("accepted invalid limits: %+v", opts)
		}
	}
	for _, n := range []int{0, -1, maxSessionLimit + 1} {
		if _, err := NewSessionLimiter(n); err == nil {
			t.Fatalf("accepted shared limit %d", n)
		}
	}
}

func TestBufferedTCPDataSurvivesSessionAbort(t *testing.T) {
	opts, err := (Options{InitialWindow: 2}).defaults()
	if err != nil {
		t.Fatal(err)
	}
	l := &Link{opts: opts, done: make(chan struct{}), send: make(chan []byte, 1), sessions: make(map[uint64]*Session)}
	s := newSession(l, 1, Address{}, false, 2)
	if !s.deliver([]byte("a")) {
		t.Fatal("data rejected")
	}
	failure := errors.New("transport failure")
	s.abort(failure)
	buf := make([]byte, 1)
	if n, err := s.ReadContext(context.Background(), buf); n != 1 || string(buf) != "a" || err != nil {
		t.Fatalf("buffered read %d %q: %v", n, buf, err)
	}
	if _, err := s.ReadContext(context.Background(), buf); !errors.Is(err, failure) {
		t.Fatalf("expected original failure: %v", err)
	}
}

func TestHeartbeatTimesOutWhenSendQueueIsFull(t *testing.T) {
	for _, test := range []struct {
		name  string
		close func(*Session) error
	}{{"close", (*Session).Close}, {"close write", (*Session).CloseWrite}} {
		t.Run(test.name, func(t *testing.T) {
			opts, err := (Options{PingInterval: 5 * time.Millisecond, PongTimeout: 30 * time.Millisecond}).defaults()
			if err != nil {
				t.Fatal(err)
			}
			a, b := net.Pipe()
			defer b.Close()
			l := &Link{conn: a, opts: opts, done: make(chan struct{}), send: make(chan []byte, 1), sessions: make(map[uint64]*Session), pong: make(chan uint64, 1)}
			defer l.Close()
			l.send <- []byte("occupied")
			s := newSession(l, 1, Address{}, false, opts.InitialWindow)
			if err := l.add(s.id, s); err != nil {
				t.Fatal(err)
			}
			closed := make(chan error, 1)
			go func() { closed <- test.close(s) }()
			waitGateHeld(t, s.writeGate)
			go l.heartbeat()
			select {
			case <-l.Done():
				if err := l.Err(); err == nil || err.Error() != "shine: heartbeat timeout" {
					t.Fatalf("unexpected link error: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("heartbeat did not time out with a full send queue")
			}
			select {
			case err := <-closed:
				if err == nil || err.Error() != "shine: heartbeat timeout" {
					t.Fatalf("unexpected session close error: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("session close remained blocked")
			}
		})
	}
}

func TestHealthyHeartbeatsKeepLinkOpen(t *testing.T) {
	client, server := pair(t, Options{PingInterval: 10 * time.Millisecond, PongTimeout: 200 * time.Millisecond})
	// Wait through multiple ping intervals and more than one pong timeout.
	timer := time.NewTimer(350 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-client.Done():
		t.Fatalf("client heartbeat failed: %v", client.Err())
	case <-server.Done():
		t.Fatalf("server heartbeat failed: %v", server.Err())
	case <-timer.C:
	}
}
