package shine

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func pair(t *testing.T, opts Options) (*Link, *Link) {
	t.Helper()
	a, b := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := make(chan *Link, 1)
	errs := make(chan error, 1)
	go func() {
		s, e := Server(ctx, b, opts)
		if e != nil {
			errs <- e
		} else {
			ch <- s
		}
	}()
	c, e := Client(ctx, a, opts)
	if e != nil {
		t.Fatal(e)
	}
	var s *Link
	select {
	case s = <-ch:
	case e = <-errs:
		t.Fatal(e)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	t.Cleanup(func() { c.Close(); s.Close() })
	return c, s
}
func TestFrameWire(t *testing.T) {
	a := Address{"example.com", 443}
	ab, e := a.marshal()
	if e != nil {
		t.Fatal(e)
	}
	wantAddr := append([]byte{3, 11}, []byte("example.com")...)
	wantAddr = append(wantAddr, 1, 187)
	if !bytes.Equal(ab, wantAddr) {
		t.Fatalf("address %x", ab)
	}
	id := uint64(0x0102030405060708)
	wire := control("NEW", u64(id), ab, u32(262144))
	want := append([]byte("*4\r\n$3\r\nNEW\r\n$8\r\n"), u64(id)...)
	want = append(want, []byte("\r\n$15\r\n")...)
	want = append(want, ab...)
	want = append(want, []byte("\r\n$4\r\n\x00\x04\x00\x00\r\n")...)
	if !bytes.Equal(wire, want) {
		t.Fatalf("wire %x want %x", wire, want)
	}
	f, e := readFrame(bufio.NewReader(bytes.NewReader(wire)), 1024)
	if e != nil || f.cmd != "NEW" {
		t.Fatalf("frame %v %v", f, e)
	}
	got, e := parseAddress(f.args[1])
	if e != nil || got != a {
		t.Fatalf("address %v %v", got, e)
	}
	for _, bad := range []string{"$-1\r\n", "*1025\r\n", "$99999999\r\n", "*1\r\n$1\r\na!!", "+OK\r\n"} {
		if _, e = readFrame(bufio.NewReader(bytes.NewBufferString(bad)), 1024); e == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}
func TestTCPMultiplexFlowAndHalfClose(t *testing.T) {
	c, s := pair(t, Options{Password: "pw", InitialWindow: 1024, MaxDataSize: 300})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	target := Address{"example.com", 443}
	client, err := c.Open(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	server, err := s.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if server.Target() != target {
		t.Fatal(server.Target())
	}
	second, err := c.Open(ctx, Address{"127.0.0.1", 80})
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	message := bytes.Repeat([]byte("abc123"), 3000)
	writer := make(chan error, 1)
	go func() {
		_, e := client.Write(message)
		if e == nil {
			e = client.CloseWrite()
		}
		writer <- e
	}()
	if _, err = second.Write([]byte("independent")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 11)
	if _, err = io.ReadFull(other, buf); err != nil || string(buf) != "independent" {
		t.Fatalf("other session: %q %v", buf, err)
	}
	result, err := io.ReadAll(server)
	if err != nil || !bytes.Equal(result, message) {
		t.Fatalf("read %d: %v", len(result), err)
	}
	if err = <-writer; err != nil {
		t.Fatal(err)
	}
	if _, err = server.Write([]byte("reply")); err != nil {
		t.Fatal(err)
	}
	if err = server.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	reply, err := io.ReadAll(client)
	if err != nil || string(reply) != "reply" {
		t.Fatalf("reply %q: %v", reply, err)
	}
	second.Close()
	other.Close()
}
func TestUDPAndCancellation(t *testing.T) {
	c, s := pair(t, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a, e := c.OpenUDP(ctx)
	if e != nil {
		t.Fatal(e)
	}
	b, e := s.Accept(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if !b.IsUDP() {
		t.Fatal("not UDP")
	}
	target := Address{"8.8.8.8", 53}
	if e = a.SendDatagram(ctx, []byte{0, 1, 2}, target); e != nil {
		t.Fatal(e)
	}
	d, e := b.ReceiveDatagram(ctx)
	if e != nil || d.Target != target || !bytes.Equal(d.Data, []byte{0, 1, 2}) {
		t.Fatalf("datagram: %+v %v", d, e)
	}
	if e = b.SendDatagram(ctx, []byte("response"), target); e != nil {
		t.Fatal(e)
	}
	d, e = a.ReceiveDatagram(ctx)
	if e != nil || string(d.Data) != "response" {
		t.Fatalf("response: %+v %v", d, e)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, e = a.ReceiveDatagram(canceled); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	a.Close()
	b.Close()
}
func TestHandshakeAuthAndCancellation(t *testing.T) {
	a, b := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _, _ = Server(ctx, b, Options{Password: "right"}) }()
	_, err := Client(ctx, a, Options{Password: "wrong"})
	if err == nil {
		t.Fatal("incorrect password accepted")
	}
	wg.Wait()
	a, b = net.Pipe()
	defer b.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	_, err = Client(ctx2, a, Options{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handshake error: %v", err)
	}
}
func TestCloseUnblocksWriter(t *testing.T) {
	c, s := pair(t, Options{InitialWindow: 16, MaxDataSize: 16})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a, e := c.Open(ctx, Address{"a.test", 80})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Accept(ctx)
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, e := a.Write(make([]byte, 32)); done <- e }()
	time.Sleep(20 * time.Millisecond)
	a.Close()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("writer succeeded")
		}
	case <-ctx.Done():
		t.Fatal("writer stuck")
	}
}

func TestHeartbeatTimeout(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		r := bufio.NewReader(b)
		if _, e := readFrame(r, 1024); e != nil {
			return
		}
		if e := writeAll(b, control("HELLO", []byte("1"), []byte("mux,flow"))); e != nil {
			return
		}
		_, _ = readFrame(r, 1024)
	}()
	l, e := Client(ctx, a, Options{PingInterval: 10 * time.Millisecond, PongTimeout: 20 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	select {
	case <-l.Done():
		if l.Err() == nil {
			t.Fatal("missing heartbeat error")
		}
	case <-ctx.Done():
		t.Fatal("heartbeat did not close link")
	}
}
func TestCloseWriteUnblocksFlowControl(t *testing.T) {
	c, s := pair(t, Options{InitialWindow: 16, MaxDataSize: 16})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a, e := c.Open(ctx, Address{"a.test", 80})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Accept(ctx)
	if e != nil {
		t.Fatal(e)
	}
	writer := make(chan error, 1)
	go func() { _, e := a.Write(make([]byte, 32)); writer <- e }()
	time.Sleep(20 * time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- a.CloseWrite() }()
	select {
	case e := <-writer:
		if e == nil {
			t.Fatal("blocked writer succeeded")
		}
	case <-ctx.Done():
		t.Fatal("writer stuck")
	}
	select {
	case e := <-closed:
		if e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal("CloseWrite stuck")
	}
}

func TestServerCredentials(t *testing.T) {
	cases := []struct {
		name, auth string
		want       Identity
		ok         bool
	}{
		{"alice", "alice-secret", Identity{Email: "alice@example.com", Level: 2, User: true}, true},
		{"bob", "bob-secret", Identity{Email: "bob@example.com", User: true}, true},
		{"no email", "no-email-secret", Identity{User: true}, true},
		{"unknown", "wrong", Identity{}, false},
	}
	opts := Options{Users: []User{{Auth: "alice-secret", Email: "alice@example.com", Level: 2}, {Auth: "bob-secret", Email: "bob@example.com"}, {Auth: "no-email-secret"}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := net.Pipe()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			type result struct {
				link *Link
				err  error
			}
			server := make(chan result, 1)
			go func() { l, e := Server(ctx, b, opts); server <- result{l, e} }()
			client, e := Client(ctx, a, Options{Password: tc.auth})
			sr := <-server
			if !tc.ok {
				if e == nil || sr.err == nil {
					t.Fatalf("invalid credential accepted: client=%v server=%v", e, sr.err)
				}
				return
			}
			if e != nil || sr.err != nil {
				t.Fatalf("handshake: client=%v server=%v", e, sr.err)
			}
			defer client.Close()
			defer sr.link.Close()
			got, ok := sr.link.AuthenticatedUser()
			if !ok || got != tc.want {
				t.Fatalf("identity %+v, authenticated=%v", got, ok)
			}
			if _, ok = client.AuthenticatedUser(); ok {
				t.Fatal("client unexpectedly has server identity")
			}
		})
	}
}
func TestCredentialValidation(t *testing.T) {
	bad := []Options{
		{Password: "legacy", Users: []User{{Auth: "one"}}},
		{Users: []User{{Auth: "one", Email: "x"}, {Auth: "two", Email: "x"}}},
		{Users: []User{{Auth: "same", Email: "a"}, {Auth: "same", Email: "b"}}},
		{Users: []User{{Email: "a"}}},
	}
	for _, opts := range bad {
		if err := ValidateServerCredentials(opts); err == nil {
			t.Fatalf("accepted invalid credentials: %+v", opts)
		}
	}
}
func TestUnauthenticatedIdentity(t *testing.T) {
	client, server := pair(t, Options{})
	if identity, ok := server.AuthenticatedUser(); ok || identity != (Identity{}) {
		t.Fatalf("unexpected identity: %+v, %v", identity, ok)
	}
	if _, ok := client.AuthenticatedUser(); ok {
		t.Fatal("client unexpectedly authenticated")
	}
}
