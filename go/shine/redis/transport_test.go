package redis

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	shine "github.com/hhhflow2020/shine/go/shine"
)

type recorder struct {
	io.ReadWriteCloser
	mu   *sync.Mutex
	wire *bytes.Buffer
}

func (r *recorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	r.wire.Write(p)
	r.mu.Unlock()
	return r.ReadWriteCloser.Write(p)
}
func paired(t *testing.T, clientOpts Options, serverOpts Options, required bool, auth func(string) bool) (io.ReadWriteCloser, io.ReadWriteCloser, *bytes.Buffer, *sync.Mutex) {
	t.Helper()
	a, e := NewAcceptor(serverOpts, required, auth)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	t.Cleanup(func() { a.Close() })
	accepted := make(chan io.ReadWriteCloser, 2)
	errs := make(chan error, 2)
	wire := new(bytes.Buffer)
	mu := new(sync.Mutex)
	dial := func(context.Context) (io.ReadWriteCloser, error) {
		c, s := net.Pipe()
		go func() {
			stream, e := a.AcceptConn(ctx, s)
			if e != nil {
				select {
				case errs <- e:
				default:
				}
				return
			}
			if stream != nil {
				accepted <- stream
			}
		}()
		return &recorder{ReadWriteCloser: c, wire: wire, mu: mu}, nil
	}
	c, e := Client(ctx, dial, clientOpts)
	if e != nil {
		t.Fatal(e)
	}
	var s io.ReadWriteCloser
	select {
	case s = <-accepted:
	case e = <-errs:
		t.Fatal(e)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	t.Cleanup(func() { c.Close(); s.Close() })
	return c, s, wire, mu
}
func TestCarrierDuplexAndWire(t *testing.T) {
	c, s, wire, mu := paired(t, Options{Password: "key", Compression: true}, Options{Compression: true}, true, func(p string) bool { return p == "key" })
	message := bytes.Repeat([]byte("repetitive application payload"), 10000)
	done := make(chan error, 1)
	go func() { _, e := c.Write(message); done <- e }()
	got := make([]byte, len(message))
	if _, e := io.ReadFull(s, got); e != nil || !bytes.Equal(got, message) {
		t.Fatalf("upload: %v", e)
	}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	reply := make([]byte, 131072)
	if _, e := rand.Read(reply); e != nil {
		t.Fatal(e)
	}
	go func() { _, e := s.Write(reply); done <- e }()
	got = make([]byte, len(reply))
	if _, e := io.ReadFull(c, got); e != nil || !bytes.Equal(got, reply) {
		t.Fatalf("download: %v", e)
	}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	captured := append([]byte(nil), wire.Bytes()...)
	mu.Unlock()
	if !bytes.Contains(captured, []byte("AUTH")) || !bytes.Contains(captured, []byte("SUBSCRIBE")) || !bytes.Contains(captured, []byte("PUBLISH")) {
		t.Fatal("missing Redis command")
	}
	if bytes.Contains(captured, message[:200]) {
		t.Fatal("compressible payload was not compressed")
	}
	if len(captured) >= len(message)/10 {
		t.Fatalf("compression ineffective: %d/%d", len(captured), len(message))
	}
}
func TestCodec(t *testing.T) {
	source := bytes.Repeat([]byte("abcd"), 1000)
	p, e := encodePayload(source, true, 8192)
	if e != nil || p[0] != 1 || len(p) >= len(source) {
		t.Fatalf("compression: %d %v", len(p), e)
	}
	out, e := decodePayload(p, 8192)
	if e != nil || !bytes.Equal(out, source) {
		t.Fatal(e)
	}
	if _, e = decodePayload(p, 100); e == nil {
		t.Fatal("accepted decompression overflow")
	}
	if _, e = decodePayload([]byte{1, 1, 2, 3}, 8192); e == nil {
		t.Fatal("accepted corrupt compressed data")
	}
	random := make([]byte, 1024)
	rand.Read(random)
	p, e = encodePayload(random, true, 8192)
	if e != nil || p[0] != 0 {
		t.Fatalf("random payload was compressed: %v", e)
	}
}
func TestWrongPassword(t *testing.T) {
	a, e := NewAcceptor(Options{}, true, func(p string) bool { return p == "good" })
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dial := func(context.Context) (io.ReadWriteCloser, error) {
		c, s := net.Pipe()
		go a.AcceptConn(ctx, s)
		return c, nil
	}
	if _, e = Client(ctx, dial, Options{Password: "bad"}); e == nil {
		t.Fatal("wrong password accepted")
	}
}
func TestUnpairedSubscriptionExpires(t *testing.T) {
	a, e := NewAcceptor(Options{PairTimeout: 40 * time.Millisecond}, false, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	c, s := net.Pipe()
	defer c.Close()
	done := make(chan error, 1)
	go func() { _, e := a.AcceptConn(context.Background(), s); done <- e }()
	if e = writeAll(c, command([]byte("SUBSCRIBE"), []byte("events:test:out"))); e != nil {
		t.Fatal(e)
	}
	ack, e := readValue(bufio.NewReader(c), 1024, 0)
	if e != nil || ack.kind != '*' {
		t.Fatalf("subscription %v", e)
	}
	select {
	case e = <-done:
		if e == nil {
			t.Fatal("missing timeout")
		}
	case <-time.After(time.Second):
		t.Fatal("pairing did not expire")
	}
}
func TestShineOverCarrier(t *testing.T) {
	carrier, serverCarrier, _, _ := paired(t, Options{Password: "key", Compression: true}, Options{Compression: true}, true, func(p string) bool { return p == "key" })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	servers := make(chan *shine.Link, 1)
	errs := make(chan error, 1)
	go func() {
		l, e := shine.Server(ctx, serverCarrier, shine.Options{Users: []shine.User{{Auth: "key", Email: "a@example.com"}}})
		if e != nil {
			errs <- e
		} else {
			servers <- l
		}
	}()
	client, e := shine.Client(ctx, carrier, shine.Options{Password: "key"})
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	var server *shine.Link
	select {
	case server = <-servers:
	case e = <-errs:
		t.Fatal(e)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer server.Close()
	if id, ok := server.AuthenticatedUser(); !ok || id.Email != "a@example.com" {
		t.Fatalf("identity %+v %v", id, ok)
	}
	x, e := client.Open(ctx, shine.Address{Host: "example.com", Port: 443})
	if e != nil {
		t.Fatal(e)
	}
	y, e := server.Accept(ctx)
	if e != nil {
		t.Fatal(e)
	}
	payload := bytes.Repeat([]byte("x"), 300000)
	written := make(chan error, 1)
	go func() { _, e := x.Write(payload); written <- e }()
	got := make([]byte, len(payload))
	if _, e = io.ReadFull(y, got); e != nil || !bytes.Equal(got, payload) {
		t.Fatalf("data %v", e)
	}
	if e = <-written; e != nil {
		t.Fatal(e)
	}
	u, e := client.OpenUDP(ctx)
	if e != nil {
		t.Fatal(e)
	}
	v, e := server.Accept(ctx)
	if e != nil {
		t.Fatal(e)
	}
	target := shine.Address{Host: "8.8.8.8", Port: 53}
	if e = u.SendDatagram(ctx, []byte("dns"), target); e != nil {
		t.Fatal(e)
	}
	d, e := v.ReceiveDatagram(ctx)
	if e != nil || d.Target != target || string(d.Data) != "dns" {
		t.Fatalf("UDP %+v %v", d, e)
	}
	client.Close()
	_, e = y.ReadContext(ctx, make([]byte, 1))
	if e == nil || errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("close did not unblock session: %v", e)
	}
}

func TestPairRequiresSameAuthenticatedUser(t *testing.T) {
	a, e := NewAcceptor(Options{PairTimeout: time.Second}, true, func(p string) bool { return p == "alice" || p == "bob" })
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	open := func() (net.Conn, *bufio.Reader) {
		c, s := net.Pipe()
		go a.AcceptConn(ctx, s)
		return c, bufio.NewReader(c)
	}
	down, dr := open()
	defer down.Close()
	if e = clientAuth(down, dr, "alice", 1024); e != nil {
		t.Fatal(e)
	}
	if e = writeAll(down, command([]byte("SUBSCRIBE"), []byte("events:test:out"))); e != nil {
		t.Fatal(e)
	}
	if _, e = readValue(dr, 1024, 0); e != nil {
		t.Fatal(e)
	}
	up, ur := open()
	defer up.Close()
	if e = clientAuth(up, ur, "bob", 1024); e != nil {
		t.Fatal(e)
	}
	if e = writeAll(up, command([]byte("PUBLISH"), []byte("events:test:in"), nil)); e != nil {
		t.Fatal(e)
	}
	v, e := readValue(ur, 1024, 0)
	if e != nil || v.kind != ':' || v.number != 0 {
		t.Fatalf("unexpected pairing response %+v: %v", v, e)
	}
	// A failed attempt by another user must leave the subscriber available.
	legitimate, lr := open()
	defer legitimate.Close()
	if e = clientAuth(legitimate, lr, "alice", 1024); e != nil {
		t.Fatal(e)
	}
	if e = writeAll(legitimate, command([]byte("PUBLISH"), []byte("events:test:in"), nil)); e != nil {
		t.Fatal(e)
	}
	v, e = readValue(lr, 1024, 0)
	if e != nil || v.kind != ':' || v.number != 1 {
		t.Fatalf("legitimate pairing response %+v: %v", v, e)
	}
}
func TestHandshakeTimeoutWhileClientDoesNotRead(t *testing.T) {
	a, e := NewAcceptor(Options{HandshakeTimeout: 40 * time.Millisecond}, false, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	client, server := net.Pipe()
	defer client.Close()
	done := make(chan error, 1)
	go func() { _, e := a.AcceptConn(context.Background(), server); done <- e }()
	written := make(chan error, 1)
	go func() { written <- writeAll(client, command([]byte("SUBSCRIBE"), []byte("events:test:out"))) }()
	if e = <-written; e != nil {
		t.Fatal(e)
	}
	select {
	case e = <-done:
		if !errors.Is(e, context.DeadlineExceeded) {
			t.Fatalf("unexpected handshake result %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked subscription did not time out")
	}
}
func TestAcceptorCloseTerminatesActiveStream(t *testing.T) {
	a, e := NewAcceptor(Options{}, false, nil)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accepted := make(chan io.ReadWriteCloser, 1)
	dial := func(context.Context) (io.ReadWriteCloser, error) {
		c, s := net.Pipe()
		go func() {
			v, _ := a.AcceptConn(ctx, s)
			if v != nil {
				accepted <- v
			}
		}()
		return c, nil
	}
	c, e := Client(ctx, dial, Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	s := <-accepted
	defer s.Close()
	if e = a.Close(); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, e := c.Read(make([]byte, 1)); done <- e }()
	select {
	case e = <-done:
		if e == nil {
			t.Fatal("read succeeded after close")
		}
	case <-ctx.Done():
		t.Fatal("active stream did not close")
	}
}
func TestRedisPingResponses(t *testing.T) {
	a, e := NewAcceptor(Options{}, false, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	c, s := net.Pipe()
	defer c.Close()
	go a.AcceptConn(context.Background(), s)
	r := bufio.NewReader(c)
	if e = writeAll(c, command([]byte("PING"))); e != nil {
		t.Fatal(e)
	}
	v, e := readValue(r, 1024, 0)
	if e != nil || v.kind != '+' || string(v.text) != "PONG" {
		t.Fatalf("PING %+v %v", v, e)
	}
	if e = writeAll(c, command([]byte("PING"), []byte("hello"))); e != nil {
		t.Fatal(e)
	}
	v, e = readValue(r, 1024, 0)
	if e != nil || v.kind != '$' || string(v.text) != "hello" {
		t.Fatalf("PING payload %+v %v", v, e)
	}
}

func TestAsymmetricCompression(t *testing.T) {
	for _, clientCompress := range []bool{true, false} {
		t.Run(fmt.Sprintf("client_compress_%v", clientCompress), func(t *testing.T) {
			c, s, _, _ := paired(t, Options{Compression: clientCompress}, Options{Compression: !clientCompress}, false, nil)
			message := bytes.Repeat([]byte("payload"), 2000)
			done := make(chan error, 1)
			go func() { _, e := c.Write(message); done <- e }()
			got := make([]byte, len(message))
			if _, e := io.ReadFull(s, got); e != nil || !bytes.Equal(message, got) {
				t.Fatalf("upload: %v", e)
			}
			if e := <-done; e != nil {
				t.Fatal(e)
			}
			go func() { _, e := s.Write(message); done <- e }()
			if _, e := io.ReadFull(c, got); e != nil || !bytes.Equal(message, got) {
				t.Fatalf("download: %v", e)
			}
			if e := <-done; e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestRESP2WireFixtures(t *testing.T) {
	cases := []struct {
		name string
		got  []byte
		want string
	}{
		{"auth", command([]byte("AUTH"), []byte("key")), "*2\r\n$4\r\nAUTH\r\n$3\r\nkey\r\n"},
		{"subscribe", command([]byte("SUBSCRIBE"), []byte("events:1:out")), "*2\r\n$9\r\nSUBSCRIBE\r\n$12\r\nevents:1:out\r\n"},
		{"subscribe ack", subAck("events:1:out"), "*3\r\n$9\r\nsubscribe\r\n$12\r\nevents:1:out\r\n:1\r\n"},
		{"publish", command([]byte("PUBLISH"), []byte("events:1:in"), []byte("xyz")), "*3\r\n$7\r\nPUBLISH\r\n$11\r\nevents:1:in\r\n$3\r\nxyz\r\n"},
		{"message", push("message", "events:1:out", []byte("xyz")), "*3\r\n$7\r\nmessage\r\n$12\r\nevents:1:out\r\n$3\r\nxyz\r\n"},
	}
	for _, tc := range cases {
		if string(tc.got) != tc.want {
			t.Errorf("%s got %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}
