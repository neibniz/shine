package shine

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hhhflow2020/shine/go/shine/redis"
)

// Set SHINE_CPP_BIN to run these tests against a separately built C++ executable.
func startCPP(t *testing.T, config string, addr string) {
	t.Helper()
	bin := os.Getenv("SHINE_CPP_BIN")
	if bin == "" {
		t.Skip("set SHINE_CPP_BIN to test C++ interoperability")
	}
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-c", p)
	var output strings.Builder
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("C++ server not ready: %s", output.String())
}
func unusedAddr(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	a := l.Addr().String()
	l.Close()
	return a
}
func TestInteropGoClientCPPServer(t *testing.T) {
	for _, mode := range []string{"tcp", "redis", "redis-compressed"} {
		t.Run(mode, func(t *testing.T) { interopGoClientCPPServer(t, mode) })
	}
}
func interopGoClientCPPServer(t *testing.T, mode string) {
	if os.Getenv("SHINE_CPP_BIN") == "" {
		t.Skip("set SHINE_CPP_BIN to test C++ interoperability")
	}
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		for {
			c, e := echo.Accept()
			if e != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	addr := unusedAddr(t)
	cfg := fmt.Sprintf("metrics:\n  listen: %q\ninbounds:\n  - tag: in\n    protocol: shine\n    listen: %q\n    users:\n      - auth: secret\n        email: cpp@example.com\n    transport: %s\n    compression: %t\noutbounds:\n  - tag: direct\n    protocol: direct\nroute:\n  default: direct\n", unusedAddr(t), addr, carrierName(mode), mode == "redis-compressed")
	if mode == "redis" {
		cfg = strings.Replace(cfg, "    transport: redis\n", "", 1)
	}
	startCPP(t, cfg, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var conn io.ReadWriteCloser
	if mode == "tcp" {
		conn, err = net.DialTimeout("tcp", addr, time.Second)
	} else {
		conn, err = redis.Client(ctx, func(ctx context.Context) (io.ReadWriteCloser, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		}, redis.Options{Password: "secret", Compression: mode == "redis-compressed"})
	}
	if err != nil {
		t.Fatal(err)
	}
	link, err := Client(ctx, conn, Options{Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	stopOnTimeout := context.AfterFunc(ctx, func() { _ = link.Close() })
	defer stopOnTimeout()
	target, err := ParseAddress(echo.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	s, err := link.Open(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	want := []byte("go-client to cpp-server")
	if _, err = s.Write(want); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err = io.ReadFull(s, got); err != nil || string(got) != string(want) {
		t.Fatalf("echo %q: %v", got, err)
	}
	large := bytes.Repeat([]byte("0123456789abcdef"), 32768)
	writeDone := make(chan error, 1)
	go func() { _, e := s.Write(large); writeDone <- e }()
	largeReply := make([]byte, len(large))
	if _, err = io.ReadFull(s, largeReply); err != nil || !bytes.Equal(largeReply, large) {
		t.Fatalf("large echo: %v", err)
	}
	if err = <-writeDone; err != nil {
		t.Fatal(err)
	}
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer packet.Close()
	go func() {
		buf := make([]byte, 1024)
		n, peer, e := packet.ReadFrom(buf)
		if e == nil {
			_, _ = packet.WriteTo(buf[:n], peer)
		}
	}()
	udpTarget, err := ParseAddress(packet.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	u, err := link.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if err = u.SendDatagram(ctx, []byte("udp echo"), udpTarget); err != nil {
		t.Fatal(err)
	}
	d, err := u.ReceiveDatagram(ctx)
	if err != nil || string(d.Data) != "udp echo" || d.Target != udpTarget {
		t.Fatalf("UDP echo %+v: %v", d, err)
	}
}
func TestInteropCPPClientGoServer(t *testing.T) {
	for _, mode := range []string{"tcp", "redis", "redis-compressed"} {
		t.Run(mode, func(t *testing.T) { interopCPPClientGoServer(t, mode) })
	}
}
func interopCPPClientGoServer(t *testing.T, mode string) {
	if os.Getenv("SHINE_CPP_BIN") == "" {
		t.Skip("set SHINE_CPP_BIN to test C++ interoperability")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	handled := make(chan error, 1)
	release := make(chan struct{})
	defer close(release)
	go func() {
		conn, e := acceptInterop(ctx, listener, mode)
		if e != nil {
			handled <- e
			return
		}
		l, e := Server(ctx, conn, Options{Users: []User{{Auth: "secret", Email: "cpp@example.com"}}})
		if e != nil {
			handled <- e
			return
		}
		defer l.Close()
		if identity, ok := l.AuthenticatedUser(); !ok || !identity.User || identity.Email != "cpp@example.com" {
			handled <- fmt.Errorf("unexpected identity: %+v", identity)
			return
		}
		s, e := l.Accept(ctx)
		if e != nil {
			handled <- e
			return
		}
		if s.Target() != (Address{"example.com", 443}) {
			handled <- fmt.Errorf("wrong target: %v", s.Target())
			return
		}
		b := make([]byte, len("cpp-client to go-server"))
		if _, e = io.ReadFull(s, b); e == nil {
			_, e = s.Write(b)
		}
		handled <- e
		<-release
	}()
	addr := unusedAddr(t)
	cfg := fmt.Sprintf("metrics:\n  listen: %q\ninbounds:\n  - tag: socks\n    protocol: socks5\n    listen: %q\noutbounds:\n  - tag: direct\n    protocol: direct\n  - tag: proxy\n    protocol: shine-client\n    server: %q\n    users:\n      - auth: secret\n        email: cpp@example.com\n    transport: %s\n    compression: %t\nroute:\n  default: proxy\n", unusedAddr(t), addr, listener.Addr().String(), carrierName(mode), mode == "redis-compressed")
	if mode == "redis" {
		cfg = strings.Replace(cfg, "    transport: redis\n", "", 1)
	}
	startCPP(t, cfg, addr)
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = c.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 2)
	if _, err = io.ReadFull(c, b); err != nil || b[1] != 0 {
		t.Fatalf("SOCKS method %v %v", b, err)
	}
	req := append([]byte{5, 1, 0, 3, 11}, []byte("example.com")...)
	req = append(req, 1, 187)
	if _, err = c.Write(req); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 10)
	if _, err = io.ReadFull(c, reply); err != nil || reply[1] != 0 {
		t.Fatalf("SOCKS reply %v %v", reply, err)
	}
	want := "cpp-client to go-server"
	if _, err = c.Write([]byte(want)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err = io.ReadFull(c, got); err != nil || string(got) != want {
		t.Fatalf("echo %q %v", got, err)
	}
	if err = <-handled; err != nil {
		t.Fatal(err)
	}
}

func TestInteropCPPClientGoServerUDP(t *testing.T) {
	for _, mode := range []string{"tcp", "redis", "redis-compressed"} {
		t.Run(mode, func(t *testing.T) { interopCPPClientGoServerUDP(t, mode) })
	}
}
func interopCPPClientGoServerUDP(t *testing.T, mode string) {
	if os.Getenv("SHINE_CPP_BIN") == "" {
		t.Skip("set SHINE_CPP_BIN to test C++ interoperability")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	handled := make(chan error, 1)
	release := make(chan struct{})
	defer close(release)
	go func() {
		conn, e := acceptInterop(ctx, listener, mode)
		if e != nil {
			handled <- e
			return
		}
		link, e := Server(ctx, conn, Options{Users: []User{{Auth: "secret", Email: "cpp@example.com"}}})
		if e != nil {
			handled <- e
			return
		}
		defer link.Close()
		if identity, ok := link.AuthenticatedUser(); !ok || !identity.User || identity.Email != "cpp@example.com" {
			handled <- fmt.Errorf("unexpected identity: %+v", identity)
			return
		}
		s, e := link.Accept(ctx)
		if e != nil {
			handled <- e
			return
		}
		if !s.IsUDP() {
			handled <- fmt.Errorf("expected UDP session")
			return
		}
		d, e := s.ReceiveDatagram(ctx)
		if e == nil && (d.Target != (Address{"8.8.8.8", 53}) || string(d.Data) != "udp payload") {
			e = fmt.Errorf("unexpected datagram: %+v", d)
		}
		if e == nil {
			e = s.SendDatagram(ctx, d.Data, d.Target)
		}
		handled <- e
		<-release
	}()
	addr := unusedAddr(t)
	cfg := fmt.Sprintf("metrics:\n  listen: %q\ninbounds:\n  - tag: socks\n    protocol: socks5\n    listen: %q\noutbounds:\n  - tag: direct\n    protocol: direct\n  - tag: proxy\n    protocol: shine-client\n    server: %q\n    users:\n      - auth: secret\n        email: cpp@example.com\n    transport: %s\n    compression: %t\nroute:\n  default: proxy\n", unusedAddr(t), addr, listener.Addr().String(), carrierName(mode), mode == "redis-compressed")
	if mode == "redis" {
		cfg = strings.Replace(cfg, "    transport: redis\n", "", 1)
	}
	startCPP(t, cfg, addr)
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = c.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 2)
	if _, err = io.ReadFull(c, b); err != nil || b[1] != 0 {
		t.Fatalf("SOCKS method %v %v", b, err)
	}
	if _, err = c.Write([]byte{5, 3, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 10)
	if _, err = io.ReadFull(c, reply); err != nil || reply[1] != 0 {
		t.Fatalf("SOCKS UDP reply %v %v", reply, err)
	}
	port := int(reply[8])<<8 | int(reply[9])
	u, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	_ = u.SetDeadline(time.Now().Add(5 * time.Second))
	packet := append([]byte{0, 0, 0, 1, 8, 8, 8, 8, 0, 53}, []byte("udp payload")...)
	if _, err = u.Write(packet); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 128)
	n, err := u.Read(buf)
	expected := append([]byte{0, 0, 0, 1, 8, 8, 8, 8, 0, 53}, []byte("udp payload")...)
	if err != nil || string(buf[:n]) != string(expected) {
		t.Fatalf("SOCKS UDP response %x: %v", buf[:n], err)
	}
	if err = <-handled; err != nil {
		t.Fatal(err)
	}
}

func carrierName(mode string) string {
	if mode == "tcp" {
		return "tcp"
	}
	return "redis"
}
func acceptInterop(ctx context.Context, listener net.Listener, mode string) (io.ReadWriteCloser, error) {
	if mode == "tcp" {
		return listener.Accept()
	}
	acceptor, err := redis.NewAcceptor(redis.Options{Compression: mode == "redis-compressed"}, true, func(p string) bool { return p == "secret" })
	if err != nil {
		return nil, err
	}
	type result struct {
		conn io.ReadWriteCloser
		err  error
	}
	results := make(chan result, 1)
	go func() {
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			go func() {
				stream, e := acceptor.AcceptConn(ctx, c)
				if stream != nil || e != nil {
					select {
					case results <- result{stream, e}:
					case <-ctx.Done():
					}
				}
			}()
		}
	}()
	select {
	case r := <-results:
		return r.conn, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestInteropCPPRedisCommandsAndAuthentication(t *testing.T) {
	addr := unusedAddr(t)
	cfg := fmt.Sprintf("metrics:\n  listen: %q\ninbounds:\n  - tag: in\n    protocol: shine\n    listen: %q\n    users:\n      - auth: first-secret\n        email: first@example.com\n      - auth: second-secret\n        email: second@example.com\noutbounds:\n  - tag: direct\n    protocol: direct\nroute:\n  default: direct\n", unusedAddr(t), addr)
	startCPP(t, cfg, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dial := func(ctx context.Context) (io.ReadWriteCloser, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}
	for _, secret := range []string{"first-secret", "second-secret"} {
		c, err := redis.Client(ctx, dial, redis.Options{Password: secret})
		if err != nil {
			t.Fatalf("valid credential %q: %v", secret, err)
		}
		c.Close()
	}
	if c, err := redis.Client(ctx, dial, redis.Options{Password: "wrong"}); err == nil {
		c.Close()
		t.Fatal("incorrect credential accepted")
	}
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	r := bufio.NewReader(c)
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"PING"}, "-NOAUTH Authentication required.\r\n"},
		{[]string{"AUTH", "first-secret"}, "+OK\r\n"},
		{[]string{"PING"}, "+PONG\r\n"},
		{[]string{"SET", "colour", "blue"}, "+OK\r\n"},
		{[]string{"GET", "colour"}, "$4\r\nblue\r\n"},
		{[]string{"SET", "colour", "red", "NX"}, "$-1\r\n"},
		{[]string{"SELECT", "1"}, "+OK\r\n"},
		{[]string{"GET", "colour"}, "$-1\r\n"},
		{[]string{"PUBLISH", "ordinary", "data"}, ":0\r\n"},
		{[]string{"CLIENT", "SETNAME", "demo"}, "+OK\r\n"},
		{[]string{"CLIENT", "GETNAME"}, "$4\r\ndemo\r\n"},
		{[]string{"QUIT"}, "+OK\r\n"},
	}
	for _, tc := range cases {
		var request strings.Builder
		fmt.Fprintf(&request, "*%d\r\n", len(tc.args))
		for _, arg := range tc.args {
			fmt.Fprintf(&request, "$%d\r\n%s\r\n", len(arg), arg)
		}
		if _, err := io.WriteString(c, request.String()); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(tc.want))
		if _, err := io.ReadFull(r, got); err != nil || string(got) != tc.want {
			t.Fatalf("%v: got %q, want %q: %v", tc.args, got, tc.want, err)
		}
	}
}

func TestInteropCPPRedisPairCredentialsAndMalformedPayload(t *testing.T) {
	addr := unusedAddr(t)
	cfg := fmt.Sprintf("metrics:\n  listen: %q\ninbounds:\n  - tag: in\n    protocol: shine\n    listen: %q\n    users:\n      - auth: first\n      - auth: second\noutbounds:\n  - tag: direct\n    protocol: direct\nroute:\n  default: direct\n", unusedAddr(t), addr)
	startCPP(t, cfg, addr)
	down, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer down.Close()
	_ = down.SetDeadline(time.Now().Add(4 * time.Second))
	up, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	_ = up.SetDeadline(time.Now().Add(4 * time.Second))
	command := func(c net.Conn, expected string, parts ...string) {
		t.Helper()
		var wire strings.Builder
		fmt.Fprintf(&wire, "*%d\r\n", len(parts))
		for _, p := range parts {
			fmt.Fprintf(&wire, "$%d\r\n%s\r\n", len(p), p)
		}
		if _, err := io.WriteString(c, wire.String()); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(expected))
		if _, err := io.ReadFull(c, got); err != nil || string(got) != expected {
			t.Fatalf("%q: got %q want %q: %v", parts, got, expected, err)
		}
	}
	command(down, "+OK\r\n", "AUTH", "first")
	command(down, "*3\r\n$9\r\nsubscribe\r\n$15\r\nevents:test:out\r\n:1\r\n", "SUBSCRIBE", "events:test:out")
	command(up, "+OK\r\n", "AUTH", "second")
	command(up, ":0\r\n", "PUBLISH", "events:test:in", "")
	command(up, "+OK\r\n", "AUTH", "first")
	command(up, ":1\r\n", "PUBLISH", "events:test:in", "")
	command(up, ":1\r\n", "PUBLISH", "events:test:in", "\x02invalid-envelope")
	b := make([]byte, 1)
	if _, err := up.Read(b); err == nil {
		t.Fatal("invalid payload did not close the connection")
	}
}

func TestInteropCPPClientCPPServer(t *testing.T) {
	if os.Getenv("SHINE_CPP_BIN") == "" {
		t.Skip("set SHINE_CPP_BIN to test C++ interoperability")
	}
	echo, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		for {
			c, e := echo.Accept()
			if e != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	serverAddr, clientAddr := unusedAddr(t), unusedAddr(t)
	server := fmt.Sprintf("metrics:\n  listen: %q\ninbounds:\n  - tag: in\n    protocol: shine\n    listen: %q\n    users:\n      - auth: secret\n        email: cpp@example.com\n    compression: true\noutbounds:\n  - tag: direct\n    protocol: direct\nroute:\n  default: direct\n", unusedAddr(t), serverAddr)
	client := fmt.Sprintf("metrics:\n  listen: %q\ninbounds:\n  - tag: in\n    protocol: socks5\n    listen: %q\noutbounds:\n  - tag: direct\n    protocol: direct\n  - tag: proxy\n    protocol: shine-client\n    server: %q\n    users:\n      - auth: secret\n    compression: true\nroute:\n  default: proxy\n", unusedAddr(t), clientAddr, serverAddr)
	startCPP(t, server, serverAddr)
	startCPP(t, client, clientAddr)
	c, err := net.DialTimeout("tcp", clientAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = c.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 2)
	if _, err = io.ReadFull(c, b); err != nil || b[1] != 0 {
		t.Fatalf("SOCKS greeting %x: %v", b, err)
	}
	port := echo.Addr().(*net.TCPAddr).Port
	if _, err = c.Write([]byte{5, 1, 0, 1, 127, 0, 0, 1, byte(port >> 8), byte(port)}); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 10)
	if _, err = io.ReadFull(c, reply); err != nil || reply[1] != 0 {
		t.Fatalf("SOCKS connect %x: %v", reply, err)
	}
	want := bytes.Repeat([]byte("cpp-transport"), 4096)
	done := make(chan error, 1)
	go func() { _, e := c.Write(want); done <- e }()
	got := make([]byte, len(want))
	if _, err = io.ReadFull(c, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("echo: %v", err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}
