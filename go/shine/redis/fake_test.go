package redis

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"
	"time"
)

type redisTestClient struct {
	conn net.Conn
	r    *bufio.Reader
}

func (c *redisTestClient) call(t *testing.T, parts ...string) value {
	t.Helper()
	args := make([][]byte, len(parts))
	for i := range parts {
		args[i] = []byte(parts[i])
	}
	if err := writeAll(c.conn, command(args...)); err != nil {
		t.Fatalf("%v write: %v", parts, err)
	}
	v, err := readValue(c.r, DefaultMaxPayload+1024, 0)
	if err != nil {
		t.Fatalf("%v read: %v", parts, err)
	}
	return v
}
func fakeClientForTest(t *testing.T, a *Acceptor) (*redisTestClient, <-chan error) {
	t.Helper()
	c, s := net.Pipe()
	done := make(chan error, 1)
	go func() {
		v, e := a.AcceptConn(context.Background(), s)
		if v != nil {
			v.Close()
		}
		done <- e
	}()
	t.Cleanup(func() { c.Close() })
	return &redisTestClient{c, bufio.NewReader(c)}, done
}
func wantReply(t *testing.T, v value, kind byte, text string) {
	t.Helper()
	if v.kind != kind || string(v.text) != text {
		t.Fatalf("got %+v want %c %q", v, kind, text)
	}
}
func wantInt(t *testing.T, v value, n int64) {
	t.Helper()
	if v.kind != ':' || v.number != n {
		t.Fatalf("got %+v want :%d", v, n)
	}
}
func TestFakeCommandsAndAuthentication(t *testing.T) {
	a, e := NewAcceptor(Options{CommandIdleTimeout: time.Second}, true, func(s string) bool { return s == "secret" })
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	c, done := fakeClientForTest(t, a)
	wantReply(t, c.call(t, "PING"), '-', "NOAUTH Authentication required.")
	wantReply(t, c.call(t, "AUTH", "wrong"), '-', "WRONGPASS invalid username-password pair or user is disabled.")
	wantReply(t, c.call(t, "AUTH", "default", "secret"), '+', "OK")
	wantReply(t, c.call(t, "AUTH", "wrong"), '-', "WRONGPASS invalid username-password pair or user is disabled.")
	wantReply(t, c.call(t, "PING"), '+', "PONG")
	wantReply(t, c.call(t, "PING", "abc"), '$', "abc")
	wantReply(t, c.call(t, "ECHO", "hello"), '$', "hello")
	hello := c.call(t, "HELLO", "2")
	if hello.kind != '*' || len(hello.array) != 14 || string(hello.array[1].text) != "redis" || hello.array[5].number != 2 {
		t.Fatalf("HELLO %+v", hello)
	}
	id := c.call(t, "CLIENT", "ID")
	if id.kind != ':' || id.number != hello.array[7].number {
		t.Fatal("inconsistent client id")
	}
	wantReply(t, c.call(t, "CLIENT", "SETNAME", "app"), '+', "OK")
	wantReply(t, c.call(t, "CLIENT", "GETNAME"), '$', "app")
	wantReply(t, c.call(t, "CLIENT", "SETINFO", "LIB-NAME", "example"), '+', "OK")
	missing := c.call(t, "GET", "missing")
	if missing.kind != '$' || !missing.null {
		t.Fatalf("missing %+v", missing)
	}
	wantReply(t, c.call(t, "SET", "key", "value", "NX"), '+', "OK")
	if v := c.call(t, "SET", "key", "other", "NX"); !v.null {
		t.Fatalf("NX %+v", v)
	}
	wantReply(t, c.call(t, "GET", "key"), '$', "value")
	wantInt(t, c.call(t, "EXISTS", "key", "key"), 2)
	wantReply(t, c.call(t, "TYPE", "key"), '+', "string")
	wantInt(t, c.call(t, "DBSIZE"), 1)
	m := c.call(t, "MGET", "key", "missing")
	if m.kind != '*' || len(m.array) != 2 || string(m.array[0].text) != "value" || !m.array[1].null {
		t.Fatalf("MGET %+v", m)
	}
	wantReply(t, c.call(t, "SELECT", "2"), '+', "OK")
	if v := c.call(t, "GET", "key"); !v.null {
		t.Fatal("DB not isolated")
	}
	wantReply(t, c.call(t, "SELECT", "0"), '+', "OK")
	wantInt(t, c.call(t, "PEXPIRE", "key", "60"), 1)
	time.Sleep(80 * time.Millisecond)
	wantInt(t, c.call(t, "TTL", "key"), -2)
	wantReply(t, c.call(t, "SET", "short", "x", "PX", "60"), '+', "OK")
	time.Sleep(80 * time.Millisecond)
	if v := c.call(t, "GET", "short"); !v.null {
		t.Fatal("expired key remains")
	}
	wantInt(t, c.call(t, "PUBLISH", "ordinary", "message"), 0)
	wantReply(t, c.call(t, "PING"), '+', "PONG")
	info := c.call(t, "INFO", "server")
	if info.kind != '$' || !bytes.Contains(info.text, []byte("redis_version:7.2.10")) {
		t.Fatalf("INFO %+v", info)
	}
	wantReply(t, c.call(t, "QUIT"), '+', "OK")
	select {
	case e = <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("quit did not close")
	}
}
func TestHelloAuth(t *testing.T) {
	a, e := NewAcceptor(Options{}, true, func(s string) bool { return s == "secret" })
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	c, _ := fakeClientForTest(t, a)
	v := c.call(t, "HELLO", "2")
	if v.kind != '-' || !bytes.HasPrefix(v.text, []byte("NOAUTH")) {
		t.Fatalf("hello before auth %+v", v)
	}
	v = c.call(t, "HELLO", "2", "AUTH", "default", "secret", "SETNAME", "app")
	if v.kind != '*' || len(v.array) != 14 {
		t.Fatalf("HELLO AUTH %+v", v)
	}
	wantReply(t, c.call(t, "CLIENT", "GETNAME"), '$', "app")
	v = c.call(t, "HELLO", "3")
	if v.kind != '-' || !bytes.HasPrefix(v.text, []byte("NOPROTO")) {
		t.Fatalf("RESP3 %+v", v)
	}
}
func TestFakeClientsShareDatabase(t *testing.T) {
	a, err := NewAcceptor(Options{}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	one, _ := fakeClientForTest(t, a)
	two, _ := fakeClientForTest(t, a)
	wantReply(t, one.call(t, "SET", "shared", "value"), '+', "OK")
	wantReply(t, two.call(t, "GET", "shared"), '$', "value")
	wantReply(t, two.call(t, "SELECT", "1"), '+', "OK")
	if got := two.call(t, "GET", "shared"); !got.null {
		t.Fatal("database isolation failed")
	}
}

func TestFakeStoreBoundsAndConcurrency(t *testing.T) {
	s := newFakeStore()
	if ok, full := s.set(0, []byte("big"), make([]byte, fakeMaxBytes), "", time.Time{}); ok || !full {
		t.Fatal("size limit not enforced")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%d", i))
			for j := 0; j < 100; j++ {
				s.set(i, key, []byte("value"), "", time.Time{})
				s.get(i, key)
				s.exists(i, [][]byte{key})
				s.del(i, [][]byte{key})
			}
		}(i)
	}
	wg.Wait()
	large := bytes.Repeat([]byte("v"), 1<<20)
	if ok, full := s.set(0, []byte("large"), large, "", time.Time{}); !ok || full {
		t.Fatal("large test value rejected")
	}
	if _, _, tooLarge := s.getMany(0, [][]byte{[]byte("large"), []byte("large"), []byte("large"), []byte("large"), []byte("large")}); !tooLarge {
		t.Fatal("MGET response limit not enforced")
	}
	s.del(0, [][]byte{[]byte("large")})
	for i := 0; i < fakeMaxEntries; i++ {
		if ok, full := s.set(0, []byte(fmt.Sprintf("entry-%d", i)), nil, "", time.Time{}); !ok || full {
			t.Fatalf("entry %d rejected", i)
		}
	}
	if ok, full := s.set(0, []byte("extra"), nil, "", time.Time{}); ok || !full {
		t.Fatal("entry limit not enforced")
	}
}
func TestFakeIdleAndAcceptorClose(t *testing.T) {
	a, e := NewAcceptor(Options{HandshakeTimeout: time.Second, CommandIdleTimeout: 50 * time.Millisecond}, false, nil)
	if e != nil {
		t.Fatal(e)
	}
	c, done := fakeClientForTest(t, a)
	wantReply(t, c.call(t, "PING"), '+', "PONG")
	select {
	case e = <-done:
		if !errors.Is(e, context.DeadlineExceeded) {
			t.Fatalf("idle: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("idle timeout not enforced")
	}
	c2, done2 := fakeClientForTest(t, a)
	wantReply(t, c2.call(t, "PING"), '+', "PONG")
	a.Close()
	select {
	case e = <-done2:
		if !errors.Is(e, io.ErrClosedPipe) {
			t.Fatalf("close: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("fake connection not closed")
	}
}

// SHINE_REDIS_SERVER enables differential testing against a real Redis 7.2 binary.
func TestRealRedisDifferential(t *testing.T) {
	binary := os.Getenv("SHINE_REDIS_SERVER")
	if binary == "" {
		t.Skip("set SHINE_REDIS_SERVER to a Redis 7.2 executable")
	}
	probe, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	probe.Close()
	cmd := exec.Command(binary, "--bind", "127.0.0.1", "--port", fmt.Sprint(port), "--requirepass", "secret", "--save", "", "--appendonly", "no")
	cmd.Dir = t.TempDir()
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	var real net.Conn
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		real, e = net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if e == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if e != nil {
		t.Fatal(e)
	}
	defer real.Close()
	real.SetDeadline(time.Now().Add(5 * time.Second))
	rc := &redisTestClient{real, bufio.NewReader(real)}
	a, e := NewAcceptor(Options{}, true, func(s string) bool { return s == "secret" })
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	fc, _ := fakeClientForTest(t, a)
	cases := [][]string{{"PING"}, {"AUTH", "bad"}, {"PING"}, {"AUTH", "secret"}, {"PING"}, {"PING", "payload"}, {"ECHO", "hello"}, {"SELECT", "0"}, {"DEL", "item", "missing"}, {"GET", "missing"}, {"SET", "bad", "x", "EX", "bad"}, {"SET", "bad", "x", "EX", "0"}, {"SET", "item", "v", "NX"}, {"SET", "item", "x", "NX"}, {"GET", "item"}, {"EXISTS", "item", "item"}, {"TYPE", "item"}, {"MGET", "item", "missing"}, {"SELECT", "2"}, {"GET", "item"}, {"SET", "item", "v2"}, {"SELECT", "0"}, {"EXPIRE", "item", "0"}, {"TTL", "item"}, {"CLIENT", "SETNAME", "app"}, {"CLIENT", "GETNAME"}, {"CLIENT", "SETINFO", "LIB-NAME", "myapp"}, {"AUTH", "bad"}, {"PING"}, {"PUBLISH", "unsubscribed", "data"}, {"SELECT", "16"}, {"QUIT"}}
	for _, parts := range cases {
		if parts[0] == "QUIT" {
			gotHello := fc.call(t, "HELLO", "2")
			wantHello := rc.call(t, "HELLO", "2")
			if len(gotHello.array) != 14 || len(wantHello.array) != 14 {
				t.Fatalf("HELLO fake %+v real %+v", gotHello, wantHello)
			}
			gotHello.array[7].number = 0
			wantHello.array[7].number = 0
			if !reflect.DeepEqual(gotHello, wantHello) {
				t.Fatalf("HELLO fake %+v real %+v", gotHello, wantHello)
			}
		}
		got := fc.call(t, parts...)
		want := rc.call(t, parts...)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%v: fake %+v real %+v", parts, got, want)
		}
	}
}

func TestFakeConnectionIdleExtendsPastHandshake(t *testing.T) {
	a, err := NewAcceptor(Options{HandshakeTimeout: 30 * time.Millisecond, CommandIdleTimeout: 150 * time.Millisecond}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	c, done := fakeClientForTest(t, a)
	wantReply(t, c.call(t, "PING"), '+', "PONG")
	time.Sleep(70 * time.Millisecond)
	wantReply(t, c.call(t, "PING"), '+', "PONG")
	select {
	case e := <-done:
		if !errors.Is(e, context.DeadlineExceeded) {
			t.Fatalf("idle timeout: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("idle timeout not enforced")
	}
}
func TestRESPAggregateLimits(t *testing.T) {
	data := []byte("*2\r\n*1\r\n$4\r\n1234\r\n*1\r\n$4\r\n5678\r\n")
	if _, err := readValue(bufio.NewReader(bytes.NewReader(data)), 7, 0); err == nil {
		t.Fatal("accepted nested payloads exceeding aggregate limit")
	}
	if _, err := readValue(bufio.NewReader(bytes.NewReader(data)), 8, 0); err != nil {
		t.Fatal(err)
	}
}

// SHINE_REDIS_CLI enables an integration test with the real Redis CLI.
func TestRealRedisCLI(t *testing.T) {
	cli := os.Getenv("SHINE_REDIS_CLI")
	if cli == "" {
		t.Skip("set SHINE_REDIS_CLI to a redis-cli executable")
	}
	a, err := NewAcceptor(Options{}, true, func(s string) bool { return s == "secret" })
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			go func() {
				stream, _ := a.AcceptConn(ctx, conn)
				if stream != nil {
					stream.Close()
				}
			}()
		}
	}()
	port := fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
	for _, tc := range []struct {
		args []string
		want string
	}{{[]string{"PING"}, "PONG\n"}, {[]string{"SET", "cli-key", "cli-value"}, "OK\n"}, {[]string{"GET", "cli-key"}, "cli-value\n"}} {
		args := append([]string{"-h", "127.0.0.1", "-p", port, "-a", "secret", "--no-auth-warning", "--raw"}, tc.args...)
		cmd := exec.Command(cli, args...)
		output, e := cmd.CombinedOutput()
		if e != nil || string(output) != tc.want {
			t.Fatalf("%v: %q %v", tc.args, output, e)
		}
	}
}
