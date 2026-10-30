package xray

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
func launchXray(t *testing.T, config any, port int) {
	t.Helper()
	bin := os.Getenv("SHINE_XRAY_BIN")
	if bin == "" {
		t.Skip("set SHINE_XRAY_BIN to a patched Xray executable")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data, e := json.Marshal(config)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	log, e := os.Create(filepath.Join(dir, "xray.log"))
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(bin, "run", "-config", path)
	cmd.Stdout = log
	cmd.Stderr = log
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait(); log.Close() })
	until := time.Now().Add(8 * time.Second)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for time.Now().Before(until) {
		c, e := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if e == nil {
			c.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	data, _ = os.ReadFile(filepath.Join(dir, "xray.log"))
	t.Fatalf("Xray did not start: %s", data)
}
func socksConn(t *testing.T, port int, cmd byte, dest *net.TCPAddr) (net.Conn, []byte) {
	t.Helper()
	c, e := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if e != nil {
		t.Fatal(e)
	}
	_ = c.SetDeadline(time.Now().Add(8 * time.Second))
	if _, e = c.Write([]byte{5, 1, 0}); e != nil {
		t.Fatal(e)
	}
	b := make([]byte, 2)
	if _, e = io.ReadFull(c, b); e != nil || b[1] != 0 {
		t.Fatalf("SOCKS greeting %x: %v", b, e)
	}
	ip := dest.IP.To4()
	req := []byte{5, cmd, 0, 1, ip[0], ip[1], ip[2], ip[3], byte(dest.Port >> 8), byte(dest.Port)}
	if _, e = c.Write(req); e != nil {
		t.Fatal(e)
	}
	resp := make([]byte, 10)
	if _, e = io.ReadFull(c, resp); e != nil || resp[1] != 0 {
		t.Fatalf("SOCKS request %x: %v", resp, e)
	}
	return c, resp
}

// probe forwards the transport while counting physical connections.
func probe(t *testing.T, target string) (int, *atomic.Int32) {
	t.Helper()
	l, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { l.Close() })
	count := new(atomic.Int32)
	go func() {
		for {
			client, e := l.Accept()
			if e != nil {
				return
			}
			count.Add(1)
			go func() {
				defer client.Close()
				up, e := net.Dial("tcp4", target)
				if e != nil {
					return
				}
				defer up.Close()
				done := make(chan struct{})
				go func() { io.Copy(up, client); close(done) }()
				io.Copy(client, up)
				up.Close()
				<-done
			}()
		}
	}()
	return l.Addr().(*net.TCPAddr).Port, count
}

func TestXrayEndToEnd(t *testing.T) {
	if os.Getenv("SHINE_XRAY_BIN") == "" {
		t.Skip("set SHINE_XRAY_BIN to a patched Xray executable")
	}
	echo, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer echo.Close()
	go func() {
		for {
			c, e := echo.Accept()
			if e != nil {
				return
			}
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()
	udp, e := net.ListenPacket("udp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer udp.Close()
	go func() {
		b := make([]byte, 65535)
		for {
			n, a, e := udp.ReadFrom(b)
			if e != nil {
				return
			}
			udp.WriteTo(b[:n], a)
		}
	}()
	serverPort, clientPort := freePort(t), freePort(t)
	password := "integration-secret"
	serverUsers := []any{map[string]any{"auth": password, "email": "alice@example.com", "level": 2}, map[string]any{"auth": "other-secret", "email": "bob@example.com"}}
	server := map[string]any{"log": map[string]any{"loglevel": "warning"}, "inbounds": []any{map[string]any{"listen": "127.0.0.1", "port": serverPort, "protocol": "shine", "settings": map[string]any{"users": serverUsers}}}, "outbounds": []any{map[string]any{"protocol": "blackhole", "tag": "blocked"}, map[string]any{"protocol": "freedom", "tag": "direct"}}, "routing": map[string]any{"rules": []any{map[string]any{"type": "field", "user": []string{"alice@example.com"}, "outboundTag": "direct"}}}}
	clientUsers := []any{map[string]any{"auth": password, "email": "alice@example.com", "level": 2}}
	client := map[string]any{"log": map[string]any{"loglevel": "warning"}, "inbounds": []any{map[string]any{"listen": "127.0.0.1", "port": clientPort, "protocol": "socks", "settings": map[string]any{"udp": true}}}, "outbounds": []any{map[string]any{"protocol": "shine", "settings": map[string]any{"address": "127.0.0.1", "port": serverPort, "users": clientUsers}}}}
	launchXray(t, server, serverPort)
	proxyPort, physicalCount := probe(t, fmt.Sprintf("127.0.0.1:%d", serverPort))
	client["outbounds"].([]any)[0].(map[string]any)["settings"].(map[string]any)["port"] = proxyPort
	launchXray(t, client, clientPort)
	// Concurrent independent sessions share the outbound link.
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, e := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", clientPort), time.Second)
			if e != nil {
				errs <- e
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(8 * time.Second))
			if _, e = c.Write([]byte{5, 1, 0}); e != nil {
				errs <- e
				return
			}
			b := make([]byte, 2)
			if _, e = io.ReadFull(c, b); e != nil || b[1] != 0 {
				errs <- fmt.Errorf("method %x %v", b, e)
				return
			}
			dest := echo.Addr().(*net.TCPAddr)
			req := []byte{5, 1, 0, 1, 127, 0, 0, 1, byte(dest.Port >> 8), byte(dest.Port)}
			if _, e = c.Write(req); e != nil {
				errs <- e
				return
			}
			reply := make([]byte, 10)
			if _, e = io.ReadFull(c, reply); e != nil || reply[1] != 0 {
				errs <- fmt.Errorf("connect %x %v", reply, e)
				return
			}
			msg := []byte(fmt.Sprintf("hello-%d", i))
			if _, e = c.Write(msg); e != nil {
				errs <- e
				return
			}
			got := make([]byte, len(msg))
			if _, e = io.ReadFull(c, got); e != nil || string(got) != string(msg) {
				errs <- fmt.Errorf("echo %q %v", got, e)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if t.Failed() {
		return
	}
	t.Run("IPv6", func(t *testing.T) {
		listener, err := net.Listen("tcp6", "[::1]:0")
		if err != nil {
			t.Skipf("IPv6 loopback unavailable: %v", err)
		}
		defer listener.Close()
		go func() {
			c, e := listener.Accept()
			if e == nil {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}
		}()
		conn, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", clientPort), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
		if _, err = conn.Write([]byte{5, 1, 0}); err != nil {
			t.Fatal(err)
		}
		resp := make([]byte, 2)
		if _, err = io.ReadFull(conn, resp); err != nil || resp[1] != 0 {
			t.Fatalf("greeting %x %v", resp, err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		request := append([]byte{5, 1, 0, 4}, net.ParseIP("::1").To16()...)
		request = append(request, byte(port>>8), byte(port))
		if _, err = conn.Write(request); err != nil {
			t.Fatal(err)
		}
		reply := make([]byte, 10)
		if _, err = io.ReadFull(conn, reply); err != nil || reply[1] != 0 {
			t.Fatalf("connect %x %v", reply, err)
		}
		if _, err = conn.Write([]byte("ipv6")); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 4)
		if _, err = io.ReadFull(conn, got); err != nil || string(got) != "ipv6" {
			t.Fatalf("reply %q %v", got, err)
		}
	})
	control, reply := socksConn(t, clientPort, 3, &net.TCPAddr{IP: net.IPv4zero, Port: 0})
	defer control.Close()
	port := int(binary.BigEndian.Uint16(reply[8:10]))
	u, e := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	if e != nil {
		t.Fatal(e)
	}
	defer u.Close()
	u.SetDeadline(time.Now().Add(8 * time.Second))
	udpPort := udp.LocalAddr().(*net.UDPAddr).Port
	packet := append([]byte{0, 0, 0, 1, 127, 0, 0, 1, byte(udpPort >> 8), byte(udpPort)}, []byte("udp-via-xray")...)
	if _, e = u.Write(packet); e != nil {
		t.Fatal(e)
	}
	b := make([]byte, 1024)
	n, e := u.Read(b)
	if e != nil || string(b[:n]) != string(packet) {
		t.Fatalf("UDP reply %x: %v", b[:n], e)
	}
	// A second destination on the same UDP association keeps its own address.
	other, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	go func() {
		b := make([]byte, 1024)
		n, a, e := other.ReadFrom(b)
		if e == nil {
			_, _ = other.WriteTo(b[:n], a)
		}
	}()
	otherPort := other.LocalAddr().(*net.UDPAddr).Port
	secondPacket := append([]byte{0, 0, 0, 1, 127, 0, 0, 1, byte(otherPort >> 8), byte(otherPort)}, []byte("second-target")...)
	if _, err = u.Write(secondPacket); err != nil {
		t.Fatal(err)
	}
	n, err = u.Read(b)
	if err != nil || string(b[:n]) != string(secondPacket) {
		t.Fatalf("second UDP reply %x: %v", b[:n], err)
	}
	if n := physicalCount.Load(); n != 1 {
		t.Fatalf("expected one shared Shine link, got %d", n)
	}
}
