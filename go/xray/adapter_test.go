package xray

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	shine "github.com/hhhflow2020/shine/go/shine"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet/stat"
)

type testDialer struct {
	count   atomic.Int32
	servers chan *shine.Link
}

func (d *testDialer) Dial(ctx context.Context, _ xnet.Destination) (stat.Connection, error) {
	d.count.Add(1)
	c, s := net.Pipe()
	go func() {
		l, e := shine.Server(context.Background(), s, shine.Options{})
		if e == nil {
			d.servers <- l
		}
	}()
	return c, nil
}
func (*testDialer) DestIpAddress() xnet.IP                                { return nil }
func (*testDialer) SetOutboundGateway(context.Context, *session.Outbound) {}
func TestRegisteredConfigsAndLinkReuse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	v, e := common.CreateObject(ctx, &ClientConfig{Address: "127.0.0.1", Port: 9999, Transport: "tcp"})
	if e != nil {
		t.Fatal(e)
	}
	h := v.(*Outbound)
	defer h.Close()
	if _, e = common.CreateObject(ctx, &ServerConfig{}); e != nil {
		t.Fatal(e)
	}
	d := &testDialer{servers: make(chan *shine.Link, 4)}
	links := make(chan *shine.Link, 20)
	errs := make(chan error, 20)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, e := h.getLink(ctx, d)
			if e != nil {
				errs <- e
			} else {
				links <- l
			}
		}()
	}
	wg.Wait()
	close(links)
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	var first *shine.Link
	for l := range links {
		if first == nil {
			first = l
		} else if l != first {
			t.Fatal("multiple physical links")
		}
	}
	if d.count.Load() != 1 {
		t.Fatalf("dial count %d", d.count.Load())
	}
	peer := <-d.servers
	peer.Close()
	select {
	case <-first.Done():
	case <-ctx.Done():
		t.Fatal("link did not close")
	}
	second, e := h.getLink(ctx, d)
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close()
	peer2 := <-d.servers
	defer peer2.Close()
	if first == second || d.count.Load() != 2 {
		t.Fatal("link did not reconnect")
	}
	h.Close()
	if _, e = h.getLink(ctx, d); e == nil {
		t.Fatal("closed outbound dialed again")
	}
}

func TestAddressConversion(t *testing.T) {
	for _, host := range []string{"::1", "127.0.0.1", "example.com"} {
		d := xnet.TCPDestination(xnet.ParseAddress(host), 443)
		a, err := shineAddress(d)
		if err != nil || a.Host != host || a.Port != 443 {
			t.Fatalf("%s: %+v %v", host, a, err)
		}
		back := destination(a, xnet.Network_TCP)
		if back.Address.Family() != d.Address.Family() || back.Address.String() != d.Address.String() {
			t.Fatalf("address round trip %v to %v", d, back)
		}
	}
}

type eofReader struct{}

func (eofReader) ReadMultiBuffer() (buf.MultiBuffer, error) { return nil, io.EOF }

type discardWriter struct{}

func (discardWriter) WriteMultiBuffer(b buf.MultiBuffer) error { buf.ReleaseMulti(b); return nil }

func TestUDPStopsWhenXrayInputCloses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	obj, err := common.CreateObject(ctx, &ClientConfig{Address: "127.0.0.1", Port: 1234, Transport: "tcp"})
	if err != nil {
		t.Fatal(err)
	}
	outbound := obj.(*Outbound)
	defer outbound.Close()
	dialer := &testDialer{servers: make(chan *shine.Link, 1)}
	l, err := outbound.getLink(ctx, dialer)
	if err != nil {
		t.Fatal(err)
	}
	peer := <-dialer.servers
	defer peer.Close()
	s, err := l.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	completed := make(chan error, 1)
	go func() {
		completed <- relayUDP(ctx, s, &transport.Link{Reader: eofReader{}, Writer: discardWriter{}}, xnet.UDPDestination(xnet.ParseAddress("127.0.0.1"), 53), nil)
	}()
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("UDP association did not stop on EOF")
	}
}

func TestAuthenticatedIdentityInContext(t *testing.T) {
	original := &session.Inbound{Tag: "shine-in"}
	ctx := session.ContextWithInbound(context.Background(), original)
	got := session.InboundFromContext(childContext(ctx, shine.Identity{Email: "a@example.com", Level: 2, User: true}, true))
	if got == original || got.Tag != "shine-in" || got.User == nil || got.User.Email != "a@example.com" || got.User.Level != 2 {
		t.Fatalf("bad inbound metadata: %+v", got)
	}
	account, ok := got.User.Account.(*UserIdentity)
	if !ok || account.Email != "a@example.com" || account.Level != 2 {
		t.Fatalf("bad account: %+v", got.User.Account)
	}
	if original.User != nil {
		t.Fatal("original metadata mutated")
	}
}

func TestDefaultTransport(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		transport string
		redis     bool
	}{{"default", "", true}, {"explicit redis", "redis", true}, {"legacy tcp", "tcp", false}} {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := common.CreateObject(ctx, &ServerConfig{Transport: tc.transport})
			if err != nil {
				t.Fatal(err)
			}
			in := obj.(*Inbound)
			defer in.Close()
			if (in.carrier != nil) != tc.redis {
				t.Fatalf("transport %q carrier=%v", tc.transport, in.carrier != nil)
			}
		})
	}
}

func TestInboundSharedLimitAndIdleUDPTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	obj, err := common.CreateObject(ctx, &ServerConfig{Transport: "tcp", MaxSessions: 3, MaxTotalSessions: 1, UdpFirstPacketTimeoutSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	inbound := obj.(*Inbound)
	defer inbound.Close()
	// Occupy the inbound-wide limit using another server link.
	c1, s1 := net.Pipe()
	ready := make(chan *shine.Link, 1)
	go func() {
		l, e := shine.Server(ctx, s1, shine.Options{SessionLimiter: inbound.limiter})
		if e == nil {
			ready <- l
		}
	}()
	client1, err := shine.Client(ctx, c1, shine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client1.Close()
	server1 := <-ready
	defer server1.Close()
	if _, err = client1.OpenUDP(ctx); err != nil {
		t.Fatal(err)
	}
	occupied, err := server1.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c2, s2 := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- inbound.Process(ctx, xnet.Network_TCP, s2, nil) }()
	client2, err := shine.Client(ctx, c2, shine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()
	excess, err := client2.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = excess.ReceiveDatagram(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared limit did not reset excess session: %v", err)
	}
	if err = occupied.Close(); err != nil {
		t.Fatal(err)
	}
	idle, err := client2.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	if _, err = idle.ReceiveDatagram(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("idle session did not close on timeout: %v", err)
	}
	// The timeout must free its reservation so a new session can be opened.
	next, err := client2.OpenUDP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if _, err = next.ReceiveDatagram(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("slot not reusable after timeout: %v", err)
	}
	client2.Close()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("inbound did not stop")
	}
}
