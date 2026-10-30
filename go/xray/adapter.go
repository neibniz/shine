// Package xray integrates the independent Shine protocol library with Xray-core.
package xray

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	shine "github.com/hhhflow2020/shine/go/shine"
	rediswire "github.com/hhhflow2020/shine/go/shine/redis"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	xerrors "github.com/xtls/xray-core/common/errors"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/task"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
	"google.golang.org/protobuf/proto"
)

func options(password string, window, maxFrame, maxData, handshake, ping, pong, maxSessions, datagramQueueBytes uint32) shine.Options {
	return shine.Options{Password: password, InitialWindow: window, MaxFrameSize: int(maxFrame), MaxDataSize: int(maxData), HandshakeTimeout: time.Duration(handshake) * time.Second, PingInterval: time.Duration(ping) * time.Second, PongTimeout: time.Duration(pong) * time.Second, MaxSessions: int(maxSessions), DatagramQueueBytes: int(datagramQueueBytes)}
}
func (c *ServerConfig) opts() shine.Options {
	o := options(c.Password, c.InitialWindow, c.MaxFrameSize, c.MaxDataSize, c.HandshakeTimeoutSeconds, c.PingIntervalSeconds, c.PongTimeoutSeconds, c.MaxSessions, c.DatagramQueueBytes)
	for _, u := range c.Users {
		if u == nil {
			o.Users = append(o.Users, shine.User{})
			continue
		}
		o.Users = append(o.Users, shine.User{Auth: u.Auth, Email: u.Email, Level: u.Level})
	}
	return o
}
func (c *ClientConfig) opts(password string) shine.Options {
	return options(password, c.InitialWindow, c.MaxFrameSize, c.MaxDataSize, c.HandshakeTimeoutSeconds, c.PingIntervalSeconds, c.PongTimeoutSeconds, c.MaxSessions, c.DatagramQueueBytes)
}

const defaultMaxTotalSessions = 1024
const defaultUDPFirstPacketTimeout = 30 * time.Second

func validateResourceLimits(maxSessions, maxTotalSessions, datagramQueueBytes uint32) error {
	// These bounds also ensure converting to int works on 32-bit targets.
	if maxSessions > 1<<20 || maxTotalSessions > 1<<20 || datagramQueueBytes > 64<<20 {
		return fmt.Errorf("shine: session or datagram limit is too large")
	}
	return nil
}

// Equals and ToProto implement Xray's protocol.Account without exposing secrets.
func (u *UserIdentity) Equals(other protocol.Account) bool {
	v, ok := other.(*UserIdentity)
	return ok && (u == v || (u.GetEmail() != "" && u.GetEmail() == v.GetEmail()))
}
func (u *UserIdentity) ToProto() proto.Message { return u }

// ValidateServerConfig validates credentials and protocol limits.
func ValidateServerConfig(c *ServerConfig) error {
	if err := validateResourceLimits(c.MaxSessions, c.MaxTotalSessions, c.DatagramQueueBytes); err != nil {
		return err
	}
	if err := validateTransport(c.Transport, c.Compression); err != nil {
		return err
	}
	if err := ValidateSettings(c.MaxFrameSize, c.MaxDataSize); err != nil {
		return err
	}
	return shine.ValidateServerCredentials(c.opts())
}

// ResolveClientPassword returns the configured authentication secret.
// A client outbound accepts exactly one user, or the legacy password field.
func ResolveClientPassword(c *ClientConfig) (string, error) {
	if err := validateResourceLimits(c.MaxSessions, 0, c.DatagramQueueBytes); err != nil {
		return "", err
	}
	if err := validateTransport(c.Transport, c.Compression); err != nil {
		return "", err
	}
	if err := ValidateSettings(c.MaxFrameSize, c.MaxDataSize); err != nil {
		return "", err
	}
	if c.Address == "" || c.Port == 0 || c.Port > 65535 {
		return "", fmt.Errorf("shine: invalid server address or port")
	}
	if len(c.Users) > 1 {
		return "", fmt.Errorf("shine: outbound requires exactly one user")
	}
	if len(c.Users) == 1 {
		if c.Password != "" {
			return "", fmt.Errorf("shine: use either password or users")
		}
		if c.Users[0] == nil || c.Users[0].Auth == "" {
			return "", fmt.Errorf("shine: user auth cannot be empty")
		}
		return c.Users[0].Auth, nil
	}
	return c.Password, nil
}

// Empty transport selects the Redis-style carrier; tcp keeps the original wire format.
func useRedisTransport(name string) bool { return name == "" || name == "redis" }

func validateTransport(name string, compression bool) error {
	if name != "" && name != "tcp" && name != "redis" {
		return fmt.Errorf("shine: unsupported transport %q", name)
	}
	if compression && !useRedisTransport(name) {
		return fmt.Errorf("shine: compression requires redis transport")
	}
	return nil
}

// ValidateSettings checks the protocol frame limits used by both config and handlers.
func ValidateSettings(maxFrame, maxData uint32) error {
	if maxFrame != 0 && (maxFrame < 64 || maxFrame > 512<<20) {
		return fmt.Errorf("shine: maxFrameSize must be between 64 and 512 MiB")
	}
	if maxFrame == 0 {
		maxFrame = 512 << 20
	}
	if maxData == 0 {
		maxData = 256 << 10
	}
	if maxData > maxFrame-9 {
		return fmt.Errorf("shine: maxDataSize exceeds frame limit")
	}
	return nil
}
func init() {
	common.Must(common.RegisterConfig((*ServerConfig)(nil), func(_ context.Context, v interface{}) (interface{}, error) {
		c := v.(*ServerConfig)
		if e := ValidateServerConfig(c); e != nil {
			return nil, e
		}
		maxTotal := c.MaxTotalSessions
		if maxTotal == 0 {
			maxTotal = defaultMaxTotalSessions
		}
		limiter, err := shine.NewSessionLimiter(int(maxTotal))
		if err != nil {
			return nil, err
		}
		in := &Inbound{config: c, limiter: limiter}
		if useRedisTransport(c.Transport) {
			var secrets []string
			if c.Password != "" {
				secrets = append(secrets, c.Password)
			}
			for _, user := range c.Users {
				secrets = append(secrets, user.Auth)
			}
			var authenticate func(string) bool
			if len(secrets) > 0 {
				authenticate = func(password string) bool {
					matched := 0
					for _, secret := range secrets {
						matched |= subtle.ConstantTimeCompare([]byte(password), []byte(secret))
					}
					return matched == 1
				}
			}
			var err error
			in.carrier, err = rediswire.NewAcceptor(rediswire.Options{Compression: c.Compression}, len(secrets) > 0, authenticate)
			if err != nil {
				return nil, err
			}
		}
		return in, nil
	}))
	common.Must(common.RegisterConfig((*ClientConfig)(nil), func(_ context.Context, v interface{}) (interface{}, error) {
		c := v.(*ClientConfig)
		password, e := ResolveClientPassword(c)
		if e != nil {
			return nil, e
		}
		ctx, cancel := context.WithCancel(context.Background())
		return &Outbound{config: c, password: password, done: ctx, cancel: cancel}, nil
	}))
}

var _ proxy.Inbound = (*Inbound)(nil)
var _ proxy.Outbound = (*Outbound)(nil)

// Inbound accepts a Shine link and dispatches each multiplexed session to Xray.
type Inbound struct {
	config  *ServerConfig
	carrier *rediswire.Acceptor
	limiter *shine.SessionLimiter
}

// Close releases pending and active Redis transport connections.
func (h *Inbound) Close() error {
	if h.carrier != nil {
		return h.carrier.Close()
	}
	return nil
}

func (*Inbound) Network() []xnet.Network { return []xnet.Network{xnet.Network_TCP} }
func (h *Inbound) Process(ctx context.Context, network xnet.Network, conn stat.Connection, dispatcher routing.Dispatcher) error {
	if network != xnet.Network_TCP {
		return fmt.Errorf("shine: unsupported transport network %v", network)
	}
	var stream io.ReadWriteCloser = conn
	if h.carrier != nil {
		var err error
		stream, err = h.carrier.AcceptConn(ctx, conn)
		if err != nil {
			return err
		}
		if stream == nil {
			return nil
		}
	}
	opts := h.config.opts()
	opts.SessionLimiter = h.limiter
	link, err := shine.Server(ctx, stream, opts)
	if err != nil {
		return err
	}
	identity, authenticated := link.AuthenticatedUser()
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() { cancel(); link.Close(); wg.Wait() }()
	for {
		s, e := link.Accept(ctx)
		if e != nil {
			if ctx.Err() != nil || e == io.EOF {
				return nil
			}
			return e
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.Close()
			sessionCtx := childContext(ctx, identity, authenticated)
			if err := h.dispatchSession(sessionCtx, s, dispatcher); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
				xerrors.LogInfoInner(sessionCtx, err, "shine session ended")
			}
		}()
	}
}
func childContext(ctx context.Context, identity shine.Identity, authenticated bool) context.Context {
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	if in := session.InboundFromContext(ctx); in != nil {
		copyIn := *in
		copyIn.Name = "shine"
		copyIn.CanSpliceCopy = 3
		if authenticated && identity.User {
			copyIn.User = &protocol.MemoryUser{Email: identity.Email, Level: identity.Level, Account: &UserIdentity{Email: identity.Email, Level: identity.Level}}
		}
		ctx = session.ContextWithInbound(ctx, &copyIn)
	}
	content := session.ContentFromContext(ctx)
	if content != nil {
		c := *content
		if content.Attributes != nil {
			c.Attributes = make(map[string]string, len(content.Attributes))
			for k, v := range content.Attributes {
				c.Attributes[k] = v
			}
		}
		ctx = session.ContextWithContent(ctx, &c)
	} else {
		ctx = session.ContextWithContent(ctx, &session.Content{})
	}
	return ctx
}
func destination(a shine.Address, network xnet.Network) xnet.Destination {
	return xnet.Destination{Network: network, Address: xnet.ParseAddress(a.Host), Port: xnet.Port(a.Port)}
}
func shineAddress(d xnet.Destination) (shine.Address, error) {
	if !d.IsValid() || d.Address == nil {
		return shine.Address{}, fmt.Errorf("shine: invalid destination")
	}
	host := ""
	if d.Address.Family().IsIP() {
		host = d.Address.IP().String()
	} else {
		host = d.Address.Domain()
	}
	return shine.Address{Host: host, Port: uint16(d.Port)}, nil
}
func (h *Inbound) dispatchSession(ctx context.Context, s *shine.Session, d routing.Dispatcher) error {
	if s.IsUDP() {
		timeout := defaultUDPFirstPacketTimeout
		if h.config.UdpFirstPacketTimeoutSeconds != 0 {
			timeout = time.Duration(h.config.UdpFirstPacketTimeoutSeconds) * time.Second
		}
		firstCtx, cancel := context.WithTimeout(ctx, timeout)
		first, e := s.ReceiveDatagram(firstCtx)
		cancel()
		if e != nil {
			return e
		}
		target := destination(first.Target, xnet.Network_UDP)
		link, e := d.Dispatch(ctx, target)
		if e != nil {
			return e
		}
		defer common.Interrupt(link.Reader)
		defer common.Interrupt(link.Writer)
		return relayUDP(ctx, s, link, target, &first)
	}
	target := destination(s.Target(), xnet.Network_TCP)
	link, e := d.Dispatch(ctx, target)
	if e != nil {
		return e
	}
	defer common.Interrupt(link.Reader)
	defer common.Interrupt(link.Writer)
	return relayTCP(ctx, s, link)
}

// Outbound shares one live Shine transport among Xray requests.
type Outbound struct {
	config     *ClientConfig
	password   string
	mu         sync.Mutex
	link       *shine.Link
	connecting chan struct{}
	done       context.Context
	cancel     context.CancelFunc
}

func (h *Outbound) Close() error {
	h.cancel()
	h.mu.Lock()
	l := h.link
	h.link = nil
	h.mu.Unlock()
	if l != nil {
		return l.Close()
	}
	return nil
}
func (h *Outbound) getLink(ctx context.Context, dialer internet.Dialer) (*shine.Link, error) {
	for {
		h.mu.Lock()
		if h.done.Err() != nil {
			h.mu.Unlock()
			return nil, io.ErrClosedPipe
		}
		if h.link != nil {
			select {
			case <-h.link.Done():
				h.link = nil
			default:
				l := h.link
				h.mu.Unlock()
				return l, nil
			}
		}
		if h.connecting != nil {
			wait := h.connecting
			h.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-h.done.Done():
				return nil, io.ErrClosedPipe
			}
		}
		wait := make(chan struct{})
		h.connecting = wait
		h.mu.Unlock()
		dialCtx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(h.done, cancel)
		dest := xnet.TCPDestination(xnet.ParseAddress(h.config.Address), xnet.Port(h.config.Port))
		var conn io.ReadWriteCloser
		var err error
		if useRedisTransport(h.config.Transport) {
			conn, err = rediswire.Client(dialCtx, func(ctx context.Context) (io.ReadWriteCloser, error) {
				return dialer.Dial(ctx, dest)
			}, rediswire.Options{Password: h.password, Compression: h.config.Compression})
		} else {
			conn, err = dialer.Dial(dialCtx, dest)
		}
		var l *shine.Link
		if err == nil {
			l, err = shine.Client(dialCtx, conn, h.config.opts(h.password))
		}
		stop()
		cancel()
		h.mu.Lock()
		if h.done.Err() != nil && l != nil {
			l.Close()
			l = nil
			err = io.ErrClosedPipe
		}
		if err == nil {
			h.link = l
		}
		h.connecting = nil
		close(wait)
		h.mu.Unlock()
		return l, err
	}
}
func (h *Outbound) Process(ctx context.Context, link *transport.Link, dialer internet.Dialer) error {
	outbounds := session.OutboundsFromContext(ctx)
	if len(outbounds) == 0 {
		return fmt.Errorf("shine: missing outbound target")
	}
	ob := outbounds[len(outbounds)-1]
	ob.Name = "shine"
	ob.CanSpliceCopy = 3
	target := ob.Target
	if target.Network != xnet.Network_TCP && target.Network != xnet.Network_UDP {
		return fmt.Errorf("shine: unsupported target network %v", target.Network)
	}
	a, err := shineAddress(target)
	if err != nil {
		return err
	}
	physical, err := h.getLink(ctx, dialer)
	if err != nil {
		return err
	}
	var s *shine.Session
	if target.Network == xnet.Network_UDP {
		s, err = physical.OpenUDP(ctx)
	} else {
		s, err = physical.Open(ctx, a)
	}
	if err != nil {
		return err
	}
	defer s.Close()
	defer common.Interrupt(link.Reader)
	defer common.Interrupt(link.Writer)
	if target.Network == xnet.Network_UDP {
		return relayUDP(ctx, s, link, target, nil)
	}
	return relayTCP(ctx, s, link)
}
func relayTCP(ctx context.Context, s *shine.Session, link *transport.Link) error {
	up := func() error {
		if e := buf.Copy(link.Reader, buf.NewWriter(s)); e != nil {
			return e
		}
		return s.CloseWrite()
	}
	down := func() error {
		if e := buf.Copy(buf.NewReader(s), link.Writer); e != nil {
			return e
		}
		return common.Close(link.Writer)
	}
	return task.Run(ctx, up, down)
}
func relayUDP(ctx context.Context, s *shine.Session, link *transport.Link, defaultTarget xnet.Destination, first *shine.Datagram) error {
	// outbound=true: Xray -> Shine; inbound=true: Shine -> Xray. The same
	// bidirectional loops apply; first is only set for a newly accepted session.
	if first != nil {
		b := buf.FromBytes(first.Data)
		target := destination(first.Target, xnet.Network_UDP)
		b.UDP = &target
		if err := link.Writer.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
			return err
		}
	}
	toShine := func() error {
		for {
			mb, err := link.Reader.ReadMultiBuffer()
			for i, b := range mb {
				if b == nil {
					continue
				}
				target := defaultTarget
				if b.UDP != nil {
					target = *b.UDP
				}
				a, e := shineAddress(target)
				if e == nil {
					e = s.SendDatagram(ctx, b.Bytes(), a)
				}
				b.Release()
				mb[i] = nil
				if e != nil {
					buf.ReleaseMulti(mb)
					return e
				}
			}
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
	}
	fromShine := func() error {
		for {
			d, err := s.ReceiveDatagram(ctx)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			b := buf.FromBytes(d.Data)
			target := destination(d.Target, xnet.Network_UDP)
			b.UDP = &target
			if err = link.Writer.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
				return err
			}
		}
	}
	// UDP has no half-close: the association ends when either side ends.
	// The caller interrupts the Xray pipes and closes the session on return,
	// which wakes the other pump without waiting for an idle timeout.
	finished := make(chan error, 2)
	go func() { finished <- toShine() }()
	go func() { finished <- fromShine() }()
	select {
	case err := <-finished:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
