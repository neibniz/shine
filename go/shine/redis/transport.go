// Package redis provides an optional Redis-like RESP2 transport for Shine.
// It uses two connections and does not implement or require a Redis server.
package redis

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultMaxPayload = 2 << 20

// Options controls the outer transport. Both peers must use the same password.
// Compression controls outgoing messages; incoming messages may be compressed
// regardless of the local setting. No encryption is performed.
type Options struct {
	Password           string
	Compression        bool
	MaxPayload         int
	HandshakeTimeout   time.Duration
	PairTimeout        time.Duration
	CommandIdleTimeout time.Duration // idle timeout for simulated command connections
	MaxPending         int
}

func (o Options) defaults() (Options, error) {
	if o.MaxPayload == 0 {
		o.MaxPayload = DefaultMaxPayload
	}
	if o.HandshakeTimeout == 0 {
		o.HandshakeTimeout = 5 * time.Second
	}
	if o.PairTimeout == 0 {
		o.PairTimeout = 10 * time.Second
	}
	if o.CommandIdleTimeout == 0 {
		o.CommandIdleTimeout = 2 * time.Minute
	}
	if o.MaxPending == 0 {
		o.MaxPending = 256
	}
	if o.MaxPayload < 1024 || o.MaxPayload > 32<<20 || o.HandshakeTimeout < 0 || o.PairTimeout < 0 || o.CommandIdleTimeout < 0 || o.MaxPending < 1 {
		return o, errors.New("redis transport: invalid options")
	}
	return o, nil
}

// DialFunc creates one full-duplex connection. Two connections are dialed for
// each transport. Closing either connection must unblock its reads and writes.
type DialFunc func(context.Context) (io.ReadWriteCloser, error)

// Client pairs a publisher and subscriber connection. On failure both
// connections are closed. The returned stream owns both connections.
func Client(ctx context.Context, dial DialFunc, opts Options) (io.ReadWriteCloser, error) {
	o, err := opts.defaults()
	if err != nil {
		return nil, err
	}
	if dial == nil {
		return nil, errors.New("redis transport: nil dialer")
	}
	ctx, cancel := context.WithTimeout(ctx, o.HandshakeTimeout)
	defer cancel()
	down, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	stopDown := context.AfterFunc(ctx, func() { down.Close() })
	success := false
	defer func() {
		stopDown()
		if !success {
			down.Close()
		}
	}()
	dr := bufio.NewReader(down)
	if err = clientAuth(down, dr, o.Password, o.MaxPayload); err != nil {
		return nil, err
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return nil, err
	}
	base := "events:" + hex.EncodeToString(id[:])
	downChannel, upChannel := base+":out", base+":in"
	if err = writeAll(down, command([]byte("SUBSCRIBE"), []byte(downChannel))); err != nil {
		return nil, err
	}
	ack, err := readValue(dr, o.MaxPayload+1024, 0)
	if err != nil {
		return nil, err
	}
	if ack.kind != '*' || len(ack.array) != 3 || string(ack.array[0].text) != "subscribe" || string(ack.array[1].text) != downChannel || ack.array[2].kind != ':' || ack.array[2].number != 1 {
		return nil, errors.New("redis transport: invalid subscription response")
	}
	up, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	stopUp := context.AfterFunc(ctx, func() { up.Close() })
	defer func() {
		stopUp()
		if !success {
			up.Close()
		}
	}()
	ur := bufio.NewReader(up)
	if err = clientAuth(up, ur, o.Password, o.MaxPayload); err != nil {
		return nil, err
	}
	if err = writeAll(up, command([]byte("PUBLISH"), []byte(upChannel), nil)); err != nil {
		return nil, err
	}
	ack, err = readValue(ur, o.MaxPayload+1024, 0)
	if err != nil {
		return nil, err
	}
	if ack.kind != ':' || ack.number != 1 {
		return nil, errors.New("redis transport: unable to pair connections")
	}
	// Disarm both cancellation callbacks before handing ownership to the stream.
	upStopped := stopUp()
	downStopped := stopDown()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !upStopped || !downStopped {
		return nil, context.Canceled
	}
	success = true
	return &stream{client: true, up: up, down: down, upReader: ur, downReader: dr, upChannel: upChannel, downChannel: downChannel, opts: o, done: make(chan struct{})}, nil
}
func clientAuth(conn io.Writer, r *bufio.Reader, password string, max int) error {
	if password == "" {
		return nil
	}
	if err := writeAll(conn, command([]byte("AUTH"), []byte(password))); err != nil {
		return err
	}
	v, err := readValue(r, max+1024, 0)
	if err != nil {
		return err
	}
	if v.kind != '+' || string(v.text) != "OK" {
		return errors.New("redis transport: authentication failed")
	}
	return nil
}

// Acceptor pairs server-side connections for a Redis-like transport.
// Authenticate is called for AUTH and must be safe for concurrent use.
// When requireAuth is true every connection must authenticate; when false,
// AUTH is rejected, matching a Redis server without configured credentials.
type Acceptor struct {
	opts         Options
	requireAuth  bool
	authenticate func(string) bool
	mu           sync.Mutex
	pending      map[string]*pending
	active       map[*pending]struct{}
	watches      map[*connectionWatch]struct{}
	store        *fakeStore
	started      time.Time
	nextID       atomic.Uint64
	closed       bool
}

func NewAcceptor(opts Options, requireAuth bool, authenticate func(string) bool) (*Acceptor, error) {
	o, e := opts.defaults()
	if e != nil {
		return nil, e
	}
	if requireAuth && authenticate == nil {
		return nil, errors.New("redis transport: missing authenticator")
	}
	return &Acceptor{opts: o, requireAuth: requireAuth, authenticate: authenticate, pending: make(map[string]*pending), active: make(map[*pending]struct{}), watches: make(map[*connectionWatch]struct{}), store: newFakeStore(), started: time.Now()}, nil
}

// Close stops waiting pairs and closes all active paired streams.
func (a *Acceptor) Close() error {
	a.mu.Lock()
	a.closed = true
	all := make([]*pending, 0, len(a.pending))
	for _, p := range a.pending {
		all = append(all, p)
	}
	for p := range a.active {
		all = append(all, p)
	}
	watches := make([]*connectionWatch, 0, len(a.watches))
	for w := range a.watches {
		watches = append(watches, w)
	}
	a.mu.Unlock()
	for _, w := range watches {
		w.abort(io.ErrClosedPipe)
	}
	for _, p := range all {
		p.close()
	}
	return nil
}

type pending struct {
	owner      *Acceptor
	channel    string
	secret     string
	down       io.ReadWriteCloser
	downReader *bufio.Reader
	downMu     sync.Mutex
	mu         sync.Mutex
	up         io.ReadWriteCloser
	closed     bool
	done       chan struct{}
	ready      chan struct{}
	paired     chan struct{}
	once       sync.Once
}

func (p *pending) close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.done)
		up := p.up
		p.mu.Unlock()
		p.down.Close()
		if up != nil {
			up.Close()
		}
		p.owner.mu.Lock()
		if p.owner.pending[p.channel] == p {
			delete(p.owner.pending, p.channel)
		}
		delete(p.owner.active, p)
		p.owner.mu.Unlock()
	})
}
func validChannel(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

// AcceptConn processes one inbound connection. It returns a paired stream on
// the publisher connection, and (nil, nil) on the subscriber connection after
// the stream closes. The caller must keep both invocations alive. AcceptConn
// owns conn in every case, including errors.
func (a *Acceptor) AcceptConn(ctx context.Context, conn io.ReadWriteCloser) (io.ReadWriteCloser, error) {
	if conn == nil {
		return nil, errors.New("redis transport: nil connection")
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		conn.Close()
		return nil, io.ErrClosedPipe
	}
	watch := newConnectionWatch(ctx, conn, a.opts.HandshakeTimeout)
	a.watches[watch] = struct{}{}
	a.mu.Unlock()
	owned := false
	defer func() {
		_ = watch.stop()
		a.mu.Lock()
		delete(a.watches, watch)
		a.mu.Unlock()
		if !owned {
			conn.Close()
		}
	}()
	r := bufio.NewReader(conn)
	state := &fakeSession{authenticated: !a.requireAuth, id: a.nextID.Add(1)}
	for {
		f, err := readValue(r, a.opts.MaxPayload+1024, 0)
		if err != nil {
			return nil, watch.errorOr(err)
		}
		if f.kind != '*' || len(f.array) == 0 || f.array[0].kind != '$' {
			return nil, errors.New("redis transport: expected command array")
		}
		name := strings.ToUpper(string(f.array[0].text))
		if reply, handled, quit := a.fakeReply(f, state); handled {
			if err = writeAll(conn, reply); err != nil {
				return nil, watch.errorOr(err)
			}
			if quit {
				return nil, nil
			}
			watch.reset(a.opts.CommandIdleTimeout)
			continue
		}
		if name == "SUBSCRIBE" && arrayStrings(f, 2) {
			channel := string(f.array[1].text)
			if !validChannel(channel) {
				return nil, errors.New("redis transport: invalid channel")
			}
			p := &pending{owner: a, channel: channel, secret: state.secret, down: conn, downReader: r, done: make(chan struct{}), ready: make(chan struct{}), paired: make(chan struct{})}
			a.mu.Lock()
			if a.closed || len(a.pending) >= a.opts.MaxPending || a.pending[channel] != nil {
				a.mu.Unlock()
				return nil, errors.New("redis transport: subscriber unavailable")
			}
			a.pending[channel] = p
			a.mu.Unlock()
			owned = true
			watch.reset(a.opts.HandshakeTimeout)
			if err = writeAll(conn, subAck(channel)); err != nil {
				p.close()
				return nil, watch.errorOr(err)
			}
			if err := watch.stop(); err != nil {
				p.close()
				return nil, err
			}
			close(p.ready)
			go p.monitor()
			timer := time.NewTimer(a.opts.PairTimeout)
			defer timer.Stop()
			select {
			case <-p.paired:
			case <-p.done:
				return nil, nil
			case <-ctx.Done():
				p.close()
				return nil, ctx.Err()
			case <-timer.C:
				p.close()
				return nil, errors.New("redis transport: pairing timeout")
			}
			select {
			case <-p.done:
				return nil, nil
			case <-ctx.Done():
				p.close()
				return nil, ctx.Err()
			}
		}
		if name == "PUBLISH" && arrayStrings(f, 3) && len(f.array[2].text) == 0 {
			upChannel := string(f.array[1].text)
			downChannel := ""
			if strings.HasSuffix(upChannel, ":in") {
				downChannel = strings.TrimSuffix(upChannel, ":in") + ":out"
			}
			a.mu.Lock()
			p := a.pending[downChannel]
			if p != nil && p.secret == state.secret && !a.closed {
				delete(a.pending, downChannel)
				a.active[p] = struct{}{}
			} else {
				p = nil
			}
			a.mu.Unlock()
			if p == nil {
				if err = writeAll(conn, []byte(":0\r\n")); err != nil {
					return nil, watch.errorOr(err)
				}
				watch.reset(a.opts.CommandIdleTimeout)
				continue
			}
			select {
			case <-p.ready:
			case <-p.done:
				return nil, io.ErrClosedPipe
			case <-ctx.Done():
				p.close()
				return nil, ctx.Err()
			case <-watch.done:
				p.close()
				return nil, watch.errorOr(io.ErrClosedPipe)
			}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				return nil, io.ErrClosedPipe
			}
			p.up = conn
			close(p.paired)
			p.mu.Unlock()
			owned = true
			watch.reset(a.opts.HandshakeTimeout)
			if err = writeAll(conn, []byte(":1\r\n")); err != nil {
				p.close()
				return nil, watch.errorOr(err)
			}
			if err := watch.stop(); err != nil {
				p.close()
				return nil, err
			}
			return &stream{up: conn, down: p.down, upReader: r, downReader: p.downReader, upChannel: upChannel, downChannel: downChannel, opts: a.opts, done: p.done, closeFn: p.close, serverDownMu: &p.downMu}, nil
		}
		if name == "PUBLISH" && arrayStrings(f, 3) {
			if err = writeAll(conn, []byte(":0\r\n")); err != nil {
				return nil, watch.errorOr(err)
			}
			watch.reset(a.opts.CommandIdleTimeout)
			continue
		}
		if err = writeAll(conn, wrongArgs(name)); err != nil {
			return nil, watch.errorOr(err)
		}
		watch.reset(a.opts.CommandIdleTimeout)
	}
}
func (p *pending) monitor() {
	for {
		f, err := readValue(p.downReader, 4096, 0)
		if err != nil {
			p.close()
			return
		}
		if f.kind != '*' || len(f.array) < 1 || f.array[0].kind != '$' || strings.ToUpper(string(f.array[0].text)) != "PING" || len(f.array) > 2 {
			p.close()
			return
		}
		msg := []byte{}
		if len(f.array) == 2 {
			if f.array[1].kind != '$' {
				p.close()
				return
			}
			msg = f.array[1].text
		}
		p.downMu.Lock()
		err = writeAll(p.down, command([]byte("pong"), msg))
		p.downMu.Unlock()
		if err != nil {
			p.close()
			return
		}
	}
}

type stream struct {
	client                 bool
	up, down               io.ReadWriteCloser
	upReader, downReader   *bufio.Reader
	upChannel, downChannel string
	opts                   Options
	done                   chan struct{}
	once                   sync.Once
	closeFn                func()
	serverDownMu           *sync.Mutex
	readMu, writeMu        sync.Mutex
	left                   []byte
}

func (s *stream) Close() error {
	if s.closeFn != nil {
		s.closeFn()
		return nil
	}
	s.once.Do(func() { close(s.done); s.up.Close(); s.down.Close() })
	return nil
}
func (s *stream) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	s.readMu.Lock()
	defer s.readMu.Unlock()
	for len(s.left) == 0 {
		var f value
		var err error
		if s.client {
			f, err = readValue(s.downReader, s.opts.MaxPayload+1024, 0)
			if err == nil && (!arrayStrings(f, 3) || string(f.array[0].text) != "message" || string(f.array[1].text) != s.downChannel) {
				err = errors.New("redis transport: invalid message")
			}
		} else {
			f, err = readValue(s.upReader, s.opts.MaxPayload+1024, 0)
			if err == nil && (!arrayStrings(f, 3) || strings.ToUpper(string(f.array[0].text)) != "PUBLISH" || string(f.array[1].text) != s.upChannel) {
				err = errors.New("redis transport: invalid publish")
			}
			if err == nil {
				err = writeAll(s.up, []byte(":1\r\n"))
			}
		}
		if err != nil {
			s.Close()
			return 0, err
		}
		s.left, err = decodePayload(f.array[2].text, s.opts.MaxPayload)
		if err != nil {
			s.Close()
			return 0, err
		}
	}
	n := copy(b, s.left)
	s.left = s.left[n:]
	return n, nil
}
func (s *stream) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	written := 0
	for len(p) > 0 {
		n := len(p)
		if n > s.opts.MaxPayload {
			n = s.opts.MaxPayload
		}
		payload, err := encodePayload(p[:n], s.opts.Compression, s.opts.MaxPayload)
		if err != nil {
			return written, err
		}
		if s.client {
			err = writeAll(s.up, command([]byte("PUBLISH"), []byte(s.upChannel), payload))
			if err == nil {
				var ack value
				ack, err = readValue(s.upReader, s.opts.MaxPayload+1024, 0)
				if err == nil && (ack.kind != ':' || ack.number != 1) {
					err = errors.New("redis transport: invalid publish response")
				}
			}
		} else {
			s.serverDownMu.Lock()
			err = writeAll(s.down, push("message", s.downChannel, payload))
			s.serverDownMu.Unlock()
		}
		if err != nil {
			s.Close()
			return written, err
		}
		written += n
		p = p[n:]
	}
	return written, nil
}
