package shine

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// User associates one authentication secret with optional Xray-style metadata.
// Only Auth is sent over the Shine v1 wire protocol.
type User struct {
	Auth  string
	Email string
	Level uint32
}

// Identity is the server-side metadata for an authenticated user.
type Identity struct {
	Email string
	Level uint32
	User  bool // true if authentication matched a configured User
}

type authEntry struct {
	password string
	identity Identity
}

// Options configures a link. Zero values use the protocol defaults.
// Both peers must use the same InitialWindow when interoperating with the C++ implementation.
type Options struct {
	Password           string // client password, or a single server password
	Users              []User // server-side password-to-identity mapping
	InitialWindow      uint32
	MaxFrameSize       int // maximum RESP bulk size and total array contents
	MaxDataSize        int // maximum DATA payload emitted per frame
	HandshakeTimeout   time.Duration
	PingInterval       time.Duration
	PongTimeout        time.Duration
	AcceptQueue        int
	DatagramQueue      int             // maximum queued packets per UDP session
	DatagramQueueBytes int             // accounted payload and address bytes per UDP session
	MaxSessions        int             // active TCP and UDP sessions per link
	SessionLimiter     *SessionLimiter // optional limit shared across links
}

func (o Options) defaults() (Options, error) {
	if o.InitialWindow == 0 {
		o.InitialWindow = 256 << 10
	}
	if o.MaxFrameSize == 0 {
		o.MaxFrameSize = maxBulk
	}
	if o.MaxDataSize == 0 {
		o.MaxDataSize = 256 << 10
	}
	if o.HandshakeTimeout == 0 {
		o.HandshakeTimeout = 5 * time.Second
	}
	if o.PingInterval == 0 {
		o.PingInterval = 15 * time.Second
	}
	if o.PongTimeout == 0 {
		o.PongTimeout = 10 * time.Second
	}
	if o.AcceptQueue == 0 {
		o.AcceptQueue = 128
	}
	if o.DatagramQueue == 0 {
		o.DatagramQueue = 1024
	}
	if o.DatagramQueueBytes == 0 {
		o.DatagramQueueBytes = DefaultDatagramQueueBytes
	}
	if o.MaxSessions == 0 {
		o.MaxSessions = DefaultMaxSessions
	}
	if o.MaxFrameSize < 64 || o.MaxFrameSize > maxBulk || o.MaxDataSize <= 0 || o.MaxDataSize > o.MaxFrameSize-9 || o.HandshakeTimeout < 0 || o.PingInterval < 0 || o.PongTimeout < 0 || o.AcceptQueue < 1 || o.DatagramQueue < 1 || o.DatagramQueueBytes < 1 || o.DatagramQueueBytes > maxDatagramQueueBytes || o.MaxSessions < 1 || o.MaxSessions > maxSessionLimit || (o.SessionLimiter != nil && o.SessionLimiter.limit == 0) {
		return o, errors.New("shine: invalid options")
	}
	return o, nil
}

// Link owns a full-duplex transport and multiplexes sessions over it.
// Close closes the underlying transport and all sessions.
type Link struct {
	conn          io.ReadWriteCloser
	reader        *bufio.Reader
	opts          Options
	server        bool
	send          chan []byte
	incoming      chan *Session
	done          chan struct{}
	once          sync.Once
	mu            sync.Mutex
	sessions      map[uint64]*Session
	err           error
	pong          chan uint64
	authEntries   []authEntry
	identity      Identity
	authenticated bool
}

// Client performs the client handshake. The link takes ownership of conn,
// including on error. Closing conn must unblock concurrent reads and writes.
func Client(ctx context.Context, conn io.ReadWriteCloser, opts Options) (*Link, error) {
	return newLink(ctx, conn, opts, false)
}

// Server performs the server handshake; ownership rules are the same as Client.
func Server(ctx context.Context, conn io.ReadWriteCloser, opts Options) (*Link, error) {
	return newLink(ctx, conn, opts, true)
}
func newLink(ctx context.Context, conn io.ReadWriteCloser, opts Options, server bool) (*Link, error) {
	if conn == nil {
		return nil, errors.New("shine: nil connection")
	}
	o, err := opts.defaults()
	if err != nil {
		conn.Close()
		return nil, err
	}
	l := &Link{conn: conn, reader: bufio.NewReader(conn), opts: o, server: server, send: make(chan []byte, 128), incoming: make(chan *Session, o.AcceptQueue), done: make(chan struct{}), sessions: make(map[uint64]*Session), pong: make(chan uint64, 1)}
	if !server && len(o.Users) != 0 {
		conn.Close()
		return nil, errors.New("shine: Users is server-only")
	}
	if server {
		l.authEntries, err = makeAuthEntries(o)
		if err != nil {
			conn.Close()
			return nil, err
		}
	}
	hctx, cancel := context.WithTimeout(ctx, o.HandshakeTimeout)
	defer cancel()
	stop := context.AfterFunc(hctx, func() { conn.Close() })
	err = l.handshake()
	stopped := stop()
	if !stopped && hctx.Err() != nil {
		err = hctx.Err()
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	go l.writer()
	go l.readerLoop()
	go l.heartbeat()
	return l, nil
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(b) {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func (l *Link) handshake() error {
	read := func(want string) (frame, error) {
		// HELLO and authentication happen before the peer is trusted. Bound
		// their total size without imposing a smaller limit on configured secrets.
		limit := 64 << 10
		if want == "AUTH" {
			for _, entry := range l.authEntries {
				if len(entry.password) > limit-len("AUTH") {
					limit = len(entry.password) + len("AUTH")
				}
			}
		}
		if limit > l.opts.MaxFrameSize {
			limit = l.opts.MaxFrameSize
		}
		f, e := readFrame(l.reader, limit)
		if e != nil {
			return f, e
		}
		if f.cmd != want {
			return f, fmt.Errorf("shine: expected %s, got %s", want, f.cmd)
		}
		return f, nil
	}
	hello := control("HELLO", []byte("1"), []byte("mux,flow"))
	if l.server {
		f, e := read("HELLO")
		if e != nil {
			return e
		}
		if len(f.args) < 2 {
			return errors.New("shine: invalid HELLO")
		}
		if e = writeAll(l.conn, hello); e != nil {
			return e
		}
		if len(l.authEntries) != 0 {
			f, e = read("AUTH")
			if e != nil {
				return e
			}
			matched := -1
			if len(f.args) > 0 {
				for i, entry := range l.authEntries {
					if subtle.ConstantTimeCompare(f.args[0], []byte(entry.password)) == 1 {
						matched = i
					}
				}
			}
			if matched < 0 {
				_ = writeAll(l.conn, control("ERR", []byte("AUTH"), []byte("bad password")))
				return errors.New("shine: authentication failed")
			}
			l.identity = l.authEntries[matched].identity
			l.authenticated = true
			return writeAll(l.conn, control("OK"))
		}
		return nil
	}
	if err := writeAll(l.conn, hello); err != nil {
		return err
	}
	f, err := read("HELLO")
	if err != nil {
		return err
	}
	if len(f.args) < 2 {
		return errors.New("shine: invalid HELLO")
	}
	if l.opts.Password != "" {
		if err = writeAll(l.conn, control("AUTH", []byte(l.opts.Password))); err != nil {
			return err
		}
		_, err = read("OK")
		return err
	}
	return nil
}

// AuthenticatedUser reports the locally configured server identity for the
// authenticated password. ok is false for clients and unauthenticated servers.
// The returned identity may be empty when a plain password was configured.
func (l *Link) AuthenticatedUser() (identity Identity, ok bool) {
	return l.identity, l.authenticated
}

// ValidateServerCredentials checks password and identity uniqueness before listening.
func ValidateServerCredentials(o Options) error {
	_, err := makeAuthEntries(o)
	return err
}

func makeAuthEntries(o Options) ([]authEntry, error) {
	if o.Password != "" && len(o.Users) > 0 {
		return nil, errors.New("shine: use either password or users")
	}
	if o.Password != "" {
		return []authEntry{{password: o.Password}}, nil
	}
	entries := make([]authEntry, 0, len(o.Users))
	secrets := make(map[string]struct{})
	emails := make(map[string]struct{})
	for _, u := range o.Users {
		if u.Auth == "" {
			return nil, errors.New("shine: user auth cannot be empty")
		}
		if _, exists := secrets[u.Auth]; exists {
			return nil, errors.New("shine: duplicate user auth")
		}
		secrets[u.Auth] = struct{}{}
		if u.Email != "" {
			if _, exists := emails[u.Email]; exists {
				return nil, errors.New("shine: duplicate user email")
			}
			emails[u.Email] = struct{}{}
		}
		entries = append(entries, authEntry{password: u.Auth, identity: Identity{Email: u.Email, Level: u.Level, User: true}})
	}
	return entries, nil
}

// Done is closed when the link terminates.
func (l *Link) Done() <-chan struct{} { return l.done }

// Err returns the termination cause, or nil while the link is running.
func (l *Link) Err() error { l.mu.Lock(); defer l.mu.Unlock(); return l.err }

// Close closes the transport and all its sessions.
func (l *Link) Close() error { l.fail(io.ErrClosedPipe); return nil }
func (l *Link) fail(err error) {
	l.once.Do(func() {
		l.mu.Lock()
		l.err = err
		all := l.sessions
		l.sessions = make(map[uint64]*Session)
		if l.opts.SessionLimiter != nil {
			l.opts.SessionLimiter.release(len(all))
		}
		close(l.done)
		l.mu.Unlock()
		l.conn.Close()
		for _, s := range all {
			s.abort(err)
		}
	})
}
func (l *Link) enqueue(ctx context.Context, b []byte) error {
	return l.enqueueWithStop(ctx, b, nil)
}

// stop aborts a session-scoped enqueue without closing other sessions.
func (l *Link) enqueueWithStop(ctx context.Context, b []byte, stop <-chan struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-l.done:
		return l.Err()
	case <-stop:
		return io.ErrClosedPipe
	default:
	}
	select {
	case <-l.done:
		return l.Err()
	case <-stop:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	case l.send <- b:
		return nil
	}
}
func (l *Link) writer() {
	for {
		select {
		case <-l.done:
			return
		case b := <-l.send:
			if err := writeAll(l.conn, b); err != nil {
				l.fail(err)
				return
			}
		}
	}
}
func (l *Link) readerLoop() {
	for {
		f, err := readFrame(l.reader, l.opts.MaxFrameSize)
		if err != nil {
			l.fail(err)
			return
		}
		if err = l.dispatch(f); err != nil {
			l.fail(err)
			return
		}
	}
}
func (l *Link) find(id uint64) *Session { l.mu.Lock(); defer l.mu.Unlock(); return l.sessions[id] }
func (l *Link) remove(id uint64, s *Session) {
	l.mu.Lock()
	if l.sessions[id] == s {
		delete(l.sessions, id)
		if l.opts.SessionLimiter != nil {
			l.opts.SessionLimiter.release(1)
		}
	}
	l.mu.Unlock()
}
func (l *Link) add(id uint64, s *Session) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-l.done:
		return l.err
	default:
	}
	if _, ok := l.sessions[id]; ok {
		return errDuplicateSession
	}
	if len(l.sessions) >= l.opts.MaxSessions || (l.opts.SessionLimiter != nil && !l.opts.SessionLimiter.acquire()) {
		return ErrSessionLimit
	}
	l.sessions[id] = s
	return nil
}
func randomID() (uint64, error) {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			return 0, err
		}
		id := binary.BigEndian.Uint64(b[:])
		if id != 0 {
			return id, nil
		}
	}
}

// Open opens a TCP session. Like the C++ implementation, it does not wait for ACK.
func (l *Link) Open(ctx context.Context, target Address) (*Session, error) {
	return l.open(ctx, target, false)
}

// OpenUDP opens a datagram session. Each datagram carries its own destination.
func (l *Link) OpenUDP(ctx context.Context) (*Session, error) { return l.open(ctx, Address{}, true) }
func (l *Link) open(ctx context.Context, a Address, udp bool) (*Session, error) {
	if l.server {
		return nil, errors.New("shine: server cannot initiate sessions")
	}
	var addr []byte
	var err error
	if !udp {
		addr, err = a.marshal()
		if err != nil {
			return nil, err
		}
	}
	var s *Session
	for {
		id, e := randomID()
		if e != nil {
			return nil, e
		}
		s = newSession(l, id, a, udp, l.opts.InitialWindow)
		if err = l.add(id, s); err == nil {
			break
		}
		if !errors.Is(err, errDuplicateSession) {
			return nil, err
		}
	}
	var wire []byte
	if udp {
		wire = control("NEW_UDP", u64(s.id))
	} else {
		wire = control("NEW", u64(s.id), addr, u32(l.opts.InitialWindow))
	}
	if err = l.enqueue(ctx, wire); err != nil {
		l.remove(s.id, s)
		s.abort(err)
		return nil, err
	}
	return s, nil
}

// Accept waits for a peer-created TCP or UDP session. UDP targets are supplied
// on each received datagram, not in the NEW_UDP frame.
func (l *Link) Accept(ctx context.Context) (*Session, error) {
	if !l.server {
		return nil, errors.New("shine: client cannot accept sessions")
	}
	select {
	case <-l.done:
		return nil, l.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	case s := <-l.incoming:
		return s, nil
	}
}
func (l *Link) dispatch(f frame) error {
	switch f.cmd {
	case "NEW", "NEW_UDP":
		if !l.server {
			return errors.New("shine: unexpected NEW")
		}
		n := 1
		if f.cmd == "NEW" {
			n = 3
		}
		id, err := idArg(f, n)
		if err != nil || id == 0 {
			return errors.New("shine: invalid NEW")
		}
		udp := f.cmd == "NEW_UDP"
		var a Address
		win := uint32(0)
		if !udp {
			a, err = parseAddress(f.args[1])
			if err != nil {
				_ = l.enqueue(context.Background(), control("RESET", u64(id)))
				return nil
			}
			if len(f.args[2]) != 4 {
				return errors.New("shine: invalid window")
			}
			win = binary.BigEndian.Uint32(f.args[2])
			if win == 0 {
				return errors.New("shine: zero window")
			}
		}
		s := newSession(l, id, a, udp, win)
		if err = l.add(id, s); err != nil {
			if errors.Is(err, ErrSessionLimit) {
				return l.enqueue(context.Background(), control("RESET", u64(id)))
			}
			return err
		}
		if err = l.enqueue(context.Background(), control("ACK", u64(id))); err != nil {
			return err
		}
		select {
		case l.incoming <- s:
		default:
			l.remove(id, s)
			s.abort(errors.New("shine: accept queue full"))
			return l.enqueue(context.Background(), control("RESET", u64(id)))
		}
		return nil
	case "DATA", "UDP_DATA", "CLOSE", "RESET", "WINDOW", "ACK":
		n := 1
		if f.cmd == "UDP_DATA" {
			n = 3
		}
		if f.cmd == "WINDOW" {
			n = 2
		}
		id, err := idArg(f, n)
		if err != nil {
			return err
		}
		s := l.find(id)
		if s == nil {
			return nil
		}
		switch f.cmd {
		case "DATA":
			if s.udp {
				return errors.New("shine: DATA on UDP session")
			}
			if !s.deliver(f.data) {
				l.remove(id, s)
				s.abort(errors.New("shine: receive window exceeded"))
				return l.enqueue(context.Background(), control("RESET", u64(id)))
			}
		case "UDP_DATA":
			if !s.udp {
				return errors.New("shine: UDP_DATA on TCP session")
			}
			if len(f.args[2]) > 65535 {
				return errors.New("shine: UDP datagram too large")
			}
			a, e := parseAddress(f.args[1])
			if e != nil {
				return e
			}
			s.deliverDatagram(Datagram{Data: f.args[2], Target: a})
		case "CLOSE":
			s.peerClose()
		case "RESET":
			l.remove(id, s)
			s.abort(errors.New("shine: session reset"))
		case "WINDOW":
			if len(f.args[1]) != 4 {
				return errors.New("shine: invalid WINDOW")
			}
			s.addCredit(binary.BigEndian.Uint32(f.args[1]))
		case "ACK":
		}
		return nil
	case "PING", "PONG":
		id, err := idArg(f, 1)
		if err != nil {
			return err
		}
		if f.cmd == "PING" {
			return l.enqueue(context.Background(), control("PONG", u64(id)))
		}
		select {
		case l.pong <- id:
		default:
		}
		return nil
	default:
		return fmt.Errorf("shine: unexpected command %q", f.cmd)
	}
}
func (l *Link) heartbeat() {
	ticker := time.NewTicker(l.opts.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.done:
			return
		case <-ticker.C:
			id, err := randomID()
			if err != nil {
				l.fail(err)
				return
			}
			// Include time spent waiting for space in the send queue. Otherwise a
			// stalled transport can prevent the heartbeat timer from ever starting.
			ctx, cancel := context.WithTimeout(context.Background(), l.opts.PongTimeout)
			if err = l.enqueue(ctx, control("PING", u64(id))); err != nil {
				cancel()
				if errors.Is(err, context.DeadlineExceeded) {
					l.fail(errors.New("shine: heartbeat timeout"))
				}
				return
			}
			waiting := true
			for waiting {
				select {
				case <-l.done:
					cancel()
					return
				case p := <-l.pong:
					if p == id {
						waiting = false
					}
				case <-ctx.Done():
					cancel()
					l.fail(errors.New("shine: heartbeat timeout"))
					return
				}
			}
			cancel()
		}
	}
}
