package shine

import (
	"context"
	"errors"
	"io"
	"sync"
)

// Datagram is a single UDP payload and its destination or source address.
type Datagram struct {
	Data   []byte
	Target Address
}

// Session is a multiplexed TCP byte stream or UDP datagram association.
// Read and Write are safe concurrently. Read calls and Write calls are each
// serialized. Use CloseWrite to send EOF while continuing to read.
type Session struct {
	link                        *Link
	id                          uint64
	target                      Address
	udp                         bool
	readGate, writeGate         chan struct{}
	mu                          sync.Mutex
	notify                      chan struct{}
	closed, writeClosed         chan struct{}
	buf                         []byte
	credit                      uint64
	consumed                    uint64
	localClosed, remoteClosed   bool
	err                         error
	datagrams                   []Datagram
	datagramHead, datagramBytes int
}

func newSession(l *Link, id uint64, a Address, udp bool, credit uint32) *Session {
	s := &Session{link: l, id: id, target: a, udp: udp, notify: make(chan struct{}), closed: make(chan struct{}), writeClosed: make(chan struct{}), credit: uint64(credit), readGate: make(chan struct{}, 1), writeGate: make(chan struct{}, 1)}
	s.readGate <- struct{}{}
	s.writeGate <- struct{}{}
	return s
}

// lockGate makes waiting for another operation cancellable as well as the I/O
// itself. stop is closed when the relevant side of the session is closed.
func lockGate(ctx context.Context, gate chan struct{}, stop <-chan struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Preserve buffered reads after a link failure when no other reader holds
	// the gate. Only a caller that must wait needs the session-close signal.
	select {
	case <-gate:
		if err := ctx.Err(); err != nil {
			gate <- struct{}{}
			return err
		}
		return nil
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stop:
		return io.ErrClosedPipe
	case <-gate:
		if err := ctx.Err(); err != nil {
			gate <- struct{}{}
			return err
		}
		return nil
	}
}

func unlockGate(gate chan struct{}) { gate <- struct{}{} }

// Preserve a reset or link failure as the cause when closing a gate wakes an
// operation. A locally half-closed write side still reports ErrClosedPipe.
func (s *Session) gateError(err error) error {
	if errors.Is(err, io.ErrClosedPipe) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.err != nil {
			return s.err
		}
	}
	return err
}

// ID is the session identifier on this link.
func (s *Session) ID() uint64 { return s.id }

// Target is the TCP destination. For UDP, the target is carried by each datagram.
func (s *Session) Target() Address { return s.target }

// IsUDP reports whether this session carries datagrams.
func (s *Session) IsUDP() bool { return s.udp }
func (s *Session) wake()       { close(s.notify); s.notify = make(chan struct{}) } // caller holds mu
func (s *Session) abort(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
		if !s.localClosed {
			s.localClosed = true
			close(s.writeClosed)
		}
		s.remoteClosed = true
		close(s.closed)
		s.datagrams = nil
		s.datagramHead, s.datagramBytes = 0, 0
		s.wake()
	}
	s.mu.Unlock()
}
func (s *Session) peerClose() {
	s.mu.Lock()
	s.remoteClosed = true
	s.wake()
	both := s.localClosed
	s.mu.Unlock()
	if both {
		s.link.remove(s.id, s)
	}
}
func (s *Session) addCredit(n uint32) { s.mu.Lock(); s.credit += uint64(n); s.wake(); s.mu.Unlock() }
func (s *Session) deliver(b []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil || s.remoteClosed {
		return true
	}
	if uint64(len(s.buf))+uint64(len(b)) > uint64(s.link.opts.InitialWindow) {
		return false
	}
	s.buf = append(s.buf, b...)
	s.wake()
	return true
}
func (s *Session) deliverDatagram(d Datagram) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil || s.remoteClosed {
		return
	}
	// Include per-packet overhead so empty packets are bounded as well.
	cost := len(d.Data) + len(d.Target.Host) + 64
	if len(s.datagrams)-s.datagramHead >= s.link.opts.DatagramQueue || cost > s.link.opts.DatagramQueueBytes-s.datagramBytes {
		return
	}
	if s.datagramHead > 0 && len(s.datagrams) == cap(s.datagrams) {
		count := copy(s.datagrams, s.datagrams[s.datagramHead:])
		clear(s.datagrams[count:])
		s.datagrams = s.datagrams[:count]
		s.datagramHead = 0
	}
	s.datagrams = append(s.datagrams, d)
	s.datagramBytes += cost
	s.wake()
} // UDP is lossy under pressure

// flushWindow preserves credit when cancellation interrupts a blocked enqueue.
// The next read retries it before waiting for more data.
func (s *Session) flushWindow(ctx context.Context) error {
	s.mu.Lock()
	if s.err != nil {
		// No further credit is useful after a hard close; buffered TCP data can
		// still be delivered before reporting the close reason to the reader.
		s.mu.Unlock()
		return nil
	}
	threshold := uint64(s.link.opts.InitialWindow / 2)
	if threshold == 0 {
		threshold = 1
	}
	if s.consumed < threshold {
		s.mu.Unlock()
		return nil
	}
	delta := s.consumed
	if delta > uint64(^uint32(0)) {
		delta = uint64(^uint32(0))
	}
	s.consumed -= delta
	s.mu.Unlock()
	if err := s.link.enqueueWithStop(ctx, control("WINDOW", u64(s.id), u32(uint32(delta))), s.closed); err != nil {
		s.mu.Lock()
		s.consumed += delta
		s.mu.Unlock()
		return s.gateError(err)
	}
	return nil
}

// Read reads TCP bytes, returning EOF after the peer closes its write side.
func (s *Session) Read(p []byte) (int, error) { return s.ReadContext(context.Background(), p) }

// ReadContext is Read with cancellation; cancellation does not close the session.
func (s *Session) ReadContext(ctx context.Context, p []byte) (int, error) {
	if s.udp {
		return 0, errors.New("shine: Read on UDP session")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := lockGate(ctx, s.readGate, s.closed); err != nil {
		return 0, s.gateError(err)
	}
	defer unlockGate(s.readGate)
	if err := s.flushWindow(ctx); err != nil {
		return 0, err
	}
	for {
		s.mu.Lock()
		if len(s.buf) > 0 {
			n := copy(p, s.buf)
			s.buf = s.buf[n:]
			if len(s.buf) == 0 {
				s.buf = nil
			}
			s.consumed += uint64(n)
			s.mu.Unlock()
			if err := s.flushWindow(ctx); err != nil {
				return n, err
			}
			return n, nil
		}
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return 0, err
		}
		if s.remoteClosed {
			s.mu.Unlock()
			return 0, io.EOF
		}
		notify := s.notify
		s.mu.Unlock()
		select {
		case <-notify:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-s.link.done:
			return 0, s.link.Err()
		}
	}
}

// Write writes TCP bytes, waiting for session-level flow-control credit.
func (s *Session) Write(p []byte) (int, error) { return s.WriteContext(context.Background(), p) }

// WriteContext is Write with cancellation; cancellation does not close the session.
func (s *Session) WriteContext(ctx context.Context, p []byte) (int, error) {
	if s.udp {
		return 0, errors.New("shine: Write on UDP session")
	}
	if err := lockGate(ctx, s.writeGate, s.writeClosed); err != nil {
		return 0, s.gateError(err)
	}
	defer unlockGate(s.writeGate)
	written := 0
	for len(p) > 0 {
		s.mu.Lock()
		if s.err != nil {
			e := s.err
			s.mu.Unlock()
			return written, e
		}
		if s.localClosed {
			s.mu.Unlock()
			return written, io.ErrClosedPipe
		}
		if s.credit == 0 {
			notify := s.notify
			s.mu.Unlock()
			select {
			case <-notify:
				continue
			case <-ctx.Done():
				return written, ctx.Err()
			case <-s.link.done:
				return written, s.link.Err()
			}
		}
		n := len(p)
		if n > s.link.opts.MaxDataSize {
			n = s.link.opts.MaxDataSize
		}
		if uint64(n) > s.credit {
			n = int(s.credit)
		}
		s.credit -= uint64(n)
		s.mu.Unlock()
		if err := s.link.enqueueWithStop(ctx, dataFrame(s.id, p[:n]), s.writeClosed); err != nil {
			s.mu.Lock()
			s.credit += uint64(n)
			s.mu.Unlock()
			return written, s.gateError(err)
		}
		written += n
		p = p[n:]
	}
	return written, nil
}

// CloseWrite signals EOF to the peer. The session remains readable until peer EOF.
func (s *Session) CloseWrite() error {
	s.mu.Lock()
	if s.localClosed {
		s.mu.Unlock()
		return nil
	}
	s.localClosed = true
	close(s.writeClosed)
	s.wake()
	both := s.remoteClosed
	s.mu.Unlock()
	// A pending writer must finish (or observe localClosed) before CLOSE is sent.
	_ = lockGate(context.Background(), s.writeGate, nil)
	defer unlockGate(s.writeGate)
	err := s.link.enqueue(context.Background(), control("CLOSE", u64(s.id), nil))
	if both {
		s.link.remove(s.id, s)
	}
	return err
}

// Close releases the session and wakes blocked operations.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return nil
	}
	send := !s.localClosed
	s.err = io.ErrClosedPipe
	if !s.localClosed {
		s.localClosed = true
		close(s.writeClosed)
	}
	s.remoteClosed = true
	close(s.closed)
	s.datagrams = nil
	s.datagramHead, s.datagramBytes = 0, 0
	s.wake()
	s.mu.Unlock()
	s.link.remove(s.id, s)
	if send {
		_ = lockGate(context.Background(), s.writeGate, nil)
		defer unlockGate(s.writeGate)
		return s.link.enqueue(context.Background(), control("CLOSE", u64(s.id), nil))
	}
	return nil
}

// SendDatagram sends one UDP payload to target.
func (s *Session) SendDatagram(ctx context.Context, data []byte, target Address) error {
	if !s.udp {
		return errors.New("shine: datagram on TCP session")
	}
	addr, err := target.marshal()
	if err != nil {
		return err
	}
	if len(data) > 65535 || len(data)+len(addr)+len("UDP_DATA")+8 > s.link.opts.MaxFrameSize {
		return errors.New("shine: datagram exceeds frame limit")
	}
	if err := lockGate(ctx, s.writeGate, s.writeClosed); err != nil {
		return s.gateError(err)
	}
	defer unlockGate(s.writeGate)
	s.mu.Lock()
	closed := s.localClosed
	err = s.err
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if closed {
		return io.ErrClosedPipe
	}
	return s.gateError(s.link.enqueueWithStop(ctx, control("UDP_DATA", u64(s.id), addr, data), s.writeClosed))
}

// ReceiveDatagram receives one UDP datagram. A full receive queue drops packets.
func (s *Session) ReceiveDatagram(ctx context.Context) (Datagram, error) {
	if !s.udp {
		return Datagram{}, errors.New("shine: datagram on TCP session")
	}
	if err := lockGate(ctx, s.readGate, s.closed); err != nil {
		return Datagram{}, s.gateError(err)
	}
	defer unlockGate(s.readGate)
	for {
		s.mu.Lock()
		if s.datagramHead < len(s.datagrams) {
			d := s.datagrams[s.datagramHead]
			s.datagrams[s.datagramHead] = Datagram{}
			s.datagramHead++
			s.datagramBytes -= len(d.Data) + len(d.Target.Host) + 64
			if s.datagramHead == len(s.datagrams) {
				s.datagrams = s.datagrams[:0]
				s.datagramHead = 0
			}
			s.mu.Unlock()
			return d, nil
		}
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return Datagram{}, err
		}
		if s.remoteClosed {
			s.mu.Unlock()
			return Datagram{}, io.EOF
		}
		notify := s.notify
		s.mu.Unlock()
		select {
		case <-notify:
		case <-ctx.Done():
			return Datagram{}, ctx.Err()
		case <-s.link.done:
			return Datagram{}, s.link.Err()
		}
	}
}
