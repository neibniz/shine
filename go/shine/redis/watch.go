package redis

import (
	"context"
	"io"
	"sync"
	"time"
)

// connectionWatch closes a handshaking or simulated-command connection on
// timeout, cancellation or acceptor shutdown. A generation prevents stale
// timer callbacks from closing a connection after its timeout was extended.
type connectionWatch struct {
	mu          sync.Mutex
	conn        io.ReadWriteCloser
	ctx         context.Context
	timer       *time.Timer
	generation  uint64
	stopped     bool
	err         error
	stopContext func() bool
	done        chan struct{}
}

func newConnectionWatch(ctx context.Context, conn io.ReadWriteCloser, d time.Duration) *connectionWatch {
	w := &connectionWatch{conn: conn, ctx: ctx, done: make(chan struct{})}
	w.stopContext = context.AfterFunc(ctx, func() { w.abort(ctx.Err()) })
	w.reset(d)
	return w
}
func (w *connectionWatch) reset(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return
	}
	w.generation++
	gen := w.generation
	if w.timer != nil {
		w.timer.Stop()
	}
	w.timer = time.AfterFunc(d, func() {
		w.mu.Lock()
		if w.stopped || w.generation != gen {
			w.mu.Unlock()
			return
		}
		w.stopped = true
		close(w.done)
		w.err = context.DeadlineExceeded
		w.mu.Unlock()
		w.conn.Close()
	})
}
func (w *connectionWatch) abort(err error) {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	close(w.done)
	w.err = err
	if w.timer != nil {
		w.timer.Stop()
	}
	w.mu.Unlock()
	w.conn.Close()
}

// stop transfers ownership to a paired stream without closing the connection.
func (w *connectionWatch) stop() error {
	w.mu.Lock()
	err := w.err
	if err == nil {
		err = w.ctx.Err()
	}
	if !w.stopped {
		w.stopped = true
		close(w.done)
	}
	if w.timer != nil {
		w.timer.Stop()
	}
	w.mu.Unlock()
	w.stopContext()
	return err
}
func (w *connectionWatch) errorOr(fallback error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if w.ctx.Err() != nil {
		return w.ctx.Err()
	}
	return fallback
}
