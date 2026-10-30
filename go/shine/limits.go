package shine

import (
	"errors"
	"sync/atomic"
)

const (
	// DefaultMaxSessions is the maximum number of active sessions per link.
	DefaultMaxSessions = 256
	// DefaultDatagramQueueBytes bounds the accounted size of a UDP receive queue.
	DefaultDatagramQueueBytes = 256 << 10
	maxSessionLimit           = 1 << 20
	maxDatagramQueueBytes     = 64 << 20
)

// ErrSessionLimit means a link or its shared limiter is at capacity.
var ErrSessionLimit = errors.New("shine: session limit reached")

var errDuplicateSession = errors.New("shine: duplicate session ID")

// SessionLimiter bounds the total active sessions across multiple links.
// Share one limiter between server links that belong to the same service.
type SessionLimiter struct {
	limit  int64
	active atomic.Int64
}

// NewSessionLimiter creates a shared limit. limit must be positive.
func NewSessionLimiter(limit int) (*SessionLimiter, error) {
	if limit < 1 || limit > maxSessionLimit {
		return nil, errors.New("shine: invalid shared session limit")
	}
	return &SessionLimiter{limit: int64(limit)}, nil
}

func (l *SessionLimiter) acquire() bool {
	for {
		active := l.active.Load()
		if active >= l.limit {
			return false
		}
		if l.active.CompareAndSwap(active, active+1) {
			return true
		}
	}
}

func (l *SessionLimiter) release(n int) {
	if n != 0 {
		l.active.Add(-int64(n))
	}
}
