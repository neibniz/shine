// Package bounded reads peer-declared payloads without allocating their full
// declared size before the bytes arrive.
package bounded

import "io"

// Read consumes exactly n bytes, allocating at most 4 KiB before receiving
// data and growing only after each previously allocated portion is filled.
// Callers must validate n against their protocol's maximum first.
func Read(r io.Reader, n int) ([]byte, error) {
	initial := n
	if initial > 4096 {
		initial = 4096
	}
	body := make([]byte, 0, initial)
	for len(body) < n {
		if len(body) == cap(body) {
			next := cap(body) * 2
			if next > n {
				next = n
			}
			grown := make([]byte, len(body), next)
			copy(grown, body)
			body = grown
		}
		offset := len(body)
		body = body[:cap(body)]
		if _, err := io.ReadFull(r, body[offset:]); err != nil {
			return nil, err
		}
	}
	return body, nil
}
