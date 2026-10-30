package redis

import (
	"bufio"
	"runtime"
	"strings"
	"testing"
)

func TestIncompleteBulkDoesNotPreallocate(t *testing.T) {
	// A simulated-command connection can send this before authenticating.
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := readValue(bufio.NewReader(strings.NewReader("*1\r\n$2097152\r\n")), DefaultMaxPayload+1024, 0)
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Fatal("accepted a missing bulk body")
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("declared-only bulk allocated %d bytes", allocated)
	}
}
