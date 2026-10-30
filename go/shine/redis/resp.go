package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

type value struct {
	kind   byte
	text   []byte
	number int64
	array  []value
	null   bool
}

func line(r *bufio.Reader) ([]byte, error) {
	b, e := r.ReadSlice('\n')
	if e != nil {
		return nil, e
	}
	if len(b) > 4096 || len(b) < 2 || b[len(b)-2] != '\r' {
		return nil, errors.New("redis transport: invalid line")
	}
	return b[:len(b)-2], nil
}
func number(r *bufio.Reader) (int64, error) {
	b, e := line(r)
	if e != nil {
		return 0, e
	}
	v, e := strconv.ParseInt(string(b), 10, 64)
	if e != nil {
		return 0, errors.New("redis transport: invalid integer")
	}
	return v, nil
}
func readValue(r *bufio.Reader, max int, depth int) (value, error) {
	if max < 0 {
		return value{}, errors.New("redis transport: frame too large")
	}
	nodes := 2048
	return readValueBudget(r, &max, &nodes, depth)
}
func readValueBudget(r *bufio.Reader, remaining *int, nodes *int, depth int) (value, error) {
	if depth > 2 || *nodes <= 0 {
		return value{}, errors.New("redis transport: RESP nesting or element limit")
	}
	*nodes--
	kind, err := r.ReadByte()
	if err != nil {
		return value{}, err
	}
	v := value{kind: kind}
	switch kind {
	case '+', '-':
		v.text, err = line(r)
		if err != nil {
			return value{}, err
		}
		if len(v.text) > *remaining {
			return value{}, errors.New("redis transport: frame too large")
		}
		*remaining -= len(v.text)
		return v, nil
	case ':':
		v.number, err = number(r)
		return v, err
	case '$':
		n, e := number(r)
		if e != nil {
			return v, e
		}
		if n == -1 {
			v.null = true
			return v, nil
		}
		if n < 0 || n > int64(*remaining) {
			return v, errors.New("redis transport: bulk length out of range")
		}
		*remaining -= int(n)
		b := make([]byte, int(n)+2)
		if _, e = io.ReadFull(r, b); e != nil {
			return v, e
		}
		if b[n] != '\r' || b[n+1] != '\n' {
			return v, errors.New("redis transport: invalid bulk terminator")
		}
		v.text = b[:n]
		return v, nil
	case '*':
		n, e := number(r)
		if e != nil {
			return v, e
		}
		if n == -1 {
			v.null = true
			return v, nil
		}
		if n < 0 || n > 1024 || n > int64(*nodes) {
			return v, errors.New("redis transport: array length out of range")
		}
		v.array = make([]value, int(n))
		for i := range v.array {
			v.array[i], e = readValueBudget(r, remaining, nodes, depth+1)
			if e != nil {
				return v, e
			}
		}
		return v, nil
	default:
		return v, errors.New("redis transport: invalid RESP type")
	}
}
func bulk(b *bytes.Buffer, p []byte) {
	fmt.Fprintf(b, "$%d\r\n", len(p))
	b.Write(p)
	b.WriteString("\r\n")
}
func command(parts ...[]byte) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(parts))
	for _, p := range parts {
		bulk(&b, p)
	}
	return b.Bytes()
}
func push(kind string, channel string, payload []byte) []byte {
	return command([]byte(kind), []byte(channel), payload)
}
func subAck(channel string) []byte {
	var b bytes.Buffer
	b.WriteString("*3\r\n")
	bulk(&b, []byte("subscribe"))
	bulk(&b, []byte(channel))
	b.WriteString(":1\r\n")
	return b.Bytes()
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, e := w.Write(b)
		if e != nil {
			return e
		}
		if n <= 0 || n > len(b) {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func arrayStrings(v value, n int) bool {
	if v.kind != '*' || len(v.array) != n {
		return false
	}
	for _, x := range v.array {
		if x.kind != '$' || x.null {
			return false
		}
	}
	return true
}
