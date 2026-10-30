package shine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const maxBulk = 512 << 20
const dataMagic = 0xd1

type frame struct {
	cmd  string
	args [][]byte
	data []byte
}

func u64(v uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }
func u32(v uint32) []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, v); return b }
func control(cmd string, args ...[]byte) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args)+1)
	for _, p := range append([][]byte{[]byte(cmd)}, args...) {
		fmt.Fprintf(&b, "$%d\r\n", len(p))
		b.Write(p)
		b.WriteString("\r\n")
	}
	return b.Bytes()
}
func dataFrame(id uint64, p []byte) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "$%d\r\n", len(p)+9)
	b.WriteByte(dataMagic)
	b.Write(u64(id))
	b.Write(p)
	b.WriteString("\r\n")
	return b.Bytes()
}
func readNumber(r *bufio.Reader) (int, error) {
	// Limit the header independently of the buffered reader to avoid unbounded input.
	line, err := r.ReadSlice('\n')
	if err != nil {
		return 0, err
	}
	if len(line) > 22 || len(line) < 3 || line[len(line)-2] != '\r' {
		return 0, errors.New("shine: invalid RESP length")
	}
	for _, c := range line[:len(line)-2] {
		if c < '0' || c > '9' {
			return 0, errors.New("shine: invalid RESP length")
		}
	}
	n, err := strconv.ParseUint(string(line[:len(line)-2]), 10, 31)
	if err != nil {
		return 0, errors.New("shine: invalid RESP length")
	}
	return int(n), nil
}
func readBulk(r *bufio.Reader, limit int) ([]byte, error) {
	marker, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if marker != '$' {
		return nil, errors.New("shine: expected bulk string")
	}
	n, err := readNumber(r)
	if err != nil {
		return nil, err
	}
	if n > limit {
		return nil, errors.New("shine: frame exceeds limit")
	}
	b := make([]byte, n+2)
	if _, err = io.ReadFull(r, b); err != nil {
		return nil, err
	}
	if b[n] != '\r' || b[n+1] != '\n' {
		return nil, errors.New("shine: invalid bulk terminator")
	}
	return b[:n], nil
}
func readFrame(r *bufio.Reader, limit int) (frame, error) {
	t, err := r.ReadByte()
	if err != nil {
		return frame{}, err
	}
	if t == '$' {
		if err = r.UnreadByte(); err != nil {
			return frame{}, err
		}
		b, e := readBulk(r, limit)
		if e != nil {
			return frame{}, e
		}
		if len(b) < 9 || b[0] != dataMagic {
			return frame{}, errors.New("shine: invalid DATA frame")
		}
		return frame{cmd: "DATA", args: [][]byte{b[1:9]}, data: b[9:]}, nil
	}
	if t != '*' {
		return frame{}, errors.New("shine: invalid RESP frame type")
	}
	n, err := readNumber(r)
	if err != nil {
		return frame{}, err
	}
	if n < 1 || n > 1024 {
		return frame{}, errors.New("shine: invalid array length")
	}
	parts := make([][]byte, n)
	remaining := limit
	for i := range parts {
		parts[i], err = readBulk(r, remaining)
		if err != nil {
			return frame{}, err
		}
		remaining -= len(parts[i])
	}
	return frame{cmd: string(parts[0]), args: parts[1:]}, nil
}
func idArg(f frame, n int) (uint64, error) {
	if len(f.args) < n || len(f.args[0]) != 8 {
		return 0, fmt.Errorf("shine: invalid %s frame", f.cmd)
	}
	return binary.BigEndian.Uint64(f.args[0]), nil
}
