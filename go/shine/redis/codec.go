package redis

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"
)

func encodePayload(p []byte, compress bool, max int) ([]byte, error) {
	if len(p) > max {
		return nil, errors.New("redis transport: payload too large")
	}
	if compress && len(p) >= 128 {
		var b bytes.Buffer
		b.WriteByte(1)
		z := zlib.NewWriter(&b)
		if _, err := z.Write(p); err != nil {
			return nil, err
		}
		if err := z.Close(); err != nil {
			return nil, err
		}
		if b.Len()+16 <= len(p)+1 {
			return b.Bytes(), nil
		}
	}
	out := make([]byte, len(p)+1)
	copy(out[1:], p)
	return out, nil
}
func decodePayload(p []byte, max int) ([]byte, error) {
	if len(p) < 1 {
		return nil, errors.New("redis transport: empty envelope")
	}
	if p[0] == 0 {
		if len(p)-1 > max {
			return nil, errors.New("redis transport: payload too large")
		}
		return p[1:], nil
	}
	if p[0] != 1 {
		return nil, errors.New("redis transport: unknown envelope")
	}
	z, err := zlib.NewReader(bytes.NewReader(p[1:]))
	if err != nil {
		return nil, err
	}
	out, err := io.ReadAll(io.LimitReader(z, int64(max)+1))
	closeErr := z.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(out) > max {
		return nil, errors.New("redis transport: decompressed payload too large")
	}
	return out, nil
}
