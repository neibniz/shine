// Package shine implements the Shine multiplexing protocol over an existing
// reliable, ordered, full-duplex connection.
package shine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
)

// Address is a target host and port. Host is an IP literal or a domain name.
type Address struct {
	Host string
	Port uint16
}

// ParseAddress parses a host:port pair, including bracketed IPv6 addresses.
func ParseAddress(s string) (Address, error) {
	h, p, err := net.SplitHostPort(s)
	if err != nil {
		return Address{}, err
	}
	n, err := strconv.ParseUint(p, 10, 16)
	if err != nil || n == 0 {
		return Address{}, errors.New("shine: invalid port")
	}
	a := Address{h, uint16(n)}
	_, err = a.marshal()
	return a, err
}
func (a Address) String() string { return net.JoinHostPort(a.Host, strconv.Itoa(int(a.Port))) }
func (a Address) marshal() ([]byte, error) {
	if a.Host == "" || a.Port == 0 {
		return nil, errors.New("shine: empty host or port")
	}
	var typ byte
	var b []byte
	if ip, err := netip.ParseAddr(a.Host); err == nil {
		if ip.Zone() != "" {
			return nil, errors.New("shine: IPv6 zones are not supported")
		}
		if ip.Is4() {
			typ = 1
			v := ip.As4()
			b = v[:]
		} else {
			typ = 2
			v := ip.As16()
			b = v[:]
		}
	} else {
		typ = 3
		b = []byte(a.Host)
		if len(b) > 255 {
			return nil, errors.New("shine: domain exceeds 255 bytes")
		}
	}
	out := make([]byte, 2+len(b)+2)
	out[0] = typ
	out[1] = byte(len(b))
	copy(out[2:], b)
	binary.BigEndian.PutUint16(out[len(out)-2:], a.Port)
	return out, nil
}
func parseAddress(b []byte) (Address, error) {
	if len(b) < 4 || len(b) != int(b[1])+4 {
		return Address{}, errors.New("shine: invalid address length")
	}
	p := binary.BigEndian.Uint16(b[len(b)-2:])
	if p == 0 {
		return Address{}, errors.New("shine: invalid address port")
	}
	data := b[2 : len(b)-2]
	var host string
	switch b[0] {
	case 1:
		if len(data) != 4 {
			return Address{}, errors.New("shine: invalid IPv4 address")
		}
		var v [4]byte
		copy(v[:], data)
		host = netip.AddrFrom4(v).String()
	case 2:
		if len(data) != 16 {
			return Address{}, errors.New("shine: invalid IPv6 address")
		}
		var v [16]byte
		copy(v[:], data)
		host = netip.AddrFrom16(v).String()
	case 3:
		if len(data) == 0 {
			return Address{}, errors.New("shine: empty domain")
		}
		host = string(data)
	default:
		return Address{}, fmt.Errorf("shine: unknown address type %d", b[0])
	}
	return Address{host, p}, nil
}
