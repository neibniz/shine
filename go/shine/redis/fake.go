package redis

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type fakeSession struct {
	authenticated bool
	secret        string
	name          string
	db            int
	id            uint64
}

func status(s string) []byte    { return []byte("+" + s + "\r\n") }
func integer(n int64) []byte    { return []byte(":" + strconv.FormatInt(n, 10) + "\r\n") }
func bulkReply(p []byte) []byte { var b bytes.Buffer; bulk(&b, p); return b.Bytes() }
func wrongArgs(name string) []byte {
	return []byte("-ERR wrong number of arguments for '" + strings.ToLower(name) + "' command\r\n")
}
func errorReply(s string) []byte       { return []byte("-ERR " + s + "\r\n") }
func parseInt(p []byte) (int64, error) { return strconv.ParseInt(string(p), 10, 64) }
func duration(n int64, mult time.Duration) (time.Duration, bool) {
	if n > int64((1<<63-1)/int64(mult)) || n < (-1<<63)/int64(mult) {
		return 0, false
	}
	return time.Duration(n) * mult, true
}
func asBytes(v value) [][]byte {
	out := make([][]byte, len(v.array))
	for i := range v.array {
		out[i] = v.array[i].text
	}
	return out
}

// fakeReply implements an intentionally limited Redis-compatible subset.
// handled=false leaves SUBSCRIBE/PUBLISH to the carrier state machine.
func (a *Acceptor) fakeReply(f value, state *fakeSession) (reply []byte, handled bool, quit bool) {
	if !arrayStrings(f, len(f.array)) {
		return errorReply("Protocol error: expected bulk arguments"), true, false
	}
	parts := asBytes(f)
	name := strings.ToUpper(string(parts[0]))
	n := len(parts)
	if name == "AUTH" {
		if n != 2 && n != 3 {
			return wrongArgs(name), true, false
		}
		if !a.requireAuth {
			return errorReply("AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?"), true, false
		}
		if n == 3 && string(parts[1]) != "default" {
			return []byte("-WRONGPASS invalid username-password pair or user is disabled.\r\n"), true, false
		}
		secret := string(parts[n-1])
		if !a.authenticate(secret) {
			return []byte("-WRONGPASS invalid username-password pair or user is disabled.\r\n"), true, false
		}
		state.authenticated = true
		state.secret = secret
		return status("OK"), true, false
	}
	if name == "HELLO" {
		return a.hello(parts, state), true, false
	}
	if !state.authenticated {
		return []byte("-NOAUTH Authentication required.\r\n"), true, false
	}
	switch name {
	case "PING":
		if n == 1 {
			return status("PONG"), true, false
		}
		if n == 2 {
			return bulkReply(parts[1]), true, false
		}
		return wrongArgs(name), true, false
	case "ECHO":
		if n != 2 {
			return wrongArgs(name), true, false
		}
		return bulkReply(parts[1]), true, false
	case "QUIT":
		if n != 1 {
			return wrongArgs(name), true, false
		}
		return status("OK"), true, true
	case "SELECT":
		if n != 2 {
			return wrongArgs(name), true, false
		}
		db, e := parseInt(parts[1])
		if e != nil {
			return errorReply("invalid DB index"), true, false
		}
		if db < 0 || db >= 16 {
			return errorReply("DB index is out of range"), true, false
		}
		state.db = int(db)
		return status("OK"), true, false
	case "CLIENT":
		return fakeClient(parts, state), true, false
	case "INFO":
		if n > 2 {
			return wrongArgs(name), true, false
		}
		section := "default"
		if n == 2 {
			section = strings.ToLower(string(parts[1]))
		}
		return bulkReply(a.info(section, state.db)), true, false
	case "GET":
		if n != 2 {
			return wrongArgs(name), true, false
		}
		v, ok := a.store.get(state.db, parts[1])
		if !ok {
			return []byte("$-1\r\n"), true, false
		}
		return bulkReply(v), true, false
	case "SET":
		return a.setCommand(parts, state.db), true, false
	case "DEL", "EXISTS":
		if n < 2 {
			return wrongArgs(name), true, false
		}
		v := 0
		if name == "DEL" {
			v = a.store.del(state.db, parts[1:])
		} else {
			v = a.store.exists(state.db, parts[1:])
		}
		return integer(int64(v)), true, false
	case "TYPE":
		if n != 2 {
			return wrongArgs(name), true, false
		}
		_, ok := a.store.get(state.db, parts[1])
		if ok {
			return status("string"), true, false
		}
		return status("none"), true, false
	case "DBSIZE":
		if n != 1 {
			return wrongArgs(name), true, false
		}
		return integer(int64(a.store.count(state.db))), true, false
	case "MGET":
		if n < 2 {
			return wrongArgs(name), true, false
		}
		var b bytes.Buffer
		fmt.Fprintf(&b, "*%d\r\n", n-1)
		values, found, tooLarge := a.store.getMany(state.db, parts[1:])
		if tooLarge {
			return errorReply("simulated response exceeds limit"), true, false
		}
		for i, v := range values {
			if found[i] {
				bulk(&b, v)
			} else {
				b.WriteString("$-1\r\n")
			}
		}
		return b.Bytes(), true, false
	case "EXPIRE", "PEXPIRE":
		if n != 3 {
			return wrongArgs(name), true, false
		}
		num, e := parseInt(parts[2])
		if e != nil {
			return errorReply("value is not an integer or out of range"), true, false
		}
		unit := time.Second
		if name == "PEXPIRE" {
			unit = time.Millisecond
		}
		d, ok := duration(num, unit)
		if !ok {
			return errorReply("invalid expire time"), true, false
		}
		if a.store.expire(state.db, parts[1], d) {
			return integer(1), true, false
		}
		return integer(0), true, false
	case "TTL", "PTTL":
		if n != 2 {
			return wrongArgs(name), true, false
		}
		return integer(a.store.ttl(state.db, parts[1], name == "PTTL")), true, false
	case "SUBSCRIBE", "PUBLISH":
		return nil, false, false
	default:
		return errorReply("unknown command '" + safeName(name) + "'"), true, false
	}
}
func safeName(name string) string {
	if len(name) > 64 {
		name = name[:64]
	}
	var b strings.Builder
	for _, c := range name {
		if c >= 33 && c <= 126 && c != '\'' && c != '\\' {
			b.WriteRune(c)
		} else {
			b.WriteByte('?')
		}
	}
	return b.String()
}
func (a *Acceptor) setCommand(p [][]byte, db int) []byte {
	if len(p) < 3 {
		return wrongArgs("set")
	}
	mode := ""
	var expires time.Time
	expirySet := false
	for i := 3; i < len(p); i++ {
		switch strings.ToUpper(string(p[i])) {
		case "NX", "XX":
			if mode != "" {
				return errorReply("syntax error")
			}
			mode = strings.ToUpper(string(p[i]))
		case "EX", "PX":
			if expirySet || i+1 >= len(p) {
				return errorReply("syntax error")
			}
			unit := time.Second
			if strings.ToUpper(string(p[i])) == "PX" {
				unit = time.Millisecond
			}
			i++
			n, e := parseInt(p[i])
			if e != nil {
				return errorReply("value is not an integer or out of range")
			}
			d, ok := duration(n, unit)
			if n <= 0 || !ok {
				return errorReply("invalid expire time in 'set' command")
			}
			expires = time.Now().Add(d)
			expirySet = true
		default:
			return errorReply("syntax error")
		}
	}
	applied, full := a.store.set(db, p[1], p[2], mode, expires)
	if full {
		return []byte("-OOM command not allowed when used memory > 'maxmemory'.\r\n")
	}
	if !applied {
		return []byte("$-1\r\n")
	}
	return status("OK")
}
func fakeClient(p [][]byte, state *fakeSession) []byte {
	if len(p) < 2 {
		return wrongArgs("client")
	}
	sub := strings.ToUpper(string(p[1]))
	switch sub {
	case "ID":
		if len(p) != 2 {
			return wrongArgs("client|id")
		}
		return integer(int64(state.id))
	case "GETNAME":
		if len(p) != 2 {
			return wrongArgs("client|getname")
		}
		if state.name == "" {
			return []byte("$-1\r\n")
		}
		return bulkReply([]byte(state.name))
	case "SETNAME":
		if len(p) != 3 {
			return wrongArgs("client|setname")
		}
		for _, c := range p[2] {
			if c <= 32 || c >= 127 {
				return errorReply("Client names cannot contain spaces, newlines or special characters.")
			}
		}
		state.name = string(p[2])
		return status("OK")
	case "SETINFO":
		if len(p) != 4 {
			return wrongArgs("client|setinfo")
		}
		field := strings.ToUpper(string(p[2]))
		if field != "LIB-NAME" && field != "LIB-VER" {
			return errorReply("syntax error")
		}
		return status("OK")
	default:
		return errorReply("unknown subcommand '" + safeName(sub) + "'. Try CLIENT HELP.")
	}
}
func (a *Acceptor) hello(p [][]byte, state *fakeSession) []byte {
	if len(p) > 1 && string(p[1]) != "2" {
		return []byte("-NOPROTO unsupported protocol version\r\n")
	}
	for i := 2; i < len(p); i++ {
		switch strings.ToUpper(string(p[i])) {
		case "AUTH":
			if i+2 >= len(p) {
				return errorReply("Syntax error in HELLO option 'AUTH'")
			}
			if !a.requireAuth {
				return errorReply("AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
			}
			if string(p[i+1]) != "default" || !a.authenticate(string(p[i+2])) {
				return []byte("-WRONGPASS invalid username-password pair or user is disabled.\r\n")
			}
			state.authenticated = true
			state.secret = string(p[i+2])
			i += 2
		case "SETNAME":
			if i+1 >= len(p) {
				return errorReply("Syntax error in HELLO option 'SETNAME'")
			}
			reply := fakeClient([][]byte{[]byte("CLIENT"), []byte("SETNAME"), p[i+1]}, state)
			if !bytes.Equal(reply, status("OK")) {
				return reply
			}
			i++
		default:
			return errorReply("Syntax error in HELLO option '" + safeName(strings.ToUpper(string(p[i]))) + "'")
		}
	}
	if !state.authenticated {
		return []byte("-NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time\r\n")
	}
	var b bytes.Buffer
	b.WriteString("*14\r\n")
	for _, pair := range [][2]string{{"server", "redis"}, {"version", "7.2.10"}} {
		bulk(&b, []byte(pair[0]))
		bulk(&b, []byte(pair[1]))
	}
	bulk(&b, []byte("proto"))
	b.WriteString(":2\r\n")
	bulk(&b, []byte("id"))
	fmt.Fprintf(&b, ":%d\r\n", state.id)
	for _, pair := range [][2]string{{"mode", "standalone"}, {"role", "master"}} {
		bulk(&b, []byte(pair[0]))
		bulk(&b, []byte(pair[1]))
	}
	bulk(&b, []byte("modules"))
	b.WriteString("*0\r\n")
	return b.Bytes()
}
func (a *Acceptor) info(section string, _ int) []byte {
	uptime := int64(time.Since(a.started).Seconds())
	var b strings.Builder
	if section == "default" || section == "all" || section == "server" {
		fmt.Fprintf(&b, "# Server\r\nredis_version:7.2.10\r\nredis_mode:standalone\r\nuptime_in_seconds:%d\r\n\r\n", uptime)
	}
	if section == "default" || section == "all" || section == "replication" {
		b.WriteString("# Replication\r\nrole:master\r\nconnected_slaves:0\r\n\r\n")
	}
	if section == "default" || section == "all" || section == "keyspace" {
		b.WriteString("# Keyspace\r\n")
		for db, info := range a.store.stats() {
			if info.keys == 0 {
				continue
			}
			var avg int64
			if info.expires > 0 {
				avg = info.ttlMillis / int64(info.expires)
			}
			fmt.Fprintf(&b, "db%d:keys=%d,expires=%d,avg_ttl=%d\r\n", db, info.keys, info.expires, avg)
		}
		b.WriteString("\r\n")
	}
	return []byte(b.String())
}
