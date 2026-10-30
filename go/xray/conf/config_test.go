package conf

import (
	"encoding/json"
	"testing"

	adapter "github.com/hhhflow2020/shine/go/xray"
	"github.com/xtls/xray-core/common/serial"
)

func TestConfigJSONAndProtoRegistration(t *testing.T) {
	var c OutboundConfig
	if err := json.Unmarshal([]byte(`{"address":"example.com","port":443,"users":[{"auth":"secret","email":"alice@example.com","level":2}],"initialWindow":65536}`), &c); err != nil {
		t.Fatal(err)
	}
	m, e := c.Build()
	if e != nil {
		t.Fatal(e)
	}
	got, e := serial.ToTypedMessage(m).GetInstance()
	if e != nil {
		t.Fatal(e)
	}
	p := got.(*adapter.ClientConfig)
	if p.Address != "example.com" || p.Port != 443 || p.InitialWindow != 65536 || len(p.Users) != 1 || p.Users[0].Auth != "secret" || p.Users[0].Email != "alice@example.com" || p.Users[0].Level != 2 {
		t.Fatalf("wrong config: %+v", p)
	}
	pw, e := adapter.ResolveClientPassword(p)
	if e != nil || pw != "secret" {
		t.Fatalf("credential %q: %v", pw, e)
	}
}
func TestCredentialConfig(t *testing.T) {
	serverCases := []struct {
		name, config string
		wantErr      bool
	}{
		{"legacy", `{"password":"one"}`, false},
		{"users", `{"users":[{"auth":"one","email":"a@example.com","level":2},{"auth":"two","email":"b@example.com"}]}`, false},
		{"email optional", `{"users":[{"auth":"one"}]}`, false},
		{"empty users", `{"users":[]}`, true},
		{"null users", `{"users":null}`, true},
		{"missing auth", `{"users":[{"email":"a@example.com"}]}`, true},
		{"auth array", `{"users":[{"auth":["one","two"]}]}`, true},
		{"duplicate auth", `{"users":[{"auth":"same","email":"a"},{"auth":"same","email":"b"}]}`, true},
		{"duplicate email", `{"users":[{"email":"a","auth":"one"},{"email":"a","auth":"two"}]}`, true},
		{"unsupported password field", `{"users":[{"password":"one","email":"a"}]}`, true},
		{"unsupported user id", `{"users":[{"user_id":"id","auth":"one"}]}`, true},
		{"unsupported password array", `{"passwords":["one","two"]}`, true},
		{"mixed credentials", `{"password":"one","users":[{"auth":"two"}]}`, true},
	}
	for _, tc := range serverCases {
		t.Run(tc.name, func(t *testing.T) {
			var c InboundConfig
			e := json.Unmarshal([]byte(tc.config), &c)
			if e == nil {
				_, e = c.Build()
			}
			if (e != nil) != tc.wantErr {
				t.Fatalf("error = %v", e)
			}
		})
	}
	clientCases := []struct {
		name, config, password string
		wantErr                bool
	}{
		{"legacy", `{"address":"host","port":443,"password":"a"}`, "a", false},
		{"single user", `{"address":"host","port":443,"users":[{"auth":"a","email":"a@example.com"}]}`, "a", false},
		{"multiple users", `{"address":"host","port":443,"users":[{"auth":"a"},{"auth":"b"}]}`, "", true},
		{"empty auth", `{"address":"host","port":443,"users":[{"auth":""}]}`, "", true},
		{"mixed credentials", `{"address":"host","port":443,"password":"a","users":[{"auth":"b"}]}`, "", true},
	}
	for _, tc := range clientCases {
		t.Run(tc.name, func(t *testing.T) {
			var c OutboundConfig
			e := json.Unmarshal([]byte(tc.config), &c)
			var pw string
			if e == nil {
				m, err := c.Build()
				e = err
				if e == nil {
					pw, e = adapter.ResolveClientPassword(m.(*adapter.ClientConfig))
				}
			}
			if (e != nil) != tc.wantErr || e == nil && pw != tc.password {
				t.Fatalf("password %q error %v", pw, e)
			}
		})
	}
}
