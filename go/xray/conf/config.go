// Package conf builds Shine protocol settings for Xray's JSON config loader.
package conf

import (
	"bytes"
	"encoding/json"
	"fmt"

	adapter "github.com/hhhflow2020/shine/go/xray"
	"google.golang.org/protobuf/proto"
)

// Users requires a nonempty array; null must not silently disable authentication.
type Users []UserConfig

func (u *Users) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("shine: users cannot be null")
	}
	var entries []UserConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&entries); err != nil {
		return fmt.Errorf("shine: invalid users: %w", err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("shine: users array cannot be empty")
	}
	*u = entries
	return nil
}

// UserConfig follows Xray's user configuration: one auth secret per user.
type UserConfig struct {
	Auth  string `json:"auth"`
	Email string `json:"email"`
	Level uint32 `json:"level"`
}
type Options struct {
	Password                string `json:"password"` // legacy single-password configuration
	Users                   Users  `json:"users"`
	InitialWindow           uint32 `json:"initialWindow"`
	MaxFrameSize            uint32 `json:"maxFrameSize"`
	MaxDataSize             uint32 `json:"maxDataSize"`
	HandshakeTimeoutSeconds uint32 `json:"handshakeTimeoutSeconds"`
	PingIntervalSeconds     uint32 `json:"pingIntervalSeconds"`
	PongTimeoutSeconds      uint32 `json:"pongTimeoutSeconds"`
}
type InboundConfig struct{ Options }
type OutboundConfig struct {
	Options
	Address string `json:"address"`
	Port    uint32 `json:"port"`
}

// Reject unsupported fields rather than silently ignoring credential typos.
func decodeConfig(data []byte, target any) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("shine: settings cannot be null")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	return d.Decode(target)
}
func (c *InboundConfig) UnmarshalJSON(data []byte) error {
	type plain InboundConfig
	return decodeConfig(data, (*plain)(c))
}
func (c *OutboundConfig) UnmarshalJSON(data []byte) error {
	type plain OutboundConfig
	return decodeConfig(data, (*plain)(c))
}
func (o Options) userConfigs() []*adapter.User {
	users := make([]*adapter.User, 0, len(o.Users))
	for _, u := range o.Users {
		users = append(users, &adapter.User{Auth: u.Auth, Email: u.Email, Level: u.Level})
	}
	return users
}
func (c *InboundConfig) Build() (proto.Message, error) {
	o := c.Options
	config := &adapter.ServerConfig{Password: o.Password, Users: o.userConfigs(), InitialWindow: o.InitialWindow, MaxFrameSize: o.MaxFrameSize, MaxDataSize: o.MaxDataSize, HandshakeTimeoutSeconds: o.HandshakeTimeoutSeconds, PingIntervalSeconds: o.PingIntervalSeconds, PongTimeoutSeconds: o.PongTimeoutSeconds}
	if err := adapter.ValidateServerConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}
func (c *OutboundConfig) Build() (proto.Message, error) {
	o := c.Options
	config := &adapter.ClientConfig{Address: c.Address, Port: c.Port, Password: o.Password, Users: o.userConfigs(), InitialWindow: o.InitialWindow, MaxFrameSize: o.MaxFrameSize, MaxDataSize: o.MaxDataSize, HandshakeTimeoutSeconds: o.HandshakeTimeoutSeconds, PingIntervalSeconds: o.PingIntervalSeconds, PongTimeoutSeconds: o.PongTimeoutSeconds}
	if _, err := adapter.ResolveClientPassword(config); err != nil {
		return nil, err
	}
	return config, nil
}
