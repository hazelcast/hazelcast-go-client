package cluster

import (
	"time"
)

const (
	DefaultHost = "localhost"
	DefaultPort = 5701
)

type SecurityConfig struct {
	Username string
	Password string
}

func (c SecurityConfig) Clone() SecurityConfig {
	return SecurityConfig{
		Username: c.Username,
		Password: c.Password,
	}
}

type Config struct {
	Name              string
	Addrs             []string
	SmartRouting      bool
	ConnectionTimeout time.Duration
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	InvocationTimeout time.Duration
	SecurityConfig    SecurityConfig
}

func (c Config) Clone() Config {
	addrs := make([]string, len(c.Addrs))
	copy(addrs, c.Addrs)
	return Config{
		Name:              c.Name,
		Addrs:             addrs,
		SmartRouting:      c.SmartRouting,
		ConnectionTimeout: c.ConnectionTimeout,
		HeartbeatInterval: c.HeartbeatInterval,
		HeartbeatTimeout:  c.HeartbeatTimeout,
		InvocationTimeout: c.InvocationTimeout,
		SecurityConfig:    c.SecurityConfig.Clone(),
	}
}
