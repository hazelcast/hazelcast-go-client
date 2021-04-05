package cluster

import (
	"time"
)

const (
	DefaultHost = "localhost"
	DefaultPort = 5701
)

type Config struct {
	Name              string
	Addrs             []string
	SmartRouting      bool
	ConnectionTimeout time.Duration
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	InvocationTimeout time.Duration
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
	}
}
