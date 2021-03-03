package core

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

const (
	defaultHost = "localhost"
	defaultPort = 5701
)

type Address struct {
	host string
	port int
}

func NewAddress(Host string, Port int32) *Address {
	return NewAddressWithHostPort(Host, int(Port))
}

// TODO: merge this one with NewAddress
func NewAddressWithHostPort(Host string, Port int) *Address {
	return &Address{Host, Port}
}

func ParseAddress(addr string) (*Address, error) {
	// first check whether addr contains the port
	if index := strings.Index(addr, ":"); index == 0 {
		return nil, errors.New("error parsing address: address with no host")
	} else if index < 0 {
		if addr == "" {
			// default address
			return NewAddressWithHostPort(defaultHost, defaultPort), nil
		}
		// this is probably a string with only the host
		return NewAddressWithHostPort(addr, defaultPort), nil
	}
	if host, portStr, err := net.SplitHostPort(addr); err != nil {
		return nil, fmt.Errorf("error parsing address: %w", err)
	} else {
		if port, err := strconv.Atoi(portStr); err != nil {
			return nil, fmt.Errorf("error parsing address: %w", err)
		} else {
			return NewAddressWithHostPort(host, port), nil
		}
	}
}

func (a Address) Host() string {
	return a.host
}

func (a Address) Port() int {
	return int(a.port)
}

func (a Address) String() string {
	return fmt.Sprintf("%s:%d", a.host, a.port)
}
