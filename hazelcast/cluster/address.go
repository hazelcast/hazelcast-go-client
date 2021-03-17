package cluster

import (
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"net"
	"strconv"
	"strings"
)

// AddressTranslator is used to resolve private ip address of cloud services.
type AddressTranslator interface {

	// Translate translates the given address to another address specific
	// to network or service
	Translate(address Address) Address
}

// defaultAddressTranslator is a no-op. It always returns the given address.
type defaultAddressTranslator struct {
}

func NewDefaultAddressTranslator() *defaultAddressTranslator {
	return &defaultAddressTranslator{}
}

func (dat *defaultAddressTranslator) Translate(address Address) Address {
	return address
}

type Address interface {
	fmt.Stringer
	Host() string
	Port() int
	Clone() Address
	// TODO: add address hash
}

type AddressImpl struct {
	host string
	port int
	// TODO: add address hash
}

func NewAddress(Host string, Port int32) *AddressImpl {
	return NewAddressWithHostPort(Host, int(Port))
}

// TODO: merge this one with NewAddress
func NewAddressWithHostPort(Host string, Port int) *AddressImpl {
	return &AddressImpl{Host, Port}
}

func ParseAddress(addr string) (*AddressImpl, error) {
	// first check whether addr contains the port
	if index := strings.Index(addr, ":"); index == 0 {
		return nil, errors.New("error parsing address: address with no host")
	} else if index < 0 {
		if addr == "" {
			// default address
			return NewAddressWithHostPort(internal.DefaultHost, internal.DefaultPort), nil
		}
		// this is probably a string with only the host
		return NewAddressWithHostPort(addr, internal.DefaultPort), nil
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

func (a AddressImpl) Host() string {
	return a.host
}

func (a AddressImpl) Port() int {
	return int(a.port)
}

func (a AddressImpl) String() string {
	return fmt.Sprintf("%s:%d", a.host, a.port)
}

func (a AddressImpl) Clone() Address {
	return &AddressImpl{
		host: a.host,
		port: a.port,
	}
}
