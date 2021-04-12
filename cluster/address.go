package cluster

import (
	"fmt"
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
	// TODO: remove this interface
	fmt.Stringer
	Host() string
	Port() int
	Clone() Address
	// TODO: add address hash
}

type AddressImpl struct {
	// TODO: rename to Address
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
