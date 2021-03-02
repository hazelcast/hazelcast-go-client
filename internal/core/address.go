package core

import "fmt"

type Address struct {
	host string
	port int
}

func NewAddress(Host string, Port int32) *Address {
	return &Address{Host, int(Port)}
}

func NewAddressWithParameters(Host string, Port int) *Address {
	return &Address{Host, Port}
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
