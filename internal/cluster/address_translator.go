package cluster

import pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"

// AddressTranslator is used to resolve private ip address of cloud services.
type AddressTranslator interface {

	// Translate translates the given address to another address specific
	// to network or service
	Translate(address *pubcluster.AddressImpl) *pubcluster.AddressImpl
}

// defaultAddressTranslator is a no-op. It always returns the given address.
type defaultAddressTranslator struct {
}

func NewDefaultAddressTranslator() *defaultAddressTranslator {
	return &defaultAddressTranslator{}
}

func (dat *defaultAddressTranslator) Translate(address *pubcluster.AddressImpl) *pubcluster.AddressImpl {
	return address
}
