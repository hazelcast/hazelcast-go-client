package cloud

import (
	"context"
	"fmt"
	"sync"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

type AddressTranslator struct {
	dc         *DiscoveryClient
	translator map[string]pubcluster.Address
	mu         *sync.RWMutex
}

func NewAddressTranslator(config *pubcluster.Config, logger logger.Logger) *AddressTranslator {
	return &AddressTranslator{
		dc:         NewDiscoveryClient(&config.HazelcastCloudConfig, logger),
		translator: map[string]pubcluster.Address{},
		mu:         &sync.RWMutex{},
	}
}

func (a *AddressTranslator) Translate(ctx context.Context, address pubcluster.Address) (pubcluster.Address, error) {
	a.mu.RLock()
	pubAddr, ok := a.translator[address.String()]
	a.mu.RUnlock()
	if ok {
		return pubAddr, nil
	}
	// address not found, try discovering the nodes
	if err := a.reload(ctx); err != nil {
		return address, err
	}
	a.mu.RLock()
	pubAddr, ok = a.translator[address.String()]
	a.mu.RUnlock()
	if ok {
		return pubAddr, nil
	}
	// address still not found, fail
	return address, fmt.Errorf("address not found: %s", address.String())
}

func (a *AddressTranslator) TranslateMember(ctx context.Context, member *pubcluster.MemberInfo) (addr pubcluster.Address, err error) {
	return a.Translate(ctx, member.Address)
}

func (a *AddressTranslator) reload(ctx context.Context) error {
	addrs, err := a.dc.DiscoverNodes(ctx)
	if err != nil {
		return err
	}
	t := map[string]pubcluster.Address{}
	for _, addr := range addrs {
		t[addr.Private] = pubcluster.Address(addr.Public)
	}
	a.mu.Lock()
	a.translator = t
	a.mu.Unlock()
	return nil
}
