package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal"
)

type CheckedAddressHelper struct {
	err     error
	address pubcluster.Address
}

func TestTryConnectAddress_NoAddressAvailable(t *testing.T) {
	inputAddress := "127.0.0.1:0"
	checkedAddresses := []CheckedAddressHelper{
		{address: "127.0.0.1:5701", err: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5702", err: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5703", err: errors.New("cannot connect to address in the cluster")},
	}
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}
	connMemberCounter, _, err := tryConnectAddressTest(checkedAddresses, inputAddress, portRange)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "cannot connect to any address in the cluster")
	assert.Equal(t, connMemberCounter, 3)
}

func TestTryConnectAddress_LastAddressAvailable(t *testing.T) {
	inputAddress := "127.0.0.1:0"
	checkedAddresses := []CheckedAddressHelper{
		{address: "127.0.0.1:5701", err: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5702", err: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5703", err: nil},
	}
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}
	connMemberCounter, _, err := tryConnectAddressTest(checkedAddresses, inputAddress, portRange)
	assert.Nil(t, err)
	assert.Equal(t, connMemberCounter, 3)
}

func TestTryConnectAddress_SecondAddressAvailable(t *testing.T) {
	inputAddress := "127.0.0.1:0"
	checkedAddresses := []CheckedAddressHelper{
		{address: "127.0.0.1:5701", err: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5702", err: nil},
		{address: "127.0.0.1:5703", err: nil},
	}
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}
	connMemberCounter, _, err := tryConnectAddressTest(checkedAddresses, inputAddress, portRange)
	assert.Nil(t, err)
	assert.Equal(t, connMemberCounter, 2)
}

func TestTryConnectAddress_MultipleAddressesAvailable(t *testing.T) {
	inputAddresses := []string{"127.0.0.1:0", "192.168.1.2:6000"}
	checkedAddresses := []CheckedAddressHelper{
		{address: "127.0.0.1:5701", err: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5702", err: nil},
		{address: "127.0.0.1:5703", err: nil},
		{address: "192.168.1.2:6000", err: nil},
	}
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}
	connMemberCounter := 0
	for _, address := range inputAddresses {
		c, _, err := tryConnectAddressTest(checkedAddresses, address, portRange)
		assert.Nil(t, err)
		connMemberCounter += c
	}
	assert.Equal(t, connMemberCounter, 3)
}

// tryConnectAddressTest makes it easier to perform cluster address connectivity tests
func tryConnectAddressTest(checkedAddresses []CheckedAddressHelper, inputAddress string, portRange pubcluster.PortRange) (int, pubcluster.Address, error) {
	connMemberCounter := 0
	host, port, err := internal.ParseAddr(inputAddress)
	if err != nil {
		return connMemberCounter, "", err
	}
	m := &ConnectionManager{}
	m.clusterConfig = &pubcluster.Config{}
	m.clusterConfig.Network.PortRange = portRange
	resultAddr, err := m.tryConnectAddress(context.TODO(), pubcluster.NewAddress(host, int32(port)),
		func(ctx context.Context, m *ConnectionManager, currAddr pubcluster.Address) (pubcluster.Address, error) {
			connMemberCounter++
			for _, checkedAddr := range checkedAddresses {
				if currAddr == checkedAddr.address {
					return checkedAddr.address, checkedAddr.err
				}
			}
			return currAddr, nil
		})
	return connMemberCounter, resultAddr, err
}

func TestEnumerateAddresses(t *testing.T) {
	host := "127.0.0.1"
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}
	expectedAddrs := []pubcluster.Address{
		pubcluster.NewAddress(host, 5701),
		pubcluster.NewAddress(host, 5702),
		pubcluster.NewAddress(host, 5703),
	}
	addrs := EnumerateAddresses(host, portRange)
	assert.Equal(t, addrs, expectedAddrs)
}
