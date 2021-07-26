package cluster

import (
	"context"
	"errors"
	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/stretchr/testify/assert"
	"testing"
)

type CheckedAddressHelper struct {
	error   error
	address pubcluster.Address
}

func TestTryConnectAddress_NoAddressAvailable(t *testing.T) {
	// inputs
	inputAddress := "127.0.0.1:0"
	checkedAddresses := []CheckedAddressHelper{
		{address: "127.0.0.1:5701", error: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5702", error: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5703", error: errors.New("cannot connect to address in the cluster")},
	}
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}

	// perform
	connMemberCounter, _, err := tryConnectAddressTest(checkedAddresses, inputAddress, portRange)

	// assert
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "cannot connect to any address in the cluster")
	assert.Equal(t, connMemberCounter, 3)
}

func TestTryConnectAddress_LastAddressAvailable(t *testing.T) {
	// inputs
	inputAddress := "127.0.0.1:0"
	checkedAddresses := []CheckedAddressHelper{
		{address: "127.0.0.1:5701", error: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5702", error: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5703", error: nil},
	}
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}

	// perform
	connMemberCounter, _, err := tryConnectAddressTest(checkedAddresses, inputAddress, portRange)

	// assert
	assert.Nil(t, err)
	assert.Equal(t, connMemberCounter, 3)
}

func TestTryConnectAddress_SecondAddressAvailable(t *testing.T) {
	// inputs
	inputAddress := "127.0.0.1:0"
	checkedAddresses := []CheckedAddressHelper{
		{address: "127.0.0.1:5701", error: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5702", error: nil},
		{address: "127.0.0.1:5703", error: nil},
	}
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}

	// perform
	connMemberCounter, _, err := tryConnectAddressTest(checkedAddresses, inputAddress, portRange)

	// assert
	assert.Nil(t, err)
	assert.Equal(t, connMemberCounter, 2)
}

func TestTryConnectAddress_MultipleAddressesAvailable(t *testing.T) {
	// inputs
	inputAddresses := []string{"127.0.0.1:0", "192.168.1.2:6000"}
	checkedAddresses := []CheckedAddressHelper{
		{address: "127.0.0.1:5701", error: errors.New("cannot connect to address in the cluster")},
		{address: "127.0.0.1:5702", error: nil},
		{address: "127.0.0.1:5703", error: nil},
		{address: "192.168.1.2:6000", error: nil},
	}
	portRange := pubcluster.PortRange{
		Min: 5701,
		Max: 5703,
	}

	connMemberCounter := 0
	for _, address := range inputAddresses {
		// perform
		c, _, err := tryConnectAddressTest(checkedAddresses, address, portRange)
		assert.Nil(t, err)
		connMemberCounter += c
	}

	// assert
	assert.Equal(t, connMemberCounter, 3)
}

// tryConnectAddressTest makes it easier to perform cluster address connectivity tests
func tryConnectAddressTest(checkedAddresses []CheckedAddressHelper, inputAddress string, portRange pubcluster.PortRange) (int, pubcluster.Address, error) {
	connMemberCounter := 0
	host, port, err := internal.ParseAddr(inputAddress)
	if err != nil {
		return connMemberCounter, "", err
	}
	resultAddr, err := tryConnectAddress(context.TODO(), nil, portRange, pubcluster.NewAddress(host, int32(port)),
		func(ctx context.Context, m *ConnectionManager, currAddr pubcluster.Address) (pubcluster.Address, error) {
			connMemberCounter++
			for _, checkedAddr := range checkedAddresses {
				if currAddr == checkedAddr.address {
					return checkedAddr.address, checkedAddr.error
				}
			}
			return currAddr, nil
		})

	return connMemberCounter, resultAddr, err
}
