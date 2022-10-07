/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/types"
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
	nc := &pubcluster.NetworkConfig{PortRange: portRange}
	mf := func(_ context.Context, _ *ConnectionManager, currAddr pubcluster.Address, _ *pubcluster.NetworkConfig) (pubcluster.Address, error) {
		connMemberCounter++
		for _, checkedAddr := range checkedAddresses {
			if currAddr == checkedAddr.address {
				return checkedAddr.address, checkedAddr.err
			}
		}
		return currAddr, nil
	}
	resultAddr, err := m.tryConnectAddress(context.TODO(), pubcluster.NewAddress(host, int32(port)), mf, nc)
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

func TestFilterConns(t *testing.T) {
	uuid1 := types.NewUUID()
	uuid2 := types.NewUUID()
	uuid3 := types.NewUUID()
	uuid4 := types.NewUUID()
	uuid5 := types.NewUUID()
	tcs := []struct {
		description string
		input       []*Connection
		members     map[types.UUID]struct{}
		target      []*Connection
	}{
		{
			input:       []*Connection{},
			description: "empty connection slice",
			target:      []*Connection{},
		},
		{
			description: "single member",
			input:       []*Connection{makeConn(uuid1)},
			members:     map[types.UUID]struct{}{uuid1: {}},
			target:      []*Connection{makeConn(uuid1)},
		},
		{
			description: "single non-member",
			input:       []*Connection{makeConn(uuid1)},
			members:     map[types.UUID]struct{}{},
			target:      []*Connection{},
		},
		{
			description: "none members",
			input:       []*Connection{makeConn(uuid1), makeConn(uuid2), makeConn(uuid3), makeConn(uuid4), makeConn(uuid5)},
			members:     map[types.UUID]struct{}{},
			target:      []*Connection{},
		},
		{
			description: "first member",
			input:       []*Connection{makeConn(uuid1), makeConn(uuid2), makeConn(uuid3), makeConn(uuid4), makeConn(uuid5)},
			members:     map[types.UUID]struct{}{uuid1: {}},
			target:      []*Connection{makeConn(uuid1)},
		},
		{
			description: "last member",
			input:       []*Connection{makeConn(uuid1), makeConn(uuid2), makeConn(uuid3), makeConn(uuid4), makeConn(uuid5)},
			members:     map[types.UUID]struct{}{uuid5: {}},
			target:      []*Connection{makeConn(uuid5)},
		},
		{
			description: "mixed members",
			input:       []*Connection{makeConn(uuid1), makeConn(uuid2), makeConn(uuid3), makeConn(uuid4), makeConn(uuid5)},
			members:     map[types.UUID]struct{}{uuid1: {}, uuid3: {}, uuid5: {}},
			target:      []*Connection{makeConn(uuid1), makeConn(uuid5), makeConn(uuid3)},
		},
		{
			description: "all members",
			input:       []*Connection{makeConn(uuid1), makeConn(uuid2), makeConn(uuid3), makeConn(uuid4), makeConn(uuid5)},
			members:     map[types.UUID]struct{}{uuid1: {}, uuid2: {}, uuid3: {}, uuid4: {}, uuid5: {}},
			target:      []*Connection{makeConn(uuid1), makeConn(uuid2), makeConn(uuid3), makeConn(uuid4), makeConn(uuid5)},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.description, func(t *testing.T) {
			input := make([]*Connection, len(tc.input))
			copy(input, tc.input)
			output := FilterConns(input, func(conn *Connection) bool {
				_, found := tc.members[conn.MemberUUID()]
				return found
			})
			assert.Equal(t, tc.target, output)
		})
	}
}

func makeConn(uuid types.UUID) *Connection {
	c := &Connection{}
	c.memberUUID.Store(uuid)
	return c
}
