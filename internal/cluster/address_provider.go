/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
)

type AddressProvider interface {
	Addresses() []*pubcluster.AddressImpl
}

type DefaultAddressProvider struct {
	addresses []*pubcluster.AddressImpl
}

func ParseAddress(addr string) (*pubcluster.AddressImpl, error) {
	// first check whether addr contains the port
	if index := strings.Index(addr, ":"); index == 0 {
		return nil, errors.New("error parsing address: address with no host")
	} else if index < 0 {
		if addr == "" {
			// default address
			return pubcluster.NewAddressWithHostPort(pubcluster.DefaultHost, pubcluster.DefaultPort), nil
		}
		// this is probably a string with only the host
		return pubcluster.NewAddressWithHostPort(addr, pubcluster.DefaultPort), nil
	}
	if host, portStr, err := net.SplitHostPort(addr); err != nil {
		return nil, fmt.Errorf("error parsing address: %w", err)
	} else {
		if port, err := strconv.Atoi(portStr); err != nil {
			return nil, fmt.Errorf("error parsing address: %w", err)
		} else {
			return pubcluster.NewAddressWithHostPort(host, port), nil
		}
	}
}

func NewDefaultAddressProvider(networkConfig *pubcluster.Config) *DefaultAddressProvider {
	var err error
	addresses := make([]*pubcluster.AddressImpl, len(networkConfig.Addrs))
	for i, addr := range networkConfig.Addrs {
		if addresses[i], err = ParseAddress(addr); err != nil {
			panic(err)
		}
	}
	return &DefaultAddressProvider{addresses: addresses}
}

func (p DefaultAddressProvider) Addresses() []*pubcluster.AddressImpl {
	return p.addresses
}
