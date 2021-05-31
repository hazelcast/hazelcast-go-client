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
	"fmt"
)

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
	str  string
	port int
	// TODO: add address hash
}

func NewAddress(host string, port int32) *AddressImpl {
	return NewAddressWithHostPort(host, int(port))
}

// TODO: merge this one with NewAddress
func NewAddressWithHostPort(host string, port int) *AddressImpl {
	return &AddressImpl{host: host, port: port, str: fmt.Sprintf("%s:%d", host, port)}
}

func (a AddressImpl) Host() string {
	return a.host
}

func (a AddressImpl) Port() int {
	return int(a.port)
}

func (a AddressImpl) String() string {
	return a.str
}

func (a AddressImpl) Clone() Address {
	return &AddressImpl{
		host: a.host,
		port: a.port,
		str:  a.str,
	}
}

type EndpointQualifier struct {
	Identifier string
	Type       int32
}
