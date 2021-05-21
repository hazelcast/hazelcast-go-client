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

type EndpointQualifier struct {
	Identifier string
	Type       int32
}
