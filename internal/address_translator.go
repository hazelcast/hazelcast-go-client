// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import "github.com/hazelcast/hazelcast-go-client/v4/internal/core"

// AddressTranslator is used to resolve private ip address of cloud services.
type AddressTranslator interface {

	// Translate translates the given address to another address specific
	// to network or service
	Translate(address core.Address) core.Address
}

// defaultAddressTranslator is a no-op. It always returns the given address.
type defaultAddressTranslator struct {
}

func newDefaultAddressTranslator() *defaultAddressTranslator {
	return &defaultAddressTranslator{}
}

func (dat *defaultAddressTranslator) Translate(address core.Address) core.Address {
	return address
}
