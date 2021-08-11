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
	"context"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
)

// AddressTranslator is used to resolve private ip address of cloud services.
type AddressTranslator interface {
	// Translate translates the given address to another address
	Translate(ctx context.Context, address pubcluster.Address) (addr pubcluster.Address, err error)

	// TranslateMember translates the given member's address to another address
	TranslateMember(ctx context.Context, member *pubcluster.MemberInfo) (addr pubcluster.Address, err error)
}

type defaultAddressTranslator struct {
}

func NewDefaultAddressTranslator() *defaultAddressTranslator {
	return &defaultAddressTranslator{}
}

func (a *defaultAddressTranslator) Translate(ctx context.Context, addr pubcluster.Address) (pubcluster.Address, error) {
	return addr, nil
}

func (a *defaultAddressTranslator) TranslateMember(ctx context.Context, member *pubcluster.MemberInfo) (pubcluster.Address, error) {
	return member.Address, nil
}

type defaultPublicAddressTranslator struct {
}

func NewDefaultPublicAddressTranslator() *defaultPublicAddressTranslator {
	return &defaultPublicAddressTranslator{}
}

func (a *defaultPublicAddressTranslator) Translate(ctx context.Context, addr pubcluster.Address) (pubcluster.Address, error) {
	return addr, nil
}

func (a *defaultPublicAddressTranslator) TranslateMember(ctx context.Context, member *pubcluster.MemberInfo) (pubcluster.Address, error) {
	if addr, ok := member.PublicAddress(); ok {
		return addr, nil
	}
	return "", hzerrors.ErrAddressNotFound
}
