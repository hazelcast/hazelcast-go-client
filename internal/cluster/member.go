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
	"reflect"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type uuid struct {
	msb int64
	lsb int64
}

type Member struct {
	address      pubcluster.Address
	uuid         types.UUID
	isLiteMember bool
	attributes   map[string]string
	version      pubcluster.MemberVersion
	addressMap   map[pubcluster.EndpointQualifier]pubcluster.Address
}

func NewMember(address pubcluster.Address, uuid types.UUID, isLiteMember bool, attributes map[string]string, version pubcluster.MemberVersion, addressMap map[pubcluster.EndpointQualifier]pubcluster.Address) *Member {
	return &Member{address: address, uuid: uuid, isLiteMember: isLiteMember, attributes: attributes, version: version, addressMap: addressMap}
}

func (m Member) Address() pubcluster.Address {
	return m.address
}

func (m Member) UUID() types.UUID {
	return m.uuid
}

func (m Member) LiteMember() bool {
	return m.isLiteMember
}

func (m Member) Attributes() map[string]string {
	return m.attributes
}

func (m *Member) String() string {
	memberInfo := fmt.Sprintf("Member %s - %s", m.address.String(), m.UUID())
	if m.LiteMember() {
		memberInfo += " lite"
	}
	return memberInfo
}

func (m *Member) HasSameAddress(member *Member) bool {
	return m.address == member.address
}

func (m *Member) Equal(member2 Member) bool {
	if m.address != member2.address {
		return false
	}
	if m.uuid != member2.uuid {
		return false
	}
	if m.isLiteMember != member2.isLiteMember {
		return false
	}
	if !reflect.DeepEqual(m.attributes, member2.attributes) {
		return false
	}
	return true
}
