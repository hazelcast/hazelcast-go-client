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

	"github.com/hazelcast/hazelcast-go-client/types"
)

// Member represents a member in the cluster with its address, uuid, lite member status and attributes.
type Member interface {
	fmt.Stringer
	// Addr returns the address of this member.
	Address() Address

	// UUID returns the uuid of this member.
	UUID() types.UUID

	// IsLiteMember returns true if this member is a lite member.
	LiteMember() bool

	// Attributes returns configured attributes for this member.
	Attributes() map[string]string
}

// MemberVersion
type MemberVersion struct {
	major int8
	minor int8
	patch int8
}

func NewMemberVersion(major, minor, patch byte) MemberVersion {
	return MemberVersion{int8(major), int8(minor), int8(patch)}
}

func (memberVersion MemberVersion) Major() byte {
	return byte(memberVersion.major)
}

func (memberVersion MemberVersion) Minor() byte {
	return byte(memberVersion.minor)
}

func (memberVersion MemberVersion) Patch() byte {
	return byte(memberVersion.patch)
}

// MemberInfo represents a member in the cluster with its address, uuid, lite member status, attributes and version.
type MemberInfo struct {
	address    Address
	attributes map[ // address is proto.Address: Address of the member.
	// attributes are configured attributes of the member
	string]string
	addrMap map[ // addrMap
	EndpointQualifier]Address
	uuid       types.UUID
	version    MemberVersion
	liteMember bool // uuid UUID of the member.
	// liteMember represents member is a lite member. Lite members do not own any partition.
}

func NewMemberInfo(address Address, uuid types.UUID, attributes map[string]string, liteMember bool, version MemberVersion,
	isAddressMapExists bool, addressMap interface{}) MemberInfo {
	// TODO: Convert addrMap to map[EndpointQualifier]*Address
	// copy address
	return MemberInfo{address: address.Clone(), uuid: uuid, attributes: attributes, liteMember: liteMember, version: version,
		addrMap: addressMap.(map[EndpointQualifier]Address)}
}

func (mi MemberInfo) Address() Address {
	return mi.address
}

func (mi MemberInfo) UUID() types.UUID {
	return mi.uuid
}

func (mi MemberInfo) LiteMember() bool {
	return mi.liteMember
}

func (mi MemberInfo) Attributes() map[string]string {
	return mi.attributes
}

func (mi MemberInfo) Version() MemberVersion {
	return mi.version
}

func (mi MemberInfo) AddressMap() map[EndpointQualifier]Address {
	return mi.addrMap
}

func (mi MemberInfo) String() string {
	return fmt.Sprintf("%s:%s", mi.address, mi.uuid)
}
