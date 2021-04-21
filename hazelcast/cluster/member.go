package cluster

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
)

// Member represents a member in the cluster with its address, uuid, lite member status and attributes.
type Member interface {
	fmt.Stringer
	// Address returns the address of this member.
	Address() *Address

	// UUID returns the uuid of this member.
	UUID() internal.UUID

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
	// address is proto.Address: Address of the member.
	address Address

	// uuid is core.UUID: UUID of the member.
	uuid internal.UUID

	// liteMember represents member is a lite member. Lite members do not own any partition.
	liteMember bool

	// attributes are configured attributes of the member
	attributes map[string]string

	// version is core.MemberVersion: Hazelcast codebase version of the member.
	version MemberVersion

	// addressMap
	addressMap map[internal.EndpointQualifier]Address
}

func NewMemberInfo(address Address, uuid internal.UUID, attributes map[string]string, liteMember bool, version MemberVersion,
	isAddressMapExists bool, addressMap interface{}) MemberInfo {
	// TODO: Convert addressMap to map[EndpointQualifier]*Address
	// copy address
	return MemberInfo{address: address.Clone(), uuid: uuid, attributes: attributes, liteMember: liteMember, version: version,
		addressMap: addressMap.(map[internal.EndpointQualifier]Address)}
}

func (memberInfo MemberInfo) Address() Address {
	return memberInfo.address
}

func (memberInfo MemberInfo) Uuid() internal.UUID {
	return memberInfo.uuid
}

func (memberInfo MemberInfo) Attributes() map[string]string {
	return memberInfo.attributes
}

func (memberInfo MemberInfo) LiteMember() bool {
	return memberInfo.liteMember
}

func (memberInfo MemberInfo) Version() MemberVersion {
	return memberInfo.version
}

func (memberInfo MemberInfo) AddressMap() map[internal.EndpointQualifier]Address {
	return memberInfo.addressMap
}
