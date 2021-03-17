package cluster

import (
	"fmt"
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/v4/internal"
)

type uuid struct {
	msb int64
	lsb int64
}

type Member struct {
	address      pubcluster.Address
	uuid         internal.UUID
	isLiteMember bool
	attributes   map[string]string
	version      pubcluster.MemberVersion
	addressMap   map[internal.EndpointQualifier]pubcluster.Address
}

func NewMember(address pubcluster.Address, uuid internal.UUID, isLiteMember bool, attributes map[string]string, version pubcluster.MemberVersion, addressMap map[internal.EndpointQualifier]pubcluster.Address) *Member {
	return &Member{address: address, uuid: uuid, isLiteMember: isLiteMember, attributes: attributes, version: version, addressMap: addressMap}
}

func (m Member) Address() pubcluster.Address {
	return m.address
}

func (m Member) UUID() internal.UUID {
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
