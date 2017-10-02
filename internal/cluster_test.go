package internal

import (
	"github.com/hazelcast/go-client/internal/protocol"
	"testing"
)

func Test_getPossibleAddresses(t *testing.T) {
	configAddresses := []string{
		"132.63.211.12:5012",
		"132.63.211.12:5011",
		"132.63.211.12:5010",
		"132.63.211.12:5010",
		"12.63.31.12:501",
	}
	members := []protocol.Member{
		*protocol.NewMember(*protocol.NewAddressWithParameters("132.63.211.12", 5012), "", false, nil),
		*protocol.NewMember(*protocol.NewAddressWithParameters("55.63.211.112", 5011), "", false, nil),
	}
	addresses := getPossibleAddresses(&configAddresses, &members)
	if len(*addresses) != 5 {
		t.Fatal("getPossibleAddresses failed")
	}
	addresses = getPossibleAddresses(nil, nil)
	if len(*addresses) != 1 {
		t.Fatal("getPossibleAddresses failed")
	}
}
