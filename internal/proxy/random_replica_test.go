package proxy

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
)

func TestRandomPNCounterReplica_NoMembers(t *testing.T) {
	_, ok := RandomPNCounterReplica(noMembers(), 10, nil)
	if ok {
		t.Fatalf("expected false")
	}
}

func TestRandomPNCounterReplica_LiteMembers(t *testing.T) {
	_, ok := RandomPNCounterReplica(threeLiteMembers(), 10, nil)
	if ok {
		t.Fatalf("expected false")
	}
}

func TestRandomPNCounterReplica_OneDataMember(t *testing.T) {
	replica, ok := RandomPNCounterReplica(oneDataMember(), 10, nil)
	if !ok {
		t.Fatalf("expected true")
	}
	assert.Equal(t, pubcluster.Address("100.100.100.100"), replica.Address)
}

func TestRandomPNCounterReplica_FiveDataMembers(t *testing.T) {
	resetCommonRand()
	var replicas []string
	target := []string{"200.200.200.200", "100.100.100.100", "100.100.100.100", "300.300.300.300", "100.100.100.100"}
	for i := 0; i < 5; i++ {
		mem, ok := RandomPNCounterReplica(fiveDataMembers(), 3, nil)
		if !ok {
			t.Fatalf("expected true")
		}
		replicas = append(replicas, string(mem.Address))
	}
	assert.Equal(t, target, replicas)
}

func TestRandomPNCounterReplica_FiveDataFourLiteMembers(t *testing.T) {
	resetCommonRand()
	var replicas []string
	target := []string{"200.200.200.200", "100.100.100.100", "100.100.100.100", "200.200.200.200", "100.100.100.100"}
	for i := 0; i < 5; i++ {
		mem, ok := RandomPNCounterReplica(fiveDataFourLiteMembers(), 3, nil)
		if !ok {
			t.Fatalf("expected true")
		}
		replicas = append(replicas, string(mem.Address))
	}
	assert.Equal(t, target, replicas)
}

func TestRandomPNCounterReplicaExcluding_FiveDataFourLiteMembers(t *testing.T) {
	resetCommonRand()
	var replicas []string
	target := []string{"300.300.300.300", "100.100.100.100", "100.100.100.100", "300.300.300.300", "100.100.100.100"}
	exclude := pubcluster.Address("200.200.200.200")
	for i := 0; i < 5; i++ {
		mem, ok := RandomPNCounterReplica(fiveDataFourLiteMembers(), 3, func(mem *pubcluster.MemberInfo) bool {
			return mem.Address != exclude
		})
		if !ok {
			t.Fatalf("expected true")
		}
		replicas = append(replicas, string(mem.Address))
	}
	assert.Equal(t, target, replicas)
}

func noMembers() []pubcluster.MemberInfo {
	return nil
}

func threeLiteMembers() []pubcluster.MemberInfo {
	return []pubcluster.MemberInfo{
		{LiteMember: true},
		{LiteMember: true},
		{LiteMember: true},
	}
}

func oneDataMember() []pubcluster.MemberInfo {
	return []pubcluster.MemberInfo{
		{Address: "100.100.100.100"},
	}
}

func fiveDataMembers() []pubcluster.MemberInfo {
	return []pubcluster.MemberInfo{
		{Address: "100.100.100.100"},
		{Address: "200.200.200.200"},
		{Address: "300.300.300.300"},
		{Address: "400.400.400.400"},
		{Address: "500.500.500.500"},
	}
}

func fiveDataFourLiteMembers() []pubcluster.MemberInfo {
	return []pubcluster.MemberInfo{
		{Address: "100.100.100.100"},
		{Address: "150.150.150.150", LiteMember: true},
		{Address: "200.200.200.200"},
		{Address: "250.250.250.250", LiteMember: true},
		{Address: "300.300.300.300"},
		{Address: "350.350.350.350", LiteMember: true},
		{Address: "400.400.400.400"},
		{Address: "450.450.450.450", LiteMember: true},
		{Address: "500.500.500.500"},
	}
}

func resetCommonRand() {
	// reset the random generator to a known state
	commonRand = rand.New(rand.NewSource(123456789012345678))
}
