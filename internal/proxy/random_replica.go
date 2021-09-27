package proxy

import (
	"math/rand"
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
)

var commonRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandomPNCounterReplica(members []cluster.MemberInfo, n int, filter func(mem *cluster.MemberInfo) bool) (cluster.MemberInfo, bool) {
	if n > len(members) {
		n = len(members)
	}
	if n == 0 {
		return cluster.MemberInfo{}, false
	}
	// scans first n members, starting from idx, wrapping at n
	idx := commonRand.Intn(n)
	for i, mem := range members {
		if mem.LiteMember {
			continue
		}
		if i < idx {
			continue
		}
		if filter == nil || filter(&mem) {
			return mem, true
		}
	}
	return cluster.MemberInfo{}, false
}
