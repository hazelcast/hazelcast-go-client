package benchmarks_test

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"testing"
)

func BenchmarkCreateShutdownClient(b *testing.B) {
	it.Benchmarker(b, func(b *testing.B, cb *hazelcast.ConfigBuilder) {
		for i := 0; i < b.N; i++ {
			client := it.MustClient(hazelcast.StartNewClientWithConfig(cb))
			it.Must(client.Shutdown())
		}
	})
}
