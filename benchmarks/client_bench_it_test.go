package benchmarks_test

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"testing"
)

func BenchmarkCreateShutdownClient(b *testing.B) {
	it.Benchmarker(b, func(b *testing.B, config *hazelcast.Config) {
		for i := 0; i < b.N; i++ {
			client := it.MustClient(hazelcast.StartNewClientWithConfig(*config))
			it.Must(client.Shutdown())
		}
	})
}
