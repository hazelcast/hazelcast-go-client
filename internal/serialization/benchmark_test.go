package serialization

import (
	"testing"

	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

func BenchmarkService_LookUpDefaultSerializer(b *testing.B) {
	s, err := NewService(&pubserialization.Config{})
	if err != nil {
		panic(err)
	}
	data, err := s.ToData([]string{"foo1", "foo2", "foo3"})
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		ser := s.LookUpDefaultSerializer(data)
		if ser == nil {
			n++
		}
	}
}
