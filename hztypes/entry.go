package hztypes

import "github.com/hazelcast/hazelcast-go-client/internal/types"

type Entry = types.Entry

func NewEntry(key interface{}, value interface{}) Entry {
	return Entry{
		Key:   key,
		Value: value,
	}
}
