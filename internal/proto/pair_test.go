package proto

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client/hazelcast/serialization"

	"github.com/stretchr/testify/assert"
)

func TestNewPair_With_Int(t *testing.T) {
	//given
	key := 10
	value := 100

	//when
	pair := NewPair(key, value)

	//then
	assert.Equal(t, pair.key, key)
	assert.Equal(t, pair.value, value)
}

func TestNewPair_With_String(t *testing.T) {
	//given
	key := "10"
	value := "100"

	//when
	pair := NewPair(key, value)

	//then
	assert.Equal(t, pair.key, key)
	assert.Equal(t, pair.value, value)
}

func TestNewPair_With_Data(t *testing.T) {
	//given
	key := serialization.NewHeapData([]byte("key"))
	value := serialization.NewHeapData([]byte("value"))

	//when
	pair := NewPair(key, value)

	//then
	assert.Equal(t, pair.key, key)
	assert.Equal(t, pair.value, value)
}

func TestNewPair_With_ByteArray(t *testing.T) {
	//given
	key := []byte("key")
	value := []byte("value")

	//when
	pair := NewPair(key, value)

	//then
	assert.Equal(t, pair.key, key)
	assert.Equal(t, pair.value, value)
}

func TestNewPair_With_IntArray(t *testing.T) {
	//given
	key := []int32{10}
	value := []int32{100}

	//when
	pair := NewPair(key, value)

	//then
	assert.Equal(t, pair.key, key)
	assert.Equal(t, pair.value, value)
}
