package serialization_test

import (
	"encoding/binary"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"unicode/utf8"
)

// See: https://hazelcast.atlassian.net/wiki/spaces/IMDG/pages/1650294837/Hazelcast+Serialization+Improvements

func TestSerializationImprovements_1_UTFString(t *testing.T) {
	ss := mustSerializationService(iserialization.NewService(&serialization.Config{BigEndian: true}))
	target := `😭‍😭‍😭‍` // \x60\xf0\x9f\x98\xad\xe2\x80\x8d\xf0\x9f\x98\xad\xe2\x80\x8d\xf0\x9f\x98\xad\xe2\x80\x8d\x60
	data, err := ss.ToData(target)
	if err != nil {
		t.Fatal(err)
	}
	value, err := ss.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, target, value)
}

func TestSerializationImprovements_2_StringLength(t *testing.T) {
	ss := mustSerializationService(iserialization.NewService(&serialization.Config{BigEndian: true}))
	// the following text has 23 bytes but have 8 Unicode runes.
	text := "\x60\xf0\x9f\x98\xad\xe2\x80\x8d\xf0\x9f\x98\xad\xe2\x80\x8d\xf0\x9f\x98\xad\xe2\x80\x8d\x60"
	assert.Equal(t, 8, utf8.RuneCountInString(text))
	data, err := ss.ToData(text)
	if err != nil {
		t.Fatal(err)
	}
	slen := binary.BigEndian.Uint32(data.ToByteArray()[8:])
	assert.Equal(t, uint32(23), slen)
}

func TestSerializationImprovements_4_UUID(t *testing.T) {
	ss := mustSerializationService(iserialization.NewService(&serialization.Config{BigEndian: true}))
	target := types.NewUUIDWith(math.MaxUint64, math.MaxUint64)
	data, err := ss.ToData(target)
	if err != nil {
		t.Fatal(err)
	}
	value, err := ss.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, target, value)
}

func mustSerializationService(ss *iserialization.Service, err error) *iserialization.Service {
	if err != nil {
		panic(err)
	}
	return ss
}
