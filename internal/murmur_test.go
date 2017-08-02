package internal

import (
	"testing"
)

type Murmur3ATestHelper struct {
	key string
	expected int64
	index int32
}
func TestMurmur3A(t *testing.T) {
	var list = []Murmur3ATestHelper{
		{"key-1", 1228513025, 107},
		{"key-2", 1503416236, 105},
		{"key-3", 1876349747, 218},
		{"key-4", -914632498, 181},
		{"key-5", -803210507, 111},
		{"key-6", -847942313, 115},
		{"key-7", 1196747334, 223},
		{"key-8", -1444149994, 208},
		{"key-9", 1182720020, 140},
	}
	for _,ele := range list {
		hash := Murmur3ADefault([]byte(ele.key),0,len(ele.key))
		if hash != int32(ele.expected){
			t.Errorf("Expected %d but was %d for Murmur3A\n", int32(ele.expected), hash)
		}
		index := hashToIndex(hash,271)
		if index != ele.index{
			t.Errorf("Expected %d but was %d for Murmur3A\n", ele.index, index)
		}
	}
}
