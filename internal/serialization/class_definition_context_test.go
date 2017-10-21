package serialization

import (
	"testing"
)

func TestClassDefinitionContext_DecodeVersionedClassId(t *testing.T) {
	pair1, pair2 := decodeVersionedClassId(encodeVersionedClassId(2, 5))
	if pair1 != 2 || pair2 != 5 {
		t.Errorf("There is a problem in decodeVersionedClassId() or encodedVersionedClassId()")
	}
}
