package serialization

import (
	"testing"
	"bytes"
)

/// Tests for ObjectDataOutput ///
func TestObjectDataOutput_EnsureAvailable(t *testing.T) {
	o := NewObjectDataOutput(2, serializationService{}, false)
	o.EnsureAvailable(5)
	buf := o.buffer
	expectedBuf := []byte{0, 0, 0, 0, 0}
	if bytes.Compare(buf, expectedBuf) != 0 {
		t.Errorf("EnsureAvailable() makes ", buf, " expected ", expectedBuf)
	}

}

func TestObjectDataOutput_WriteInt(t *testing.T) {
	o := NewObjectDataOutput(4, serializationService{}, false)
	o.WriteInt(1)
	o.WriteInt(2)
	o.WriteInt(3)

	if o.buffer[0] != 1 || o.buffer[4] != 2 || o.buffer[8] != 3 {
		t.Errorf("WriteInt() writes to wrong position!")
	}
}

/// Tests for ObjectDataInput ///
func TestObjectDataInput_AssertAvailable(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 1, 2, 3}, 3, serializationService{}, true)
	ret := o.AssertAvailable(2)
	if ret == nil {
		t.Errorf("AssertAvailable() should return error '%s' but it returns nil!", ret)

	}

}

func TestObjectDataInput_ReadInt(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0}, 4, serializationService{}, false)
	expectedRet := 2
	ret, _ := o.ReadInt(8)

	if ret != int32(expectedRet) {
		t.Errorf("AssertAvailable() should return error '%s' but it returns nil!", ret)
	}
}
