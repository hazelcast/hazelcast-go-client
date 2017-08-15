package serialization

import (
	"testing"
	"bytes"
)

/// Tests for ObjectDataOutput ///
func TestObjectDataOutput_EnsureAvailable(t *testing.T) {
	o := NewObjectDataOutput(2, &SerializationService{}, false)
	o.EnsureAvailable(5)
	buf := o.buffer
	expectedBuf := []byte{0, 0, 0, 0, 0}
	if bytes.Compare(buf, expectedBuf) != 0 {
		t.Errorf("EnsureAvailable() makes ", buf, " expected ", expectedBuf)
	}

}

func TestObjectDataOutput_WriteInt32(t *testing.T) {
	o := NewObjectDataOutput(4, &SerializationService{}, false)
	o.WriteInt32(1)
	o.WriteInt32(2)
	o.WriteInt32(3)

	if o.buffer[0] != 1 || o.buffer[4] != 2 || o.buffer[8] != 3 {
		t.Errorf("WriteInt32() writes to wrong position!")
	}
}

/// Tests for ObjectDataInput ///
func TestObjectDataInput_AssertAvailable(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 1, 2, 3}, 3, SerializationService{}, true)
	ret := o.AssertAvailable(2)
	if ret == nil {
		t.Errorf("AssertAvailable() should return error '%s' but it returns nil!", ret)
	}
}

func TestObjectDataInput_ReadInt32(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0}, 4, SerializationService{}, false)
	expectedRet := 4
	ret, _ := o.ReadInt32()

	if ret != int32(expectedRet) {
		t.Errorf("ReadInt32() should return '%s' expected %s", ret,expectedRet)
	}
}

func TestObjectDataInput_ReadInt32WithPosition(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0}, 4, SerializationService{}, false)
	expectedRet := 2
	ret, _ := o.ReadInt32WithPosition(8)

	if ret != int32(expectedRet) {
		t.Errorf("ReadInt32WithPosition() should return '%s' expected %s", ret,expectedRet)
	}
}

func TestObjectDataInput_ReadFloat64(t *testing.T) {
	o := NewObjectDataOutput(24, &SerializationService{}, false)
	o.WriteFloat64(1.234)
	o.WriteFloat64(2.544)
	o.WriteFloat64(3.432)
	i:=NewObjectDataInput(o.buffer,16,SerializationService{},false)
	var expectedRet float64=3.432
	var ret float64
	ret,_=i.ReadFloat64()
	if ret != float64(expectedRet) {
		t.Errorf("ReadFloat64() should return '%s' expected %s", ret,expectedRet)
	}
}

func TestObjectDataInput_ReadFloat64WithPosition(t *testing.T) {
	o := NewObjectDataOutput(24, &SerializationService{}, false)
	o.WriteFloat64(1.234)
	o.WriteFloat64(2.544)
	o.WriteFloat64(3.432)
	i:=NewObjectDataInput(o.buffer,16,SerializationService{},false)
	var expectedRet float64=2.544
	var ret float64
	ret,_=i.ReadFloat64WithPosition(8)
	if ret != float64(expectedRet) {
		t.Errorf("ReadFloat64() should return '%s' expected %s", ret,expectedRet)
	}
}