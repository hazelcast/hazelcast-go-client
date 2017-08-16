package common

import (
	"bytes"
	"testing"
)

func TestWriteInt32(t *testing.T) {
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	WriteInt32(buf, 1, 5, false)

	expectedBuf := []byte{0, 5, 0, 0, 0, 0, 0, 0}

	if bytes.Compare(buf, expectedBuf) != 0 {
		t.Errorf("WriteInt32() makes ", buf, " expected ", expectedBuf)
	}
}

func TestReadInt32(t *testing.T) {
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	var expectedRet int32 = 5
	WriteInt32(buf, 1, expectedRet, false)
	var ret int32 = ReadInt32(buf, 1, false)

	if ret != expectedRet {
		t.Errorf("ReadInt32() returns", ret, " expected ", expectedRet)
	}
}

func TestReadFloat64(t *testing.T) {
	var expectedRet float64 = 6.723
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	WriteFloat64(buf, 0, 5.234, false)
	WriteFloat64(buf, 1, expectedRet, false)

	var ret float64 = ReadFloat64(buf, 1, false)

	if expectedRet != ret {
		t.Errorf("ReadFloat64() returns", ret, " expected", expectedRet)
	}
}

func TestReadBool(t *testing.T) {
	buf := []byte{0, 0, 0}
	WriteBool(buf, 0, true)
	WriteBool(buf, 2, true)
	if ReadBool(buf, 0) != true && ReadBool(buf, 2) != true {
		t.Errorf("There is a problem in ReadBool() or WriteBool()")
	}
}

func TestReadUInt8(t *testing.T) {
	buf := []byte{0, 0, 0}
	WriteUInt8(buf, 1, 5)
	WriteUInt8(buf, 2, 12)
	if ReadUInt8(buf, 1) != 5 && ReadUInt8(buf, 2) != 12 {
		t.Errorf("There is a problem in ReadUInt8() or WriteUInt8()")
	}

}
