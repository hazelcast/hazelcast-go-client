package common

import (
	"testing"
	"bytes"
)

func TestWriteInt32(t *testing.T) {
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	WriteInt32(buf, 5, 1, false)

	expectedBuf := []byte{0, 5, 0, 0, 0, 0, 0, 0}

	if bytes.Compare(buf, expectedBuf) != 0 {
		t.Errorf("WriteInt32() makes ", buf, " expected ", expectedBuf)
	}
}

func TestReadInt32(t *testing.T) {
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	var expectedRet int32 = 5
	WriteInt32(buf, expectedRet, 1, false)
	var ret int32 = ReadInt32(buf, 1, false)

	if ret != expectedRet {
		t.Errorf("ReadInt32() return ", ret, " expected ", expectedRet)
	}
}

func TestReadFloat64(t *testing.T) {
	var expectedRet float64=6.723
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	WriteFloat64(buf, 5.234, 0, false)
	WriteFloat64(buf, expectedRet, 1, false)

	var ret float64=ReadFloat64(buf,1,false)



	if expectedRet!=ret {
		t.Errorf("ReadInt64() return", ret, " expected", expectedRet)
	}
}

