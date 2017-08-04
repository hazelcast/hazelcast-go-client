package serialization

import (
	"testing"
	"bytes"
)

func TestWriteInt(t *testing.T) {
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	WriteInt(buf, 5, 1, false)

	expectedBuf := []byte{0, 5, 0, 0, 0, 0, 0, 0}

	if bytes.Compare(buf, expectedBuf) != 0 {
		t.Errorf("WriteInt() makes ", buf, " expected ", expectedBuf)
	}
}

func TestReadInt(t *testing.T) {
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	var expectedRet int32 = 5
	WriteInt(buf, expectedRet, 1, false)
	var ret int32 = ReadInt(buf, 1, false)

	if ret != expectedRet {
		t.Errorf("ReadInt() return ", ret, " expected ", expectedRet)
	}
}
