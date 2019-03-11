// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bufferutil

import (
	"bytes"
	"testing"
)

func TestWriteInt32(t *testing.T) {
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	WriteInt32(buf, 1, 5, false)

	expectedBuf := []byte{0, 5, 0, 0, 0, 0, 0, 0}

	if !bytes.Equal(buf, expectedBuf) {
		t.Error("WriteInt32() makes ", buf, " expected ", expectedBuf)
	}
}

func TestReadInt32(t *testing.T) {
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	var expectedRet int32 = 5
	WriteInt32(buf, 1, expectedRet, false)
	var ret = ReadInt32(buf, 1, false)

	if ret != expectedRet {
		t.Error("ReadInt32() returns", ret, " expected ", expectedRet)
	}
}

func TestReadFloat64(t *testing.T) {
	var expectedRet = 6.723
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	WriteFloat64(buf, 0, 5.234, false)
	WriteFloat64(buf, 1, expectedRet, false)

	var ret = ReadFloat64(buf, 1, false)

	if expectedRet != ret {
		t.Error("ReadFloat64() returns", ret, " expected", expectedRet)
	}
}

func TestReadBool(t *testing.T) {
	buf := []byte{0, 0, 0}
	WriteBool(buf, 0, true)
	WriteBool(buf, 2, true)

	if !ReadBool(buf, 0) || !ReadBool(buf, 2) {
		t.Error("There is a problem in ReadBool() or WriteBool()")
	}
}

func TestReadUInt8(t *testing.T) {
	buf := []byte{0, 0, 0}
	WriteUInt8(buf, 1, 5)
	WriteUInt8(buf, 2, 12)
	if ReadUInt8(buf, 1) != 5 || ReadUInt8(buf, 2) != 12 {
		t.Error("There is a problem in ReadUInt8() or WriteUInt8()")
	}

}
