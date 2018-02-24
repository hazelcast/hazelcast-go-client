// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package protocol

import (
	"encoding/hex"
	"testing"
)

var READ_HEADER = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0}

func TestHeaderFields(t *testing.T) {
	message := NewClientMessage(nil, 30)
	var correlationId int64 = 6474838
	var messageType MessageType = 987
	var flags uint8 = 5
	var partitionId int32 = 27
	var frameLength int32 = 100
	var dataOffset uint16 = 17

	message.SetFrameLength(frameLength)
	message.SetFlags(flags)
	message.SetMessageType(messageType)
	message.SetPartitionId(partitionId)
	message.SetDataOffset(dataOffset)
	message.SetCorrelationId(correlationId)

	if result := message.CorrelationId(); result != correlationId {
		t.Errorf("CorrelationId returned %d expected %d \n", result, correlationId)
	}
	if result := message.MessageType(); result != messageType {
		t.Errorf("MessageType returned %d expected %d \n", result, messageType)
	}
	if result := message.PartitionId(); result != partitionId {
		t.Errorf("PartitionId returned %d expected %d \n", result, partitionId)
	}
	if result := message.Flags(); result != flags {
		t.Errorf("Flags returned %d expected %d \n", result, flags)
	}
	if result := message.FrameLength(); result != frameLength {
		t.Errorf("GetFrameLengthreturned %d expected %d \n", result, frameLength)
	}
	if result := message.DataOffset(); result != dataOffset {
		t.Errorf("DataOffset returned %d expected %d \n", result, dataOffset)
	}
}

func TestClientMessage_AppendByte(t *testing.T) {
	message := NewClientMessage(nil, 30)
	message.AppendByte(0x21)
	message.AppendByte(0xF2)
	message.AppendByte(0x34)
	dataOffset := message.DataOffset()
	result := message.Buffer[dataOffset : dataOffset+3]
	if hexResult := hex.EncodeToString(result); hexResult != "21f234" {
		t.Errorf("AppendByte returned %s expected 21f234", hexResult)
	}
}
func TestClientMessage_AppendUint8(t *testing.T) {
	message := NewClientMessage(nil, 30)
	message.AppendUint8(0x21)
	message.AppendUint8(0xF2)
	message.AppendUint8(0x34)
	dataOffset := message.DataOffset()
	result := message.Buffer[dataOffset : dataOffset+3]
	if hexResult := hex.EncodeToString(result); hexResult != "21f234" {
		t.Errorf("AppendUint8 returned %s expected 21f234", hexResult)
	}
}
func TestClientMessage_AppendBool(t *testing.T) {
	message := NewClientMessage(nil, 30)
	message.AppendBool(true)
	dataOffset := message.DataOffset()
	result := message.Buffer[dataOffset : dataOffset+1]
	if hexResult := hex.EncodeToString(result); hexResult != "01" {
		t.Errorf("AppendBool returned %s expected 01", hexResult)
	}
}
func TestClientMessage_AppendInt(t *testing.T) {
	message := NewClientMessage(nil, 30)
	message.AppendInt32(0x1feeddcc)
	dataOffset := message.DataOffset()
	result := message.Buffer[dataOffset : dataOffset+4]
	if hexResult := hex.EncodeToString(result); hexResult != "ccddee1f" {
		t.Errorf("AppendInt32 returned %s expected ccddee1f", hexResult)
	}
}
func TestClientMessage_AppendInt64(t *testing.T) {
	message := NewClientMessage(nil, 30)
	message.AppendInt64(0x1feeddccbbaa8765)
	dataOffset := message.DataOffset()
	result := message.Buffer[dataOffset : dataOffset+8]
	if hexResult := hex.EncodeToString(result); hexResult != "6587aabbccddee1f" {
		t.Errorf("AppendInt64 returned %s expected 6587aabbccddee1f", hexResult)
	}
}
func TestClientMessage_AppendString(t *testing.T) {
	message := NewClientMessage(nil, 30)
	testString := "abc"
	message.AppendString(&testString)
	dataOffset := message.DataOffset()
	result := message.Buffer[dataOffset : dataOffset+4]
	if hexResult := hex.EncodeToString(result); hexResult != "03000000" {
		t.Errorf("AppendString length returned %s expected 03000000", hexResult)
	}
	result = message.Buffer[dataOffset+4 : dataOffset+7]
	if hexResult := hex.EncodeToString(result); hexResult != "616263" {
		t.Errorf("AppendString returned %s expected 616263", hexResult)
	}

}
func TestClientMessage_ReadUint8(t *testing.T) {
	buf := make([]byte, len(READ_HEADER)+1)
	buf[len(READ_HEADER)] = 0x78
	message := NewClientMessage(buf, 1)
	if result := message.ReadUint8(); result != 0x78 {
		t.Errorf("ReadUint returned %d expected 0x78", result)
	}
}
func TestClientMessage_ReadInt(t *testing.T) {
	buf := make([]byte, len(READ_HEADER)+4)
	buf[len(READ_HEADER)] = 0x12
	buf[len(READ_HEADER)+1] = 0x34
	buf[len(READ_HEADER)+2] = 0x56
	buf[len(READ_HEADER)+3] = 0x78
	message := NewClientMessage(buf, 4)
	if result := message.ReadInt32(); result != 0x78563412 {
		t.Errorf("ReadInt32 returned %d expected 0x78563412", result)
	}
}
func TestClientMessage_ReadInt64(t *testing.T) {
	buf := make([]byte, len(READ_HEADER)+8)
	buf[len(READ_HEADER)] = 0x65
	buf[len(READ_HEADER)+1] = 0x87
	buf[len(READ_HEADER)+2] = 0xaa
	buf[len(READ_HEADER)+3] = 0xbb
	buf[len(READ_HEADER)+4] = 0xcc
	buf[len(READ_HEADER)+5] = 0xdd
	buf[len(READ_HEADER)+6] = 0xee
	buf[len(READ_HEADER)+7] = 0x1f
	message := NewClientMessage(buf, 8)
	if result := message.ReadInt64(); result != 0x1feeddccbbaa8765 {
		t.Errorf("ReadInt64 returned %d expected 0x1feeddccbbaa8765", result)
	}
}
func TestClientMessage_ReadString(t *testing.T) {
	buf := make([]byte, len(READ_HEADER)+7)
	buf[len(READ_HEADER)] = 0x03
	buf[len(READ_HEADER)+1] = 0x00
	buf[len(READ_HEADER)+2] = 0x00
	buf[len(READ_HEADER)+3] = 0x00
	buf[len(READ_HEADER)+4] = 0x61
	buf[len(READ_HEADER)+5] = 0x62
	buf[len(READ_HEADER)+6] = 0x63
	message := NewClientMessage(buf, 7)
	if result := message.ReadString(); *result != "abc" {
		t.Errorf("ReadString returned %s expected abc", *result)
	}
}
func TestNoFlag(t *testing.T) {
	message := NewClientMessage(nil, 30)
	message.SetFlags(0)
	if result := message.HasFlags(BEGIN_FLAG); result != 0 {
		t.Errorf("HasFlag returned %d expected 0", result)
	}
	if result := message.HasFlags(END_FLAG); result != 0 {
		t.Errorf("HasFlag returned %d expected 0", result)
	}
	if result := message.HasFlags(LISTENER_FLAG); result != 0 {
		t.Errorf("HasFlag returned %d expected 0", result)
	}
}
func TestSetFlagBegin(t *testing.T) {
	message := NewClientMessage(nil, 30)
	message.SetFlags(0)
	message.AddFlags(BEGIN_FLAG)
	if result := message.HasFlags(BEGIN_FLAG); result == 0 {
		t.Errorf("HasFlag returned %d expected 128", result)
	}
	if result := message.HasFlags(END_FLAG); result != 0 {
		t.Errorf("HasFlag returned %d expected 0", result)
	}
	if result := message.HasFlags(LISTENER_FLAG); result != 0 {
		t.Errorf("HasFlag returned %d expected 0", result)
	}
}
func TestSetFlagEnd(t *testing.T) {
	message := NewClientMessage(nil, 30)
	message.SetFlags(0)
	message.AddFlags(END_FLAG)
	if result := message.HasFlags(BEGIN_FLAG); result != 0 {
		t.Errorf("HasFlag returned %d expected 0", result)
	}
	if result := message.HasFlags(END_FLAG); result == 0 {
		t.Errorf("HasFlag returned %d expected 64", result)
	}
	if result := message.HasFlags(LISTENER_FLAG); result != 0 {
		t.Errorf("HasFlag returned %d expected 0", result)
	}
}
func TestSetListenerFlag(t *testing.T) {
	message := NewClientMessage(nil, 30)
	message.SetFlags(0)
	message.AddFlags(LISTENER_FLAG)
	if result := message.HasFlags(BEGIN_FLAG); result != 0 {
		t.Errorf("HasFlag returned %d expected 0", result)
	}
	if result := message.HasFlags(END_FLAG); result != 0 {
		t.Errorf("HasFlag returned %d expected 0", result)
	}
	if result := message.HasFlags(LISTENER_FLAG); result == 0 {
		t.Errorf("HasFlag returned %d expected 1", result)
	}
}
func TestCalculateSizeStr(t *testing.T) {
	testString := "abc"
	if result := StringCalculateSize(&testString); result != len(testString)+INT_SIZE_IN_BYTES {
		t.Errorf("StringCalculateSize returned %d expected %d", result, len(testString)+INT_SIZE_IN_BYTES)
	}
}
func TestClientMessage_UpdateFrameLength(t *testing.T) {
	message := NewClientMessage(nil, 30)
	message.AppendBool(true)
	message.UpdateFrameLength()
	if result := message.FrameLength(); result != 23 {
		t.Errorf("UpdateFrameLength returned %d expected 23", result)
	}
	message.AppendInt64(0x1feeddccbbaa8765)
	message.UpdateFrameLength()
	if result := message.FrameLength(); result != 31 {
		t.Errorf("UpdateFrameLength returned %d expected 31", result)
	}
}
