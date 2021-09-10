/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package codec

import (
	"encoding/binary"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/types"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

func TestCodecUtil_FastForwardToEndFrame(t *testing.T) {
	frame1 := proto.NewFrameWith([]byte("value-1"), proto.BeginDataStructureFlag)
	frame2 := proto.NewFrameWith([]byte("value-2"), proto.EndDataStructureFlag)
	frame3 := proto.NewFrameWith([]byte("value-3"), proto.EndDataStructureFlag)
	message := proto.NewClientMessage(frame1)
	message.AddFrame(frame2)
	message.AddFrame(frame3)
	iterator := message.FrameIterator()
	FastForwardToEndFrame(iterator)
	assert.False(t, iterator.HasNext())
}

func TestCodecUtil_EncodeNullable(t *testing.T) {
	frame1 := proto.NewFrame([]byte("value-0"))
	message := proto.NewClientMessage(frame1)
	EncodeNullable(message, "encode-value-1", EncodeString)
	iterator := message.FrameIterator()
	assert.Equal(t, string(iterator.Next().Content), "value-0")
	assert.Equal(t, string(iterator.Next().Content), "encode-value-1")
	assert.Equal(t, string(message.Frames[len(message.Frames)-1].Content), "encode-value-1")
}

func TestCodecUtil_NextFrameIsDataStructureEndFrame(t *testing.T) {
	byteValue := []byte("value-0")
	flags := uint16(proto.EndDataStructureFlag)
	frame := proto.NewFrameWith(byteValue, flags)
	message := proto.NewClientMessage(frame)
	frameIterator := message.FrameIterator()
	isEndFrame := NextFrameIsDataStructureEndFrame(frameIterator)
	assert.True(t, isEndFrame)
}

func TestCodecUtil_NextFrameIsNullFrame(t *testing.T) {
	nullFrame := proto.NullFrame
	message := proto.NewClientMessage(nullFrame)
	frame := proto.NewFrame([]byte("value-0"))
	message.AddFrame(frame)
	frameIterator := message.FrameIterator()
	isNextFrameIsNull := NextFrameIsNullFrame(frameIterator)
	assert.True(t, isNextFrameIsNull)
}

func TestCodecUtil_NextFrameIsNullFrame_Return_False_When_Next_Frame_Is_Not_Null(t *testing.T) {
	frame1 := proto.NewFrame([]byte("value-1"))
	frame2 := proto.NewFrame([]byte("value-2"))
	message := proto.NewClientMessage(frame1)
	message.AddFrame(frame2)
	frameIterator := message.FrameIterator()
	isNextFrameIsNull := NextFrameIsNullFrame(frameIterator)
	assert.False(t, isNextFrameIsNull)
}

func TestByteArrayCodec_Encode(t *testing.T) {
	value := []byte("value-1")
	message := proto.NewClientMessageForEncode()
	EncodeByteArray(message, value)
	iterator := message.FrameIterator()
	assert.Equal(t, string(iterator.Next().Content), "value-1")
}

func TestByteArrayCodec_Decode(t *testing.T) {
	value := []byte("value-1")
	message := proto.NewClientMessage(proto.NewFrame(value))
	decode := DecodeByteArray(message.FrameIterator())
	assert.Equal(t, string(decode), "value-1")
}

func TestCodecUtil_EncodeNullable_If_Value_Is_Null_Add_Null_Frame_To_Message(t *testing.T) {
	frame1 := proto.NewFrame([]byte("value-0"))
	message := proto.NewClientMessage(frame1)
	EncodeNullable(message, nil, EncodeString)
	iterator := message.FrameIterator()
	assert.Equal(t, string(iterator.Next().Content), "value-0")
	assert.True(t, iterator.Next().IsNullFrame())
}

func TestDataCodec_EncodeNullable_When_Data_Is_Nil(t *testing.T) {
	message := proto.NewClientMessageForEncode()
	EncodeNullableData(message, nil)
	iterator := message.FrameIterator()
	assert.Len(t, iterator.Next().Content, 0)
}

func TestDataCodec_Decode(t *testing.T) {
	bytes := []byte("value-0")
	frame := proto.NewFrame(bytes)
	message := proto.NewClientMessageForDecode(frame)
	frameIterator := message.FrameIterator()
	decode := DecodeNullableData(frameIterator)
	assert.Equal(t, []byte(decode), bytes)
}

func TestDataCodec_Decode_When_Data_Is_Nil(t *testing.T) {
	nullFrame := proto.NullFrame
	message := proto.NewClientMessage(nullFrame)
	frameIterator := message.FrameIterator()
	decode := DecodeNullableData(frameIterator)
	assert.Nil(t, decode)
}

func TestEntryListCodec_Encode(t *testing.T) {
	bytes := []byte("value-0")
	frame := proto.NewFrame(bytes)
	message := proto.NewClientMessageForDecode(frame)
	var pairs []proto.Pair
	pairs = append(pairs, proto.Pair{Key: "key", Value: "value"})
	EncodeEntryList(message, pairs, EncodeString, EncodeString)
	frameIterator := message.FrameIterator()
	assert.Equal(t, string(frameIterator.Next().Content), "value-0")
	beginFrame := frameIterator.Next()
	assert.Empty(t, beginFrame.Content)
	assert.True(t, beginFrame.IsBeginFrame())
	assert.Equal(t, string(frameIterator.Next().Content), "key")
	assert.Equal(t, string(frameIterator.Next().Content), "value")
	endFrame := frameIterator.Next()
	assert.Empty(t, endFrame.Content)
	assert.True(t, endFrame.IsEndFrame())
}

func TestEntryListCodec_Encode_When_Entries_Is_Empty(t *testing.T) {
	bytes := []byte("value-0")
	frame := proto.NewFrame(bytes)
	message := proto.NewClientMessageForDecode(frame)
	var pairs []proto.Pair
	EncodeEntryList(message, pairs, EncodeString, EncodeString)
	frameIterator := message.FrameIterator()
	assert.Equal(t, string(frameIterator.Next().Content), "value-0")
	beginFrame := frameIterator.Next()
	assert.Empty(t, beginFrame.Content)
	assert.True(t, beginFrame.IsBeginFrame())
	endFrame := frameIterator.Next()
	assert.Empty(t, endFrame.Content)
	assert.True(t, endFrame.IsEndFrame())
}

func TestEntryListCodec_EncodeNullable(t *testing.T) {
	bytes := []byte("value-0")
	frame := proto.NewFrame(bytes)
	message := proto.NewClientMessageForDecode(frame)
	var pairs []proto.Pair
	pairs = append(pairs, proto.Pair{Key: "key", Value: "value"})
	EncodeNullableEntryList(message, pairs, EncodeString, EncodeString)
	frameIterator := message.FrameIterator()
	assert.Equal(t, string(frameIterator.Next().Content), "value-0")
	beginFrame := frameIterator.Next()
	assert.Empty(t, beginFrame.Content)
	assert.True(t, beginFrame.IsBeginFrame())
	assert.Equal(t, string(frameIterator.Next().Content), "key")
	assert.Equal(t, string(frameIterator.Next().Content), "value")
	endFrame := frameIterator.Next()
	assert.Empty(t, endFrame.Content)
	assert.True(t, endFrame.IsEndFrame())
}

func TestEntryListCodec_EncodeNullable_When_Entries_Is_Empty(t *testing.T) {
	message := proto.NewClientMessageForEncode()
	var pairs []proto.Pair
	EncodeNullableEntryList(message, pairs, EncodeString, EncodeString)
	frameIterator := message.FrameIterator()
	assert.True(t, frameIterator.Next().IsNullFrame())
}

func TestEntryListCodec_DecodeNullable(t *testing.T) {
	nullFrame := proto.NullFrame
	message := proto.NewClientMessageForDecode(nullFrame)
	iterator := message.FrameIterator()
	results := DecodeNullableEntryList(iterator, DecodeData, DecodeData)
	assert.Nil(t, results)
}

func TestEntryListCodec_DecodeNullable_When_Next_Frame_Is_Null_Frame(t *testing.T) {
	message := proto.NewClientMessageForDecode(proto.NullFrame)
	iterator := message.FrameIterator()
	results := DecodeNullableEntryList(iterator, DecodeData, DecodeData)
	assert.Empty(t, results)
}

func TestFixSizedTypesCodec_EncodeInt(t *testing.T) {
	buffer := make([]byte, 4)
	offset := int32(0)
	value := int32(100)
	FixSizedTypesCodec.EncodeInt(buffer, offset, value)
	assert.Equal(t, binary.LittleEndian.Uint32(buffer), uint32(100))
}

func TestFixSizedTypesCodec_DecodeInt(t *testing.T) {
	buffer := make([]byte, 4)
	offset := int32(0)
	value := int32(100)
	FixSizedTypesCodec.EncodeInt(buffer, offset, value)
	decodeInt := FixSizedTypesCodec.DecodeInt(buffer, offset)
	assert.Equal(t, decodeInt, value)
}

func TestFixSizedTypesCodec_EncodeLong(t *testing.T) {
	buffer := make([]byte, 8)
	offset := int32(0)
	value := int64(100000000000000000)
	FixSizedTypesCodec.EncodeLong(buffer, offset, value)
	assert.Equal(t, int64(binary.LittleEndian.Uint64(buffer[offset:])), value)
}

func TestFixSizedTypesCodec_DecodeLong(t *testing.T) {
	buffer := make([]byte, 8)
	offset := int32(0)
	value := int64(100000000000000000)
	FixSizedTypesCodec.EncodeLong(buffer, offset, value)
	decodeLong := FixSizedTypesCodec.DecodeLong(buffer, offset)
	assert.Equal(t, decodeLong, value)
}

func TestFixSizedTypesCodec_EncodeBool(t *testing.T) {
	buffer := make([]byte, 1)
	offset := int32(0)
	FixSizedTypesCodec.EncodeBoolean(buffer, offset, true)
	assert.True(t, buffer[offset] == 1)
}

func TestFixSizedTypesCodec_EncodeBool_When_Value_Is_False(t *testing.T) {
	buffer := make([]byte, 1)
	offset := int32(0)
	FixSizedTypesCodec.EncodeBoolean(buffer, offset, false)
	assert.True(t, buffer[offset] == 0)
}

func TestFixSizedTypesCodec_EncodeByte(t *testing.T) {
	buffer := make([]byte, 1)
	offset := int32(0)
	FixSizedTypesCodec.EncodeByte(buffer, offset, 'b')
	assert.Equal(t, string(buffer[0]), "b")
}

func TestFixSizedTypesCodec_DecodeByte(t *testing.T) {
	buffer := make([]byte, 1)
	offset := int32(0)
	FixSizedTypesCodec.EncodeByte(buffer, offset, 'b')
	decodeByte := FixSizedTypesCodec.DecodeByte(buffer, offset)
	assert.Equal(t, string(decodeByte), "b")
}

func TestFixSizedTypesCodec_EncodeUUID(t *testing.T) {
	buffer := make([]byte, proto.UUIDSizeInBytes)
	offset := int32(0)
	uuid := types.NewUUID()
	FixSizedTypesCodec.EncodeUUID(buffer, offset, uuid)
	assert.Equal(t, FixSizedTypesCodec.DecodeBoolean(buffer, offset), false)
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(buffer, offset+proto.BooleanSizeInBytes), int64(uuid.MostSignificantBits()))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(buffer, offset+proto.BooleanSizeInBytes+proto.LongSizeInBytes), int64(uuid.LeastSignificantBits()))
}

func TestFixSizedTypesCodec_EncodeUUID_When_UUID_Is_Nil(t *testing.T) {
	buffer := make([]byte, proto.UUIDSizeInBytes)
	offset := int32(0)
	FixSizedTypesCodec.EncodeUUID(buffer, offset, types.UUID{})
	assert.Equal(t, FixSizedTypesCodec.DecodeBoolean(buffer, offset), true)
}

func TestFixSizedTypesCodec_DecodeUUID(t *testing.T) {
	buffer := make([]byte, proto.UUIDSizeInBytes)
	offset := int32(0)
	uuid := types.NewUUID()
	FixSizedTypesCodec.EncodeUUID(buffer, offset, uuid)
	decodeUUID := FixSizedTypesCodec.DecodeUUID(buffer, offset)
	assert.Equal(t, FixSizedTypesCodec.DecodeBoolean(buffer, offset), false)
	assert.Equal(t, uuid, decodeUUID)
	assert.Equal(t, uuid.String(), decodeUUID.String())
	assert.Equal(t, uuid.MostSignificantBits(), decodeUUID.MostSignificantBits())
	assert.Equal(t, uuid.LeastSignificantBits(), decodeUUID.LeastSignificantBits())
}

func TestEntryListUUIDLongCodec_Encode(t *testing.T) {
	message := proto.NewClientMessageForEncode()
	key := types.NewUUID()
	value := int64(100)
	var pairs []proto.Pair
	pairs = append(pairs, proto.Pair{Key: key, Value: value})
	EncodeEntryListUUIDLong(message, pairs)
	pairs = DecodeEntryListUUIDLong(message.FrameIterator())
	frame := pairs[0]
	assert.Equal(t, frame.Key.(types.UUID).String(), key.String())
	assert.Equal(t, frame.Value.(int64), value)
}

func TestListUUIDCodec_Encode(t *testing.T) {
	message := proto.NewClientMessageForEncode()
	var entries []types.UUID
	value1 := types.NewUUID()
	value2 := types.NewUUID()
	entries = append(entries, value1, value2)
	EncodeListUUID(message, entries)
	frame := message.FrameIterator().Next()
	decodeUUID1 := FixSizedTypesCodec.DecodeUUID(frame.Content, 0)
	assert.Equal(t, value1.String(), decodeUUID1.String())
	decodeUUID2 := FixSizedTypesCodec.DecodeUUID(frame.Content, 17)
	assert.Equal(t, value2.String(), decodeUUID2.String())
}

func TestListIntegerCodec_Encode(t *testing.T) {
	clientMessage := proto.NewClientMessageForEncode()
	var entries []int32
	entries = append(entries, 1, 2, 3)
	EncodeListInteger(clientMessage, entries)
	frame := clientMessage.FrameIterator().Next()
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(frame.Content, 0), int32(1))
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(frame.Content, 4), int32(2))
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(frame.Content, 8), int32(3))
}

func TestListIntegerCodec_Decode(t *testing.T) {
	clientMessage := proto.NewClientMessageForEncode()
	var entries []int32
	entries = append(entries, 1, 2, 3)
	EncodeListInteger(clientMessage, entries)
	decodeEntries := DecodeListInteger(clientMessage.FrameIterator())
	assert.Equal(t, decodeEntries[0], int32(1))
	assert.Equal(t, decodeEntries[1], int32(2))
	assert.Equal(t, decodeEntries[2], int32(3))
}

func TestListLongCodec_Encode(t *testing.T) {
	message := proto.NewClientMessageForEncode()
	var entries []int64
	entries = append(entries, 1, 2, 3)
	EncodeListLong(message, entries)
	frame := message.FrameIterator().Next()
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 0), int64(1))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 8), int64(2))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 16), int64(3))
}

func TestListLongCodec_Decode(t *testing.T) {
	message := proto.NewClientMessageForEncode()
	var entries []int64
	entries = append(entries, 1, 2, 3)
	EncodeListLong(message, entries)
	result := DecodeListLong(message.FrameIterator())
	assert.Equal(t, result, entries)
}

func TestEntryListUUIDListIntegerCodec_Encode(t *testing.T) {
	clientMessage := proto.NewClientMessageForEncode()
	key := types.NewUUID()
	var value []int32
	value = append(value, 1, 2, 3)
	pair := proto.Pair{Key: key, Value: value}
	var entries []proto.Pair
	entries = append(entries, pair)
	EncodeEntryListUUIDListInteger(clientMessage, entries)
	iterator := clientMessage.FrameIterator()
	assert.Equal(t, iterator.Next().IsBeginFrame(), true)
	integerValues := iterator.Next()
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(integerValues.Content, 0), int32(1))
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(integerValues.Content, 4), int32(2))
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(integerValues.Content, 8), int32(3))
	assert.Equal(t, iterator.Next().IsEndFrame(), true)
	uuid := FixSizedTypesCodec.DecodeUUID(iterator.Next().Content, 0)
	assert.Equal(t, uuid.String(), key.String())
}

func TestEntryListUUIDListIntegerCodec_Decode(t *testing.T) {
	clientMessage := proto.NewClientMessageForEncode()
	key := types.NewUUID()
	var value []int32
	value = append(value, 1, 2, 3)
	pair := proto.Pair{Key: key, Value: value}
	var entries []proto.Pair
	entries = append(entries, pair)
	EncodeEntryListUUIDListInteger(clientMessage, entries)
	result := DecodeEntryListUUIDListInteger(clientMessage.FrameIterator())
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Key.([]types.UUID)[0].String(), key.String())
	assert.EqualValues(t, result[0].Value.([]int32), value)
}

func TestLongArrayCodec_Encode(t *testing.T) {
	clientMessage := proto.NewClientMessageForEncode()
	var entries []int64
	entries = append(entries, 1, 2, 3)
	EncodeLongArray(clientMessage, entries)
	frame := clientMessage.FrameIterator().Next()
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 0), int64(1))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 8), int64(2))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 16), int64(3))
}

func TestLongArrayCodec_Decode(t *testing.T) {
	clientMessage := proto.NewClientMessageForEncode()
	var entries []int64
	entries = append(entries, 1, 2, 3)
	EncodeLongArray(clientMessage, entries)
	result := DecodeLongArray(clientMessage.FrameIterator())
	assert.Equal(t, result[0], int64(1))
	assert.Equal(t, result[1], int64(2))
	assert.Equal(t, result[2], int64(3))
}

func TestStringCodec_Encode(t *testing.T) {
	value := "value-encode"
	frame := proto.NewFrame([]byte(""))
	clientMessage := proto.NewClientMessage(frame)
	EncodeString(clientMessage, value)
	content := clientMessage.Frames[len(clientMessage.Frames)-1].Content
	assert.Equal(t, value, string(content))
}
