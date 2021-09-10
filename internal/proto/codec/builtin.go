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
	"fmt"
	"net"
	"strconv"
	"strings"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// Encoder for ClientMessage and value
type Encoder func(message *proto.ClientMessage, value interface{})

// Decoder create *iserialization.Data
type Decoder func(frameIterator *proto.ForwardFrameIterator) *iserialization.Data

// CodecUtil
type codecUtil struct{}

var CodecUtil codecUtil

func (codecUtil) FastForwardToEndFrame(frameIterator *proto.ForwardFrameIterator) {
	numberOfExpectedEndFrames := 1
	var frame proto.Frame
	for numberOfExpectedEndFrames != 0 {
		frame = frameIterator.Next()
		if frame.IsEndFrame() {
			numberOfExpectedEndFrames--
		} else if frame.IsBeginFrame() {
			numberOfExpectedEndFrames++
		}
	}
}

func (codecUtil) EncodeNullable(message *proto.ClientMessage, value interface{}, encoder Encoder) {
	if value == nil {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		encoder(message, value)
	}
}

func (codecUtil) EncodeNullableForString(message *proto.ClientMessage, value string) {
	if strings.TrimSpace(value) == "" {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		EncodeString(message, value)
	}
}

func (codecUtil) EncodeNullableForBitmapIndexOptions(message *proto.ClientMessage, options *types.BitmapIndexOptions) {
	if options == nil {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		EncodeBitmapIndexOptions(message, *options)
	}
}

func (codecUtil) EncodeNullableForData(message *proto.ClientMessage, data *iserialization.Data) {
	if data == nil {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		EncodeData(message, data)
	}
}

func (c codecUtil) DecodeNullableForData(frameIterator *proto.ForwardFrameIterator) *iserialization.Data {
	if c.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeData(frameIterator)
}

func (c codecUtil) DecodeNullableForAddress(frameIterator *proto.ForwardFrameIterator) *pubcluster.Address {
	if c.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	addr := DecodeAddress(frameIterator)
	return &addr
}

func (c codecUtil) DecodeNullableForLongArray(frameIterator *proto.ForwardFrameIterator) []int64 {
	if c.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeLongArray(frameIterator)
}

func (c codecUtil) DecodeNullableForString(frameIterator *proto.ForwardFrameIterator) string {
	if c.NextFrameIsNullFrame(frameIterator) {
		return ""
	}
	return DecodeString(frameIterator)
}

func (codecUtil) NextFrameIsDataStructureEndFrame(frameIterator *proto.ForwardFrameIterator) bool {
	return frameIterator.PeekNext().IsEndFrame()
}

func (codecUtil) NextFrameIsNullFrame(frameIterator *proto.ForwardFrameIterator) bool {
	isNullFrame := frameIterator.PeekNext().IsNullFrame()
	if isNullFrame {
		frameIterator.Next()
	}
	return isNullFrame
}

func (c codecUtil) DecodeNullableForBitmapIndexOptions(frameIterator *proto.ForwardFrameIterator) types.BitmapIndexOptions {
	if c.NextFrameIsNullFrame(frameIterator) {
		return types.BitmapIndexOptions{}
	}
	return DecodeBitmapIndexOptions(frameIterator)
}

func (c codecUtil) DecodeNullableForSimpleEntryView(frameIterator *proto.ForwardFrameIterator) *types.SimpleEntryView {
	if c.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeSimpleEntryView(frameIterator)
}

func EncodeByteArray(message *proto.ClientMessage, value []byte) {
	message.AddFrame(proto.NewFrame(value))
}

func DecodeByteArray(frameIterator *proto.ForwardFrameIterator) []byte {
	return frameIterator.Next().Content
}

func EncodeData(message *proto.ClientMessage, value interface{}) {
	message.AddFrame(proto.NewFrame(value.(*iserialization.Data).ToByteArray()))
}

func EncodeNullableData(message *proto.ClientMessage, data *iserialization.Data) {
	if data == nil {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		message.AddFrame(proto.NewFrame(data.ToByteArray()))
	}
}

func DecodeData(frameIterator *proto.ForwardFrameIterator) *iserialization.Data {
	return iserialization.NewData(frameIterator.Next().Content)
}

func DecodeNullableData(frameIterator *proto.ForwardFrameIterator) *iserialization.Data {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeData(frameIterator)
}

func EncodeEntryList(message *proto.ClientMessage, entries []proto.Pair, keyEncoder, valueEncoder Encoder) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		keyEncoder(message, value.Key())
		valueEncoder(message, value.Value())
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func EncodeEntryListForStringAndString(message *proto.ClientMessage, entries []proto.Pair) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		EncodeString(message, value.Key())
		EncodeString(message, value.Value())
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func EncodeEntryListForStringAndByteArray(message *proto.ClientMessage, entries []proto.Pair) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		EncodeString(message, value.Key())
		EncodeByteArray(message, value.Value().([]byte))
	}
	message.AddFrame(proto.EndFrame.Copy())

}

func EncodeEntryListForDataAndData(message *proto.ClientMessage, entries []proto.Pair) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		EncodeData(message, value.Key())
		EncodeData(message, value.Value())
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func EncodeEntryListForDataAndListData(message *proto.ClientMessage, entries []proto.Pair) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		EncodeData(message, value.Key())
		EncodeListData(message, value.Value().([]*iserialization.Data))
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func EncodeNullableEntryList(message *proto.ClientMessage, entries []proto.Pair, keyEncoder, valueEncoder Encoder) {
	if len(entries) == 0 {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		EncodeEntryList(message, entries, keyEncoder, valueEncoder)
	}
}

func DecodeEntryList(frameIterator *proto.ForwardFrameIterator, keyDecoder, valueDecoder Decoder) []proto.Pair {
	result := make([]proto.Pair, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		key := keyDecoder(frameIterator)
		value := valueDecoder(frameIterator)
		result = append(result, proto.NewPair(key, value))
	}
	frameIterator.Next()
	return result
}

func DecodeNullableEntryList(frameIterator *proto.ForwardFrameIterator, keyDecoder, valueDecoder Decoder) []proto.Pair {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeEntryList(frameIterator, keyDecoder, valueDecoder)
}

func DecodeEntryListForStringAndEntryListIntegerLong(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	result := make([]proto.Pair, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		key := DecodeString(frameIterator)
		value := DecodeEntryListIntegerLong(frameIterator)
		result = append(result, proto.NewPair(key, value))
	}
	frameIterator.Next()
	return result
}

func DecodeEntryListForDataAndData(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	result := make([]proto.Pair, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		key := DecodeData(frameIterator)
		value := DecodeData(frameIterator)
		result = append(result, proto.NewPair(key, value))
	}
	frameIterator.Next()
	return result
}

func EncodeListIntegerIntegerInteger(message *proto.ClientMessage, entries []proto.Pair) {
	entryCount := len(entries)
	frame := proto.NewFrame(make([]byte, entryCount*proto.EntrySizeInBytes))
	for i := 0; i < entryCount; i++ {
		FixSizedTypesCodec.EncodeInt(frame.Content, int32(i*proto.EntrySizeInBytes), entries[i].Key().(int32))
		FixSizedTypesCodec.EncodeInt(frame.Content, int32(i*proto.EntrySizeInBytes+proto.IntSizeInBytes), entries[i].Value().(int32))
	}
	message.AddFrame(frame)
}

func DecodeListIntegerIntegerInteger(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.EntrySizeInBytes
	result := make([]proto.Pair, itemCount)
	for i := 0; i < itemCount; i++ {
		key := FixSizedTypesCodec.DecodeInt(frame.Content, int32(i*proto.EntrySizeInBytes))
		value := FixSizedTypesCodec.DecodeInt(frame.Content, int32(i*proto.EntrySizeInBytes+proto.IntSizeInBytes))
		result = append(result, proto.NewPair(key, value))
	}
	return result
}

func EncodeEntryListUUIDLong(message *proto.ClientMessage, entries []proto.Pair) {
	size := len(entries)
	content := make([]byte, size*proto.EntrySizeInBytes)
	newFrame := proto.NewFrame(content)
	for i, entry := range entries {
		key := entry.Key().(types.UUID)
		value := entry.Value().(int64)
		FixSizedTypesCodec.EncodeUUID(content, int32(i*proto.EntrySizeInBytes), key)
		FixSizedTypesCodec.EncodeLong(content, int32(i*proto.EntrySizeInBytes+proto.UUIDSizeInBytes), value)
	}
	message.AddFrame(newFrame)
}

func EncodeEntryListIntegerInteger(message *proto.ClientMessage, entries []proto.Pair) {
	size := len(entries)
	content := make([]byte, size*proto.EntrySizeInBytes)
	newFrame := proto.NewFrame(content)
	for i, entry := range entries {
		key := entry.Key().(int32)
		value := entry.Value().(int32)
		FixSizedTypesCodec.EncodeInt(content, int32(i*proto.EntrySizeInBytes), key)
		FixSizedTypesCodec.EncodeInt(content, int32(i*proto.EntrySizeInBytes+proto.UUIDSizeInBytes), value)
	}
	message.AddFrame(newFrame)
}

func DecodeEntryListUUIDLong(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	nextFrame := frameIterator.Next()
	itemCount := len(nextFrame.Content) / proto.EntrySizeInBytes
	content := make([]proto.Pair, itemCount)
	for i := 0; i < itemCount; i++ {
		uuid := FixSizedTypesCodec.DecodeUUID(nextFrame.Content, int32(i*proto.EntrySizeInBytes))
		value := FixSizedTypesCodec.DecodeLong(nextFrame.Content, int32(i*proto.EntrySizeInBytes+proto.UUIDSizeInBytes))
		content[i] = proto.NewPair(uuid, value)
	}
	return content
}

func DecodeEntryListIntegerInteger(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	nextFrame := frameIterator.Next()
	itemCount := len(nextFrame.Content) / proto.EntrySizeInBytes
	content := make([]proto.Pair, itemCount)
	for i := 0; i < itemCount; i++ {
		key := FixSizedTypesCodec.DecodeInt(nextFrame.Content, int32(i*proto.EntrySizeInBytes))
		value := FixSizedTypesCodec.DecodeInt(nextFrame.Content, int32(i*proto.EntrySizeInBytes+proto.IntSizeInBytes))
		content[i] = proto.NewPair(key, value)
	}
	return content
}

func EncodeEntryListUUIDListInteger(message *proto.ClientMessage, entries []proto.Pair) {
	entryCount := len(entries)
	uuids := make([]types.UUID, entryCount)
	message.AddFrame(proto.NewBeginFrame())
	for i := 0; i < entryCount; i++ {
		entry := entries[i]
		key := entry.Key().(types.UUID)
		value := entry.Value().([]int32)
		uuids[i] = key
		EncodeListInteger(message, value)
	}
	message.AddFrame(proto.NewEndFrame())
	EncodeListUUID(message, uuids)
}

func DecodeEntryListUUIDListInteger(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	values := DecodeListMultiFrameWithListInteger(frameIterator)
	keys := DecodeListUUID(frameIterator)
	keySize := len(keys)
	result := make([]proto.Pair, keySize)
	for i := 0; i < keySize; i++ {
		result[i] = proto.NewPair(keys, values)
	}
	return result
}

func DecodeEntryListIntegerUUID(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	frame := frameIterator.Next()
	entryCount := len(frame.Content) / proto.EntrySizeInBytes
	result := make([]proto.Pair, entryCount)
	for i := 0; i < entryCount; i++ {
		key := FixSizedTypesCodec.DecodeInt(frame.Content, int32(i*proto.EntrySizeInBytes))
		value := FixSizedTypesCodec.DecodeUUID(frame.Content, int32(i*proto.EntrySizeInBytes+proto.IntSizeInBytes))
		result[i] = proto.NewPair(key, value)
	}
	return result
}

func DecodeEntryListIntegerLong(iterator *proto.ForwardFrameIterator) []proto.Pair {
	frame := iterator.Next()
	entryCount := len(frame.Content) / proto.EntrySizeInBytes
	result := make([]proto.Pair, entryCount)
	for i := 0; i < entryCount; i++ {
		key := FixSizedTypesCodec.DecodeInt(frame.Content, int32(i*proto.EntrySizeInBytes))
		value := FixSizedTypesCodec.DecodeLong(frame.Content, int32(i*proto.EntrySizeInBytes+proto.IntSizeInBytes))
		result[i] = proto.NewPair(key, value)
	}
	return result
}

// fixSizedTypesCodec
type fixSizedTypesCodec struct{}

var FixSizedTypesCodec fixSizedTypesCodec

func (fixSizedTypesCodec) EncodeInt(buffer []byte, offset, value int32) {
	binary.LittleEndian.PutUint32(buffer[offset:], uint32(value))
}

func (fixSizedTypesCodec) DecodeInt(buffer []byte, offset int32) int32 {
	return int32(binary.LittleEndian.Uint32(buffer[offset:]))
}

func (fixSizedTypesCodec) EncodeLong(buffer []byte, offset int32, value int64) {
	binary.LittleEndian.PutUint64(buffer[offset:], uint64(value))
}

func (fixSizedTypesCodec) DecodeLong(buffer []byte, offset int32) int64 {
	return int64(binary.LittleEndian.Uint64(buffer[offset:]))
}

func (fixSizedTypesCodec) EncodeBoolean(buffer []byte, offset int32, value bool) {
	if value {
		buffer[offset] = 1
	} else {
		buffer[offset] = 0
	}
}

func (fixSizedTypesCodec) DecodeBoolean(buffer []byte, offset int32) bool {
	return buffer[offset] == 1
}

func (fixSizedTypesCodec) EncodeByte(buffer []byte, offset int32, value byte) {
	buffer[offset] = value
}

func (fixSizedTypesCodec) DecodeByte(buffer []byte, offset int32) byte {
	return buffer[offset]
}

func (fixSizedTypesCodec) EncodeUUID(buffer []byte, offset int32, uuid types.UUID) {
	isNullEncode := uuid.Default()
	FixSizedTypesCodec.EncodeBoolean(buffer, offset, isNullEncode)
	if isNullEncode {
		return
	}
	bufferOffset := offset + proto.BooleanSizeInBytes
	FixSizedTypesCodec.EncodeLong(buffer, bufferOffset, int64(uuid.MostSignificantBits()))
	FixSizedTypesCodec.EncodeLong(buffer, bufferOffset+proto.LongSizeInBytes, int64(uuid.LeastSignificantBits()))
}

func (fixSizedTypesCodec) DecodeUUID(buffer []byte, offset int32) types.UUID {
	isNull := FixSizedTypesCodec.DecodeBoolean(buffer, offset)
	if isNull {
		return types.UUID{}
	}

	mostSignificantOffset := offset + proto.BooleanSizeInBytes
	leastSignificantOffset := mostSignificantOffset + proto.LongSizeInBytes
	mostSignificant := uint64(FixSizedTypesCodec.DecodeLong(buffer, mostSignificantOffset))
	leastSignificant := uint64(FixSizedTypesCodec.DecodeLong(buffer, leastSignificantOffset))

	return types.NewUUIDWith(mostSignificant, leastSignificant)
}

func EncodeListInteger(message *proto.ClientMessage, entries []int32) {
	itemCount := len(entries)
	content := make([]byte, itemCount*proto.IntSizeInBytes)
	newFrame := proto.NewFrame(content)
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeInt(newFrame.Content, int32(i*proto.IntSizeInBytes), entries[i])
	}
	message.AddFrame(newFrame)
}

func DecodeListInteger(frameIterator *proto.ForwardFrameIterator) []int32 {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.IntSizeInBytes
	result := make([]int32, itemCount)
	for i := 0; i < itemCount; i++ {
		result[i] = FixSizedTypesCodec.DecodeInt(frame.Content, int32(i*proto.IntSizeInBytes))
	}
	return result
}

func EncodeListLong(message *proto.ClientMessage, entries []int64) {
	itemCount := len(entries)
	frame := proto.NewFrame(make([]byte, itemCount*proto.LongSizeInBytes))
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeLong(frame.Content, int32(i*proto.LongSizeInBytes), entries[i])
	}
	message.AddFrame(frame)
}

func DecodeListLong(frameIterator *proto.ForwardFrameIterator) []int64 {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.LongSizeInBytes
	result := make([]int64, itemCount)
	for i := 0; i < itemCount; i++ {
		result[i] = FixSizedTypesCodec.DecodeLong(frame.Content, int32(i*proto.LongSizeInBytes))
	}
	return result
}

func EncodeListMultiFrame(message *proto.ClientMessage, values []*iserialization.Data, encoder Encoder) {
	message.AddFrame(proto.NewBeginFrame())
	for i := 0; i < len(values); i++ {
		encoder(message, values[i])
	}
	message.AddFrame(proto.NewEndFrame())
}

func EncodeListMultiFrameForData(message *proto.ClientMessage, values []*iserialization.Data) {
	message.AddFrame(proto.NewBeginFrame())
	for i := 0; i < len(values); i++ {
		EncodeData(message, values[i])
	}
	message.AddFrame(proto.NewEndFrame())
}

func EncodeListMultiFrameForString(message *proto.ClientMessage, values []string) {
	message.AddFrame(proto.NewBeginFrame())
	for i := 0; i < len(values); i++ {
		EncodeString(message, values[i])
	}
	message.AddFrame(proto.NewEndFrame())
}

func EncodeListMultiFrameForStackTraceElement(message *proto.ClientMessage, values []ihzerrors.StackTraceElement) {
	message.AddFrame(proto.NewBeginFrame())
	for i := 0; i < len(values); i++ {
		EncodeStackTraceElement(message, values[i])
	}
	message.AddFrame(proto.NewEndFrame())
}

func EncodeListMultiFrameContainsNullable(message *proto.ClientMessage, values []*iserialization.Data, encoder Encoder) {
	message.AddFrame(proto.NewBeginFrame())
	for i := 0; i < len(values); i++ {
		if values[i] == nil {
			message.AddFrame(proto.NullFrame)
		} else {
			encoder(message, values[i])
		}
	}
	message.AddFrame(proto.NewEndFrame())
}

func EncodeListMultiFrameNullable(message *proto.ClientMessage, values []*iserialization.Data, encoder Encoder) {
	if len(values) == 0 {
		message.AddFrame(proto.NullFrame)
	} else {
		EncodeListMultiFrame(message, values, encoder)
	}
}

func DecodeListMultiFrame(frameIterator *proto.ForwardFrameIterator, decoder func(frameIterator *proto.ForwardFrameIterator)) {
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		decoder(frameIterator)
	}
	frameIterator.Next()
}

func DecodeListMultiFrameForData(frameIterator *proto.ForwardFrameIterator) []*iserialization.Data {
	result := make([]*iserialization.Data, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeData(frameIterator))
	}
	frameIterator.Next()
	return result
}

func DecodeListMultiFrameWithListInteger(frameIterator *proto.ForwardFrameIterator) []int32 {
	result := make([]int32, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeListInteger(frameIterator)...)
	}
	frameIterator.Next()
	return result
}

func DecodeListMultiFrameForMemberInfo(frameIterator *proto.ForwardFrameIterator) []pubcluster.MemberInfo {
	result := make([]pubcluster.MemberInfo, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeMemberInfo(frameIterator))
	}
	frameIterator.Next()
	return result
}

func DecodeListMultiFrameForStackTraceElement(frameIterator *proto.ForwardFrameIterator) []ihzerrors.StackTraceElement {
	var result []ihzerrors.StackTraceElement
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeStackTraceElement(frameIterator))
	}
	frameIterator.Next()
	return result
}

func DecodeListMultiFrameForString(frameIterator *proto.ForwardFrameIterator) []string {
	result := make([]string, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeString(frameIterator))
	}
	frameIterator.Next()
	return result
}

func DecodeListMultiFrameForDataContainsNullable(frameIterator *proto.ForwardFrameIterator) []*iserialization.Data {
	result := make([]*iserialization.Data, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		if CodecUtil.NextFrameIsNullFrame(frameIterator) {
			result = append(result, nil)
		} else {
			result = append(result, DecodeData(frameIterator))
		}
	}
	frameIterator.Next()
	return result
}

func DecodeListMultiFrameForDistributedObjectInfo(frameIterator *proto.ForwardFrameIterator) []types.DistributedObjectInfo {
	var result []types.DistributedObjectInfo
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeDistributedObjectInfo(frameIterator))
	}
	frameIterator.Next()
	return result
}

func DecodeDistributedObjectInfo(frameIterator *proto.ForwardFrameIterator) types.DistributedObjectInfo {
	frameIterator.Next()
	serviceName := DecodeString(frameIterator)
	name := DecodeString(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)
	return types.DistributedObjectInfo{Name: name, ServiceName: serviceName}
}

func EncodeListData(message *proto.ClientMessage, entries []*iserialization.Data) {
	EncodeListMultiFrameForData(message, entries)
}

func DecodeListData(frameIterator *proto.ForwardFrameIterator) []*iserialization.Data {
	return DecodeListMultiFrameForData(frameIterator)
}

func EncodeListUUID(message *proto.ClientMessage, entries []types.UUID) {
	itemCount := len(entries)
	content := make([]byte, itemCount*proto.UUIDSizeInBytes)
	newFrame := proto.NewFrame(content)
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeUUID(content, int32(i*proto.UUIDSizeInBytes), entries[i])
	}
	message.AddFrame(newFrame)
}

func DecodeListUUID(frameIterator *proto.ForwardFrameIterator) []types.UUID {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.UUIDSizeInBytes
	result := make([]types.UUID, itemCount)
	for i := 0; i < itemCount; i++ {
		result[i] = FixSizedTypesCodec.DecodeUUID(frame.Content, int32(i*proto.UUIDSizeInBytes))
	}
	return result
}

func EncodeLongArray(message *proto.ClientMessage, entries []int64) {
	itemCount := len(entries)
	frame := proto.NewFrame(make([]byte, itemCount*proto.LongSizeInBytes))
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeLong(frame.Content, int32(i*proto.LongSizeInBytes), entries[i])
	}
	message.AddFrame(frame)
}

func DecodeLongArray(frameIterator *proto.ForwardFrameIterator) []int64 {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.LongSizeInBytes
	result := make([]int64, itemCount)
	for i := 0; i < itemCount; i++ {
		result[i] = FixSizedTypesCodec.DecodeLong(frame.Content, int32(i*proto.LongSizeInBytes))
	}
	return result
}

func EncodeMapForStringAndString(message *proto.ClientMessage, values map[string]string) {
	message.AddFrame(proto.BeginFrame.Copy())
	for key, value := range values {
		EncodeString(message, key)
		EncodeString(message, value)
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func EncodeMapForEndpointQualifierAndAddress(message *proto.ClientMessage, values map[pubcluster.EndpointQualifier]pubcluster.Address) {
	message.AddFrame(proto.BeginFrame.Copy())
	for key, value := range values {
		EncodeEndpointQualifier(message, key)
		EncodeAddress(message, value)
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func DecodeMapForStringAndString(iterator *proto.ForwardFrameIterator) map[string]string {
	result := map[string]string{}
	iterator.Next()
	for !iterator.PeekNext().IsEndFrame() {
		key := DecodeString(iterator)
		value := DecodeString(iterator)
		result[key] = value
	}
	iterator.Next()
	return result
}

func DecodeMapForEndpointQualifierAndAddress(iterator *proto.ForwardFrameIterator) interface{} {
	result := map[pubcluster.EndpointQualifier]pubcluster.Address{}
	iterator.Next()
	for !iterator.PeekNext().IsEndFrame() {
		key := DecodeEndpointQualifier(iterator)
		value := DecodeAddress(iterator)
		result[key] = value
	}
	iterator.Next()
	return result
}

func EncodeString(message *proto.ClientMessage, value interface{}) {
	message.AddFrame(proto.NewFrame([]byte(value.(string))))
}

func DecodeString(frameIterator *proto.ForwardFrameIterator) string {
	return string(frameIterator.Next().Content)
}

func DecodeError(msg *proto.ClientMessage) *ihzerrors.ServerError {
	frameIterator := msg.FrameIterator()
	frameIterator.Next()
	errorHolders := []proto.ErrorHolder{}
	DecodeListMultiFrame(frameIterator, func(it *proto.ForwardFrameIterator) {
		errorHolders = append(errorHolders, DecodeErrorHolder(frameIterator))
	})
	if len(errorHolders) == 0 {
		return nil
	}
	holder := errorHolders[0]
	return ihzerrors.NewServerError(holder.ErrorCode, holder.ClassName, holder.Message, holder.StackTraceElements)
}

func NewEndpointQualifier(qualifierType int32, identifier string) pubcluster.EndpointQualifier {
	return pubcluster.EndpointQualifier{
		Type:       pubcluster.EndpointQualifierType(qualifierType),
		Identifier: identifier,
	}
}

// DistributedObject is the base interface for all distributed objects.
type DistributedObject interface {
	// Destroy destroys this object cluster-wide.
	// Destroy clears and releases all resources for this object.
	Destroy() (bool, error)

	// Name returns the unique name for this DistributedObject.
	Name() string

	// PartitionKey returns the key of partition this DistributedObject is assigned to. The returned value only has meaning
	// for a non partitioned data structure like an IAtomicLong. For a partitioned data structure like an Map
	// the returned value will not be nil, but otherwise undefined.
	PartitionKey() string

	// ServiceName returns the service name for this object.
	ServiceName() string
}

func NewMemberVersion(major, minor, patch byte) pubcluster.MemberVersion {
	return pubcluster.MemberVersion{Major: major, Minor: minor, Patch: patch}
}

func NewMemberInfo(
	address pubcluster.Address,
	uuid types.UUID,
	attributes map[string]string,
	liteMember bool,
	version pubcluster.MemberVersion,
	addressMapExists bool,
	addressMap interface{}) pubcluster.MemberInfo {
	var addrMap map[pubcluster.EndpointQualifier]pubcluster.Address
	if addressMapExists {
		addrMap = addressMap.(map[pubcluster.EndpointQualifier]pubcluster.Address)
	} else {
		addrMap = map[pubcluster.EndpointQualifier]pubcluster.Address{}
	}
	return pubcluster.MemberInfo{
		Address:    address,
		UUID:       uuid,
		Attributes: attributes,
		LiteMember: liteMember,
		Version:    version,
		AddressMap: addrMap,
	}
}

func EncodeAddress(clientMessage *proto.ClientMessage, address pubcluster.Address) {
	host, portStr, err := net.SplitHostPort(address.String())
	if err != nil {
		panic(fmt.Errorf("parsing address: %w", err))
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(fmt.Errorf("parsing address: %w", err))
	}
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, AddressCodecPortInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, AddressCodecPortFieldOffset, int32(port))
	clientMessage.AddFrame(initialFrame)
	EncodeString(clientMessage, host)
	clientMessage.AddFrame(proto.EndFrame.Copy())
}
