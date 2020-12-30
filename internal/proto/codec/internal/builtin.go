package internal

import (
	"encoding/binary"
	"strings"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"

	"github.com/hazelcast/hazelcast-go-client/serialization"

	"github.com/hazelcast/hazelcast-go-client/config"
)

// Encoder for ClientMessage and value
type Encoder func(message *proto.ClientMessage, value interface{})

// Decoder create serialization.Data
type Decoder func(frameIterator *proto.ForwardFrameIterator) serialization.Data

// CodecUtil
type codecUtil struct{}

var CodecUtil codecUtil

func (codecUtil) FastForwardToEndFrame(frameIterator *proto.ForwardFrameIterator) {
	numberOfExpectedEndFrames := 1
	var frame *proto.Frame
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
		StringCodec.Encode(message, value)
	}
}

func (codecUtil) EncodeNullableForBitmapIndexOptions(message *proto.ClientMessage, options config.BitmapIndexOptions) {
	if options == nil {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		BitmapIndexOptionsCodec.Encode(message, options)
	}
}

func (codecUtil) EncodeNullableForData(message *proto.ClientMessage, data serialization.Data) {
	if data == nil {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		DataCodec.Encode(message, data)
	}
}

func (codecUtil) DecodeNullableForData(frameIterator *proto.ForwardFrameIterator) serialization.Data {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DataCodec.Decode(frameIterator)
}

func (codecUtil) DecodeNullableForAddress(frameIterator *proto.ForwardFrameIterator) core.Address {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return AddressCodec.Decode(frameIterator)
}

func (codecUtil) DecodeNullableForLongArray(frameIterator *proto.ForwardFrameIterator) []int64 {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return LongArrayCodec.Decode(frameIterator)
}

func (codecUtil) DecodeNullableForString(frameIterator *proto.ForwardFrameIterator) string {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return ""
	}
	return StringCodec.Decode(frameIterator)
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

func (codecUtil) DecodeNullableForBitmapIndexOptions(frameIterator *proto.ForwardFrameIterator) config.BitmapIndexOptions {
	isNullFrame := frameIterator.PeekNext().IsNullFrame()
	if isNullFrame {
		frameIterator.Next()
	}
	return BitmapIndexOptionsCodec.Decode(frameIterator)
}

func (codecUtil) DecodeNullableForSimpleEntryView(frameIterator *proto.ForwardFrameIterator) core.SimpleEntryView {
	isNullFrame := frameIterator.PeekNext().IsNullFrame()
	if isNullFrame {
		frameIterator.Next()
	}
	return SimpleEntryViewCodec.Decode(frameIterator)
}

// ByteArrayCodec
type byteArrayCodec struct{}

var ByteArrayCodec byteArrayCodec

func (byteArrayCodec) Encode(message *proto.ClientMessage, value []byte) {
	message.AddFrame(proto.NewFrame(value))
}

func (byteArrayCodec) Decode(frameIterator *proto.ForwardFrameIterator) []byte {
	return frameIterator.Next().Content
}

// DataCodec
type dataCodec struct{}

var DataCodec dataCodec

func (dataCodec) Encode(message *proto.ClientMessage, value interface{}) {
	message.AddFrame(proto.NewFrame(value.(serialization.Data).ToByteArray()))
}

func (dataCodec) EncodeNullable(message *proto.ClientMessage, data serialization.Data) {
	if data == nil {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		message.AddFrame(proto.NewFrame(data.ToByteArray()))
	}
}

func (dataCodec) Decode(frameIterator *proto.ForwardFrameIterator) serialization.Data {
	return spi.NewData(frameIterator.Next().Content)
}

func (dataCodec) DecodeNullable(frameIterator *proto.ForwardFrameIterator) serialization.Data {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DataCodec.Decode(frameIterator)
}

//EntryListCodec
type entryListCodec struct{}

var EntryListCodec entryListCodec

func (entryListCodec) Encode(message *proto.ClientMessage, entries []proto.Pair, keyEncoder, valueEncoder Encoder) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		keyEncoder(message, value.Key())
		valueEncoder(message, value.Value())
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func (entryListCodec) EncodeForStringAndString(message *proto.ClientMessage, entries []proto.Pair) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		StringCodec.Encode(message, value.Key())
		StringCodec.Encode(message, value.Value())
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func (entryListCodec) EncodeForStringAndByteArray(message *proto.ClientMessage, entries []proto.Pair) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		StringCodec.Encode(message, value.Key())
		ByteArrayCodec.Encode(message, value.Value().([]byte))
	}
	message.AddFrame(proto.EndFrame.Copy())

}

func (entryListCodec) EncodeForDataAndData(message *proto.ClientMessage, entries []proto.Pair) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		DataCodec.Encode(message, value.Key())
		DataCodec.Encode(message, value.Value())
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func (entryListCodec) EncodeForDataAndListData(message *proto.ClientMessage, entries []proto.Pair) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		DataCodec.Encode(message, value.Key())
		ListDataCodec.Encode(message, value.Value().([]serialization.Data))
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func (entryListCodec) EncodeNullable(message *proto.ClientMessage, entries []proto.Pair, keyEncoder, valueEncoder Encoder) {
	if len(entries) == 0 {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		EntryListCodec.Encode(message, entries, keyEncoder, valueEncoder)
	}
}

func (entryListCodec) Decode(frameIterator *proto.ForwardFrameIterator, keyDecoder, valueDecoder Decoder) []proto.Pair {
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

func (entryListCodec) DecodeNullable(frameIterator *proto.ForwardFrameIterator, keyDecoder, valueDecoder Decoder) []proto.Pair {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return EntryListCodec.Decode(frameIterator, keyDecoder, valueDecoder)
}

func (entryListCodec) DecodeForStringAndEntryListIntegerLong(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	result := make([]proto.Pair, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		key := StringCodec.Decode(frameIterator)
		value := EntryListIntegerLongCodec.Decode(frameIterator)
		result = append(result, proto.NewPair(key, value))
	}
	frameIterator.Next()
	return result
}

func (entryListCodec) DecodeForDataAndData(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	result := make([]proto.Pair, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		key := DataCodec.Decode(frameIterator)
		value := DataCodec.Decode(frameIterator)
		result = append(result, proto.NewPair(key, value))
	}
	frameIterator.Next()
	return result
}

//EntryListIntegerIntegerCodec
type entryListIntegerIntegerCodec struct{}

var EntryListIntegerIntegerCodec entryListIntegerIntegerCodec

func (entryListIntegerIntegerCodec) Encode(message *proto.ClientMessage, entries []proto.Pair) {
	entryCount := len(entries)
	frame := proto.NewFrame(make([]byte, entryCount*proto.EntrySizeInBytes))
	for i := 0; i < entryCount; i++ {
		FixSizedTypesCodec.EncodeInt(frame.Content, int32(i*proto.EntrySizeInBytes), entries[i].Key().(int32))
		FixSizedTypesCodec.EncodeInt(frame.Content, int32(i*proto.EntrySizeInBytes+proto.IntSizeInBytes), entries[i].Value().(int32))
	}
	message.AddFrame(frame)
}

func (entryListIntegerIntegerCodec) Decode(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
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

// EntryListUUIDLongCodec
type entryListUUIDLongCodec struct{}

var EntryListUUIDLongCodec entryListUUIDLongCodec

func (entryListUUIDLongCodec) Encode(message *proto.ClientMessage, entries []proto.Pair) {
	size := len(entries)
	content := make([]byte, size*proto.EntrySizeInBytes)
	newFrame := proto.NewFrame(content)
	for i, entry := range entries {
		key := entry.Key().(core.UUID)
		value := entry.Value().(int64)
		FixSizedTypesCodec.EncodeUUID(content, int32(i*proto.EntrySizeInBytes), key)
		FixSizedTypesCodec.EncodeLong(content, int32(i*proto.EntrySizeInBytes+proto.UUIDSizeInBytes), value)
	}
	message.AddFrame(newFrame)
}

func (entryListUUIDLongCodec) Decode(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
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

// EntryListUUIDListIntegerCodec
type entryListUUIDListIntegerCodec struct{}

var EntryListUUIDListIntegerCodec entryListUUIDListIntegerCodec

func (entryListUUIDListIntegerCodec) Encode(message *proto.ClientMessage, entries []proto.Pair) {
	entryCount := len(entries)
	uuids := make([]core.UUID, entryCount)
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < entryCount; i++ {
		entry := entries[i]
		key := entry.Key().(core.UUID)
		value := entry.Value().([]int32)
		uuids[i] = key
		ListIntegerCodec.Encode(message, value)
	}
	message.AddFrame(proto.EndFrame)
	ListUUIDCodec.Encode(message, uuids)
}

func (entryListUUIDListIntegerCodec) Decode(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	values := ListMultiFrameCodec.DecodeWithListInteger(frameIterator)
	keys := ListUUIDCodec.Decode(frameIterator)
	keySize := len(keys)
	result := make([]proto.Pair, keySize)
	for i := 0; i < keySize; i++ {
		result[i] = proto.NewPair(keys, values)
	}
	return result
}

type entryListIntegerUUIDCodec struct{}

var EntryListIntegerUUIDCodec entryListIntegerUUIDCodec

func (entryListIntegerUUIDCodec) Decode(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
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

// entryListIntegerUUIDCodec
type entryListIntegerLongCodec struct{}

var EntryListIntegerLongCodec entryListIntegerLongCodec

func (entryListIntegerLongCodec) Decode(iterator *proto.ForwardFrameIterator) []proto.Pair {
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

func (fixSizedTypesCodec) EncodeUUID(buffer []byte, offset int32, uuid core.UUID) {
	isNullEncode := uuid == nil
	FixSizedTypesCodec.EncodeBoolean(buffer, offset, isNullEncode)
	if isNullEncode {
		return
	}
	bufferOffset := offset + proto.BooleanSizeInBytes
	FixSizedTypesCodec.EncodeLong(buffer, bufferOffset, int64(uuid.GetMostSignificantBits()))
	FixSizedTypesCodec.EncodeLong(buffer, bufferOffset+proto.LongSizeInBytes, int64(uuid.GetLeastSignificantBits()))
}

func (fixSizedTypesCodec) DecodeUUID(buffer []byte, offset int32) core.UUID {
	isNull := FixSizedTypesCodec.DecodeBoolean(buffer, offset)
	if isNull {
		return nil
	}

	mostSignificantOffset := offset + proto.BooleanSizeInBytes
	leastSignificantOffset := mostSignificantOffset + proto.LongSizeInBytes
	mostSignificant := uint64(FixSizedTypesCodec.DecodeLong(buffer, mostSignificantOffset))
	leastSignificant := uint64(FixSizedTypesCodec.DecodeLong(buffer, leastSignificantOffset))

	return core.NewUUIDWith(mostSignificant, leastSignificant)
}

// ListIntegerCodec
type listIntegerCodec struct{}

var ListIntegerCodec listIntegerCodec

func (listIntegerCodec) Encode(message *proto.ClientMessage, entries []int32) {
	itemCount := len(entries)
	content := make([]byte, itemCount*proto.IntSizeInBytes)
	newFrame := proto.NewFrame(content)
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeInt(newFrame.Content, int32(i*proto.IntSizeInBytes), entries[i])
	}
	message.AddFrame(newFrame)
}

func (listIntegerCodec) Decode(frameIterator *proto.ForwardFrameIterator) []int32 {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.IntSizeInBytes
	result := make([]int32, itemCount)
	for i := 0; i < itemCount; i++ {
		result[i] = FixSizedTypesCodec.DecodeInt(frame.Content, int32(i*proto.IntSizeInBytes))
	}
	return result
}

// ListLongCodec
type listLongCodec struct{}

var ListLongCodec listLongCodec

func (listLongCodec) Encode(message *proto.ClientMessage, entries []int64) {
	itemCount := len(entries)
	frame := proto.NewFrame(make([]byte, itemCount*proto.LongSizeInBytes))
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeLong(frame.Content, int32(i*proto.LongSizeInBytes), entries[i])
	}
	message.AddFrame(frame)
}

func (listLongCodec) Decode(frameIterator *proto.ForwardFrameIterator) []int64 {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.LongSizeInBytes
	result := make([]int64, itemCount)
	for i := 0; i < itemCount; i++ {
		result[i] = FixSizedTypesCodec.DecodeLong(frame.Content, int32(i*proto.LongSizeInBytes))
	}
	return result
}

//ListMultiFrameCodec
type listMultiFrameCodec struct{}

var ListMultiFrameCodec listMultiFrameCodec

func (listMultiFrameCodec) Encode(message *proto.ClientMessage, values []serialization.Data, encoder Encoder) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		encoder(message, values[i])
	}
	message.AddFrame(proto.EndFrame)
}

func (listMultiFrameCodec) EncodeForData(message *proto.ClientMessage, values []serialization.Data) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		DataCodec.Encode(message, values[i])
	}
	message.AddFrame(proto.EndFrame)
}

func (c listMultiFrameCodec) EncodeForString(message *proto.ClientMessage, values []string) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		StringCodec.Encode(message, values[i])
	}
	message.AddFrame(proto.EndFrame)
}

func (c listMultiFrameCodec) EncodeForStackTraceElement(message *proto.ClientMessage, values []proto.StackTraceElement) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		StackTraceElementCodec.Encode(message, values[i])
	}
	message.AddFrame(proto.EndFrame)
}

func (listMultiFrameCodec) EncodeContainsNullable(message *proto.ClientMessage, values []serialization.Data, encoder Encoder) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		if values[i] == nil {
			message.AddFrame(proto.NullFrame)
		} else {
			encoder(message, values[i])
		}
	}
	message.AddFrame(proto.EndFrame)
}

func (listMultiFrameCodec) EncodeNullable(message *proto.ClientMessage, values []serialization.Data, encoder Encoder) {
	if len(values) == 0 {
		message.AddFrame(proto.NullFrame)
	} else {
		ListMultiFrameCodec.Encode(message, values, encoder)
	}
}

func (listMultiFrameCodec) DecodeForData(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	result := make([]serialization.Data, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DataCodec.Decode(frameIterator))
	}
	frameIterator.Next()
	return result
}

func (c listMultiFrameCodec) DecodeWithListInteger(frameIterator *proto.ForwardFrameIterator) []int32 {
	result := make([]int32, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, ListIntegerCodec.Decode(frameIterator)...)
	}
	frameIterator.Next()
	return result
}

func (c listMultiFrameCodec) DecodeForDistributedObjectInfo(frameIterator *proto.ForwardFrameIterator) []proto.DistributedObjectInfo {
	result := make([]proto.DistributedObjectInfo, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DistributedObjectInfoCodec.Decode(frameIterator))
	}
	frameIterator.Next()
	return result
}

func (c listMultiFrameCodec) DecodeForMemberInfo(frameIterator *proto.ForwardFrameIterator) []core.MemberInfo {
	result := make([]core.MemberInfo, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, MemberInfoCodec.Decode(frameIterator))
	}
	frameIterator.Next()
	return result
}

func (listMultiFrameCodec) DecodeForStackTraceElement(frameIterator *proto.ForwardFrameIterator) []proto.StackTraceElement {
	result := make([]proto.StackTraceElement, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, StackTraceElementCodec.Decode(frameIterator))
	}
	frameIterator.Next()
	return result
}

func (listMultiFrameCodec) DecodeForString(frameIterator *proto.ForwardFrameIterator) []string {
	result := make([]string, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, StringCodec.Decode(frameIterator))
	}
	frameIterator.Next()
	return result
}

func (listMultiFrameCodec) DecodeForDataContainsNullable(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	result := make([]serialization.Data, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		if CodecUtil.NextFrameIsNullFrame(frameIterator) {
			result = append(result, nil)
		} else {
			result = append(result, DataCodec.Decode(frameIterator))
		}
	}
	frameIterator.Next()
	return result
}

// listDataCodec
type listDataCodec struct{}

var ListDataCodec listDataCodec

func (listDataCodec) Encode(message *proto.ClientMessage, entries []serialization.Data) {
	ListMultiFrameCodec.EncodeForData(message, entries)
}

func (listDataCodec) Decode(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	return ListMultiFrameCodec.DecodeForData(frameIterator)
}

// listUUIDCodec
type listUUIDCodec struct{}

var ListUUIDCodec listUUIDCodec

func (listUUIDCodec) Encode(message *proto.ClientMessage, entries []core.UUID) {
	itemCount := len(entries)
	content := make([]byte, itemCount*proto.UUIDSizeInBytes)
	newFrame := proto.NewFrame(content)
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeUUID(content, int32(i*proto.UUIDSizeInBytes), entries[i])
	}
	message.AddFrame(newFrame)
}

func (listUUIDCodec) Decode(frameIterator *proto.ForwardFrameIterator) []core.UUID {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.UUIDSizeInBytes
	result := make([]core.UUID, itemCount)
	for i := 0; i < itemCount; i++ {
		result[i] = FixSizedTypesCodec.DecodeUUID(frame.Content, int32(i*proto.UUIDSizeInBytes))
	}
	return result
}

// LongArrayCodec
type longArrayCodec struct{}

var LongArrayCodec longArrayCodec

func (longArrayCodec) Encode(message *proto.ClientMessage, entries []int64) {
	itemCount := len(entries)
	frame := proto.NewFrame(make([]byte, itemCount*proto.LongSizeInBytes))
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeLong(frame.Content, int32(i*proto.LongSizeInBytes), entries[i])
	}
	message.AddFrame(frame)
}

func (longArrayCodec) Decode(frameIterator *proto.ForwardFrameIterator) []int64 {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.LongSizeInBytes
	result := make([]int64, itemCount)
	for i := 0; i < itemCount; i++ {
		result[i] = FixSizedTypesCodec.DecodeLong(frame.Content, int32(i*proto.LongSizeInBytes))
	}
	return result
}

// MapCodec
type mapCodec struct{}

var MapCodec mapCodec

func (mapCodec) EncodeForStringAndString(message *proto.ClientMessage, values map[string]string) {
	message.AddFrame(proto.BeginFrame.Copy())
	for key, value := range values {
		StringCodec.Encode(message, key)
		StringCodec.Encode(message, value)
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func (mapCodec) EncodeForEndpointQualifierAndAddress(message *proto.ClientMessage, values map[core.EndpointQualifier]core.Address) {
	message.AddFrame(proto.BeginFrame.Copy())
	for key, value := range values {
		EndpointQualifierCodec.Encode(message, key)
		AddressCodec.Encode(message, value)
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func (c mapCodec) DecodeForStringAndString(iterator *proto.ForwardFrameIterator) map[string]string {
	result := map[string]string{}

	iterator.Next()
	for !iterator.PeekNext().IsEndFrame() {
		key := StringCodec.Decode(iterator)
		value := StringCodec.Decode(iterator)
		result[key] = value
	}
	iterator.Next()

	return result
}

func (c mapCodec) DecodeForEndpointQualifierAndAddress(iterator *proto.ForwardFrameIterator) interface{} {
	result := map[proto.EndpointQualifier]core.Address{}

	iterator.Next()
	for !iterator.PeekNext().IsEndFrame() {
		key := EndpointQualifierCodec.Decode(iterator)
		value := AddressCodec.Decode(iterator)
		result[key] = value
	}
	iterator.Next()

	return result
}

// StringCodec
type stringCodec struct{}

var StringCodec stringCodec

func (stringCodec) Encode(message *proto.ClientMessage, value interface{}) {
	message.AddFrame(proto.NewFrame([]byte(value.(string))))
}

func (stringCodec) Decode(frameIterator *proto.ForwardFrameIterator) string {
	return string(frameIterator.Next().Content)
}
