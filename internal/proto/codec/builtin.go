package codec

import (
	"encoding/binary"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
	"strings"

	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization/spi"
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
		EncodeString(message, value)
	}
}

func (codecUtil) EncodeNullableForBitmapIndexOptions(message *proto.ClientMessage, options hztypes.BitmapIndexOptions) {
	if options.IsDefault() {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		EncodeBitmapIndexOptions(message, options)
	}
}

func (codecUtil) EncodeNullableForData(message *proto.ClientMessage, data serialization.Data) {
	if data == nil {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		EncodeData(message, data)
	}
}

func (codecUtil) DecodeNullableForData(frameIterator *proto.ForwardFrameIterator) serialization.Data {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeData(frameIterator)
}

func (codecUtil) DecodeNullableForAddress(frameIterator *proto.ForwardFrameIterator) pubcluster.Address {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeAddress(frameIterator)
}

func (codecUtil) DecodeNullableForLongArray(frameIterator *proto.ForwardFrameIterator) []int64 {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeLongArray(frameIterator)
}

func (codecUtil) DecodeNullableForString(frameIterator *proto.ForwardFrameIterator) string {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
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

func (codecUtil) DecodeNullableForBitmapIndexOptions(frameIterator *proto.ForwardFrameIterator) hztypes.BitmapIndexOptions {
	isNullFrame := frameIterator.PeekNext().IsNullFrame()
	if isNullFrame {
		frameIterator.Next()
	}
	return DecodeBitmapIndexOptions(frameIterator)
}

func (codecUtil) DecodeNullableForSimpleEntryView(frameIterator *proto.ForwardFrameIterator) *hztypes.SimpleEntryView {
	isNullFrame := frameIterator.PeekNext().IsNullFrame()
	if isNullFrame {
		frameIterator.Next()
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
	message.AddFrame(proto.NewFrame(value.(serialization.Data).ToByteArray()))
}

func EncodeNullableData(message *proto.ClientMessage, data serialization.Data) {
	if data == nil {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		message.AddFrame(proto.NewFrame(data.ToByteArray()))
	}
}

func DecodeData(frameIterator *proto.ForwardFrameIterator) serialization.Data {
	return spi.NewData(frameIterator.Next().Content)
}

func DecodeNullableData(frameIterator *proto.ForwardFrameIterator) serialization.Data {
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
		EncodeListData(message, value.Value().([]serialization.Data))
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
		key := entry.Key().(internal.UUID)
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
	uuids := make([]internal.UUID, entryCount)
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < entryCount; i++ {
		entry := entries[i]
		key := entry.Key().(internal.UUID)
		value := entry.Value().([]int32)
		uuids[i] = key
		EncodeListInteger(message, value)
	}
	message.AddFrame(proto.EndFrame)
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

func (fixSizedTypesCodec) EncodeUUID(buffer []byte, offset int32, uuid internal.UUID) {
	isNullEncode := uuid == nil
	FixSizedTypesCodec.EncodeBoolean(buffer, offset, isNullEncode)
	if isNullEncode {
		return
	}
	bufferOffset := offset + proto.BooleanSizeInBytes
	FixSizedTypesCodec.EncodeLong(buffer, bufferOffset, int64(uuid.MostSignificantBits()))
	FixSizedTypesCodec.EncodeLong(buffer, bufferOffset+proto.LongSizeInBytes, int64(uuid.LeastSignificantBits()))
}

func (fixSizedTypesCodec) DecodeUUID(buffer []byte, offset int32) internal.UUID {
	isNull := FixSizedTypesCodec.DecodeBoolean(buffer, offset)
	if isNull {
		return nil
	}

	mostSignificantOffset := offset + proto.BooleanSizeInBytes
	leastSignificantOffset := mostSignificantOffset + proto.LongSizeInBytes
	mostSignificant := uint64(FixSizedTypesCodec.DecodeLong(buffer, mostSignificantOffset))
	leastSignificant := uint64(FixSizedTypesCodec.DecodeLong(buffer, leastSignificantOffset))

	return internal.NewUUIDWith(mostSignificant, leastSignificant)
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

func EncodeListMultiFrame(message *proto.ClientMessage, values []serialization.Data, encoder Encoder) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		encoder(message, values[i])
	}
	message.AddFrame(proto.EndFrame)
}

func EncodeListMultiFrameForData(message *proto.ClientMessage, values []serialization.Data) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		EncodeData(message, values[i])
	}
	message.AddFrame(proto.EndFrame)
}

func EncodeListMultiFrameForString(message *proto.ClientMessage, values []string) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		EncodeString(message, values[i])
	}
	message.AddFrame(proto.EndFrame)
}

func EncodeListMultiFrameForStackTraceElement(message *proto.ClientMessage, values []proto.StackTraceElement) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		EncodeStackTraceElement(message, values[i])
	}
	message.AddFrame(proto.EndFrame)
}

func EncodeListMultiFrameContainsNullable(message *proto.ClientMessage, values []serialization.Data, encoder Encoder) {
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

func EncodeListMultiFrameNullable(message *proto.ClientMessage, values []serialization.Data, encoder Encoder) {
	if len(values) == 0 {
		message.AddFrame(proto.NullFrame)
	} else {
		EncodeListMultiFrame(message, values, encoder)
	}
}

func DecodeListMultiFrameForData(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	result := make([]serialization.Data, 0)
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

func DecodeListMultiFrameForDistributedObjectInfo(frameIterator *proto.ForwardFrameIterator) []internal.DistributedObjectInfo {
	result := make([]internal.DistributedObjectInfo, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeDistributedObjectInfo(frameIterator))
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

func DecodeListMultiFrameForStackTraceElement(frameIterator *proto.ForwardFrameIterator) []hzerror.StackTraceElement {
	result := make([]hzerror.StackTraceElement, 0)
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

func DecodeListMultiFrameForDataContainsNullable(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	result := make([]serialization.Data, 0)
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

func EncodeListData(message *proto.ClientMessage, entries []serialization.Data) {
	EncodeListMultiFrameForData(message, entries)
}

func DecodeListData(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	return DecodeListMultiFrameForData(frameIterator)
}

func EncodeListUUID(message *proto.ClientMessage, entries []internal.UUID) {
	itemCount := len(entries)
	content := make([]byte, itemCount*proto.UUIDSizeInBytes)
	newFrame := proto.NewFrame(content)
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeUUID(content, int32(i*proto.UUIDSizeInBytes), entries[i])
	}
	message.AddFrame(newFrame)
}

func DecodeListUUID(frameIterator *proto.ForwardFrameIterator) []internal.UUID {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.UUIDSizeInBytes
	result := make([]internal.UUID, itemCount)
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

func EncodeMapForEndpointQualifierAndAddress(message *proto.ClientMessage, values map[internal.EndpointQualifier]pubcluster.Address) {
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
	result := map[internal.EndpointQualifier]pubcluster.Address{}
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
