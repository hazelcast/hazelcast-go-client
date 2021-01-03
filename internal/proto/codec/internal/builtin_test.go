package internal

import (
	"encoding/binary"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/stretchr/testify/assert"
)

func TestCodecUtil_FastForwardToEndFrame(t *testing.T) {
	// given
	frame1 := proto.NewFrameWith([]byte("value-1"), proto.BeginDataStructureFlag)
	frame2 := proto.NewFrameWith([]byte("value-2"), proto.EndDataStructureFlag)
	frame3 := proto.NewFrameWith([]byte("value-3"), proto.EndDataStructureFlag)

	message := proto.NewClientMessage(frame1)
	message.AddFrame(frame2)
	message.AddFrame(frame3)

	//when
	iterator := message.FrameIterator()
	CodecUtil.FastForwardToEndFrame(iterator)

	//then
	assert.False(t, iterator.HasNext())
	println(message.GetCorrelationID())
}

func TestCodecUtil_EncodeNullable(t *testing.T) {
	//given
	frame1 := proto.NewFrame([]byte("value-0"))
	message := proto.NewClientMessage(frame1)

	//when
	CodecUtil.EncodeNullable(message, "encode-value-1", StringCodec.Encode)

	//then
	iterator := message.FrameIterator()
	assert.Equal(t, string(iterator.Next().Content), "value-0")
	assert.Equal(t, string(iterator.Next().Content), "encode-value-1")
	assert.Equal(t, string(message.EndFrame.Content), "encode-value-1")
}

func TestCodecUtil_NextFrameIsDataStructureEndFrame(t *testing.T) {
	//given
	byteValue := []byte("value-0")
	flags := int32(proto.EndDataStructureFlag)
	frame := proto.NewFrameWith(byteValue, flags)
	message := proto.NewClientMessage(frame)
	//when
	frameIterator := message.FrameIterator()
	isEndFrame := CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator)

	//then
	assert.True(t, isEndFrame)
}

func TestCodecUtil_NextFrameIsNullFrame(t *testing.T) {
	//given
	nullFrame := proto.NullFrame
	message := proto.NewClientMessage(nullFrame)
	frame := proto.NewFrame([]byte("value-0"))
	message.AddFrame(frame)
	frameIterator := message.FrameIterator()

	//when
	isNextFrameIsNull := CodecUtil.NextFrameIsNullFrame(frameIterator)

	//then
	assert.True(t, isNextFrameIsNull)
}

func TestCodecUtil_NextFrameIsNullFrame_Return_False_When_Next_Frame_Is_Not_Null(t *testing.T) {
	//given
	frame1 := proto.NewFrame([]byte("value-1"))
	frame2 := proto.NewFrame([]byte("value-2"))
	message := proto.NewClientMessage(frame1)
	message.AddFrame(frame2)
	frameIterator := message.FrameIterator()

	//when
	isNextFrameIsNull := CodecUtil.NextFrameIsNullFrame(frameIterator)

	//then
	assert.False(t, isNextFrameIsNull)
}

func TestByteArrayCodec_Encode(t *testing.T) {
	//given
	value := []byte("value-1")
	message := proto.NewClientMessageForEncode()

	//when
	ByteArrayCodec.Encode(message, value)

	//then
	iterator := message.FrameIterator()
	assert.Equal(t, string(iterator.Next().Content), "value-1")
}

func TestByteArrayCodec_Decode(t *testing.T) {
	//given
	value := []byte("value-1")
	message := proto.NewClientMessage(proto.NewFrame(value))

	//when
	decode := ByteArrayCodec.Decode(message.FrameIterator())

	//then
	assert.Equal(t, string(decode), "value-1")
}

func TestCodecUtil_EncodeNullable_If_Value_Is_Null_Add_Null_Frame_To_Message(t *testing.T) {
	//given
	frame1 := proto.NewFrame([]byte("value-0"))
	message := proto.NewClientMessage(frame1)

	//when
	CodecUtil.EncodeNullable(message, nil, StringCodec.Encode)

	//then
	iterator := message.FrameIterator()
	assert.Equal(t, string(iterator.Next().Content), "value-0")
	assert.True(t, iterator.Next().IsNullFrame())
}

func TestDataCodec_EncodeNullable_When_Data_Is_Nil(t *testing.T) {
	//given
	message := proto.NewClientMessageForEncode()

	//when
	DataCodec.EncodeNullable(message, nil)

	//then
	iterator := message.FrameIterator()
	assert.Len(t, iterator.Next().Content, 0)
}

func TestDataCodec_Decode(t *testing.T) {
	//given
	bytes := []byte("value-0")
	frame := proto.NewFrame(bytes)
	message := proto.NewClientMessageForDecode(frame)
	frameIterator := message.FrameIterator()

	//when
	decode := DataCodec.Decode(frameIterator)

	//then
	assert.Equal(t, decode.ToByteArray(), bytes)
}

func TestDataCodec_Decode_When_Data_Is_Nil(t *testing.T) {
	//given
	nullFrame := proto.NullFrame
	message := proto.NewClientMessage(nullFrame)
	frameIterator := message.FrameIterator()

	//when
	decode := DataCodec.DecodeNullable(frameIterator)

	//then
	assert.Nil(t, decode)
}

func TestEntryListCodec_Encode(t *testing.T) {
	//given
	bytes := []byte("value-0")
	frame := proto.NewFrame(bytes)
	message := proto.NewClientMessageForDecode(frame)
	pairs := make([]proto.Pair, 0)
	pairs = append(pairs, proto.NewPair("key", "value"))

	//when
	EntryListCodec.Encode(message, pairs, StringCodec.Encode, StringCodec.Encode)

	//then
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
	//given
	bytes := []byte("value-0")
	frame := proto.NewFrame(bytes)
	message := proto.NewClientMessageForDecode(frame)
	pairs := make([]proto.Pair, 0)

	//when
	EntryListCodec.Encode(message, pairs, StringCodec.Encode, StringCodec.Encode)

	//then
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
	//given
	bytes := []byte("value-0")
	frame := proto.NewFrame(bytes)
	message := proto.NewClientMessageForDecode(frame)
	pairs := make([]proto.Pair, 0)
	pairs = append(pairs, proto.NewPair("key", "value"))

	//when
	EntryListCodec.EncodeNullable(message, pairs, StringCodec.Encode, StringCodec.Encode)

	//then
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
	//given
	message := proto.NewClientMessageForEncode()
	pairs := make([]proto.Pair, 0)

	//when
	EntryListCodec.EncodeNullable(message, pairs, StringCodec.Encode, StringCodec.Encode)

	//then
	frameIterator := message.FrameIterator()
	assert.True(t, frameIterator.Next().IsNullFrame())
}

func TestEntryListCodec_DecodeNullable(t *testing.T) {
	//given
	nullFrame := proto.NullFrame
	message := proto.NewClientMessageForDecode(nullFrame)
	iterator := message.FrameIterator()

	//when
	results := EntryListCodec.DecodeNullable(iterator, DataCodec.Decode, DataCodec.Decode)

	//then
	assert.Nil(t, results)
}

func TestEntryListCodec_DecodeNullable_When_Next_Frame_Is_Null_Frame(t *testing.T) {
	//given
	message := proto.NewClientMessageForDecode(proto.NullFrame)
	iterator := message.FrameIterator()

	//when
	results := EntryListCodec.DecodeNullable(iterator, DataCodec.Decode, DataCodec.Decode)

	//then
	assert.Empty(t, results)
}

func TestFixSizedTypesCodec_EncodeInt(t *testing.T) {
	//given
	buffer := make([]byte, 4)
	offset := int32(0)
	value := int32(100)

	//when
	FixSizedTypesCodec.EncodeInt(buffer, offset, value)

	//then
	assert.Equal(t, binary.LittleEndian.Uint32(buffer), uint32(100))
}

func TestFixSizedTypesCodec_DecodeInt(t *testing.T) {
	//given
	buffer := make([]byte, 4)
	offset := int32(0)
	value := int32(100)
	FixSizedTypesCodec.EncodeInt(buffer, offset, value)

	//when
	decodeInt := FixSizedTypesCodec.DecodeInt(buffer, offset)

	//then
	assert.Equal(t, decodeInt, value)
}

func TestFixSizedTypesCodec_EncodeLong(t *testing.T) {
	//given
	buffer := make([]byte, 8)
	offset := int32(0)
	value := int64(100000000000000000)

	//when
	FixSizedTypesCodec.EncodeLong(buffer, offset, value)

	//then
	assert.Equal(t, int64(binary.LittleEndian.Uint64(buffer[offset:])), value)
}

func TestFixSizedTypesCodec_DecodeLong(t *testing.T) {
	//given
	buffer := make([]byte, 8)
	offset := int32(0)
	value := int64(100000000000000000)
	FixSizedTypesCodec.EncodeLong(buffer, offset, value)

	//when
	decodeLong := FixSizedTypesCodec.DecodeLong(buffer, offset)

	//then
	assert.Equal(t, decodeLong, value)
}

func TestFixSizedTypesCodec_EncodeBool(t *testing.T) {
	//given
	buffer := make([]byte, 1)
	offset := int32(0)

	//when
	FixSizedTypesCodec.EncodeBoolean(buffer, offset, true)

	//then
	assert.True(t, buffer[offset] == 1)
}

func TestFixSizedTypesCodec_EncodeBool_When_Value_Is_False(t *testing.T) {
	//given
	buffer := make([]byte, 1)
	offset := int32(0)

	//when
	FixSizedTypesCodec.EncodeBoolean(buffer, offset, false)

	//then
	assert.True(t, buffer[offset] == 0)
}

func TestFixSizedTypesCodec_EncodeByte(t *testing.T) {
	//given
	buffer := make([]byte, 1)
	offset := int32(0)

	//when
	FixSizedTypesCodec.EncodeByte(buffer, offset, 'b')

	//then
	assert.Equal(t, string(buffer[0]), "b")
}

func TestFixSizedTypesCodec_DecodeByte(t *testing.T) {
	//given
	buffer := make([]byte, 1)
	offset := int32(0)
	FixSizedTypesCodec.EncodeByte(buffer, offset, 'b')

	//when
	decodeByte := FixSizedTypesCodec.DecodeByte(buffer, offset)

	//then
	assert.Equal(t, string(decodeByte), "b")
}

func TestFixSizedTypesCodec_EncodeUUID(t *testing.T) {
	//given
	buffer := make([]byte, proto.UUIDSizeInBytes)
	offset := int32(0)
	uuid := core.NewUUID()

	//when
	FixSizedTypesCodec.EncodeUUID(buffer, offset, uuid)

	//then
	assert.Equal(t, FixSizedTypesCodec.DecodeBoolean(buffer, offset), false)
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(buffer, offset+proto.BooleanSizeInBytes), int64(uuid.GetMostSignificantBits()))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(buffer, offset+proto.BooleanSizeInBytes+proto.LongSizeInBytes), int64(uuid.GetLeastSignificantBits()))
}

func TestFixSizedTypesCodec_EncodeUUID_When_UUID_Is_Nil(t *testing.T) {
	//given
	buffer := make([]byte, proto.UUIDSizeInBytes)
	offset := int32(0)

	//when
	FixSizedTypesCodec.EncodeUUID(buffer, offset, nil)

	//then
	assert.Equal(t, FixSizedTypesCodec.DecodeBoolean(buffer, offset), true)
}

func TestFixSizedTypesCodec_DecodeUUID(t *testing.T) {
	//given
	buffer := make([]byte, proto.UUIDSizeInBytes)
	offset := int32(0)
	uuid := core.NewUUID()
	FixSizedTypesCodec.EncodeUUID(buffer, offset, uuid)

	//when
	decodeUUID := FixSizedTypesCodec.DecodeUUID(buffer, offset)

	//then
	assert.Equal(t, FixSizedTypesCodec.DecodeBoolean(buffer, offset), false)
	assert.Equal(t, uuid.ToString(), decodeUUID.ToString())
	assert.Equal(t, uuid.GetMostSignificantBits(), decodeUUID.GetMostSignificantBits())
	assert.Equal(t, uuid.GetLeastSignificantBits(), decodeUUID.GetLeastSignificantBits())
}

func TestEntryListUUIDLongCodec_Encode(t *testing.T) {
	// given
	message := proto.NewClientMessageForEncode()
	key := core.NewUUID()
	value := int64(100)
	pairs := make([]proto.Pair, 0)
	pairs = append(pairs, proto.NewPair(key, value))
	EntryListUUIDLongCodec.Encode(message, pairs)

	// when
	pairs = EntryListUUIDLongCodec.Decode(message.FrameIterator())

	// then
	frame := pairs[0]
	assert.Equal(t, frame.Key().(core.UUID).ToString(), key.ToString())
	assert.Equal(t, frame.Value().(int64), value)
}

func TestListUUIDCodec_Encode(t *testing.T) {
	// given
	message := proto.NewClientMessageForEncode()
	entries := make([]core.UUID, 0)
	value1 := core.NewUUID()
	value2 := core.NewUUID()
	entries = append(entries, value1, value2)

	// when
	ListUUIDCodec.Encode(message, entries)

	// then
	frame := message.FrameIterator().Next()
	decodeUUID1 := FixSizedTypesCodec.DecodeUUID(frame.Content, 0)
	assert.Equal(t, value1.ToString(), decodeUUID1.ToString())
	decodeUUID2 := FixSizedTypesCodec.DecodeUUID(frame.Content, 17)
	assert.Equal(t, value2.ToString(), decodeUUID2.ToString())
}

func TestListIntegerCodec_Encode(t *testing.T) {
	// given
	clientMessage := proto.NewClientMessageForEncode()
	entries := make([]int32, 0)
	entries = append(entries, 1, 2, 3)

	// when
	ListIntegerCodec.Encode(clientMessage, entries)

	// then
	frame := clientMessage.FrameIterator().Next()
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(frame.Content, 0), int32(1))
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(frame.Content, 4), int32(2))
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(frame.Content, 8), int32(3))
}

func TestListIntegerCodec_Decode(t *testing.T) {
	// given
	clientMessage := proto.NewClientMessageForEncode()
	entries := make([]int32, 0)
	entries = append(entries, 1, 2, 3)
	ListIntegerCodec.Encode(clientMessage, entries)

	// when
	decodeEntries := ListIntegerCodec.Decode(clientMessage.FrameIterator())

	// then
	assert.Equal(t, decodeEntries[0], int32(1))
	assert.Equal(t, decodeEntries[1], int32(2))
	assert.Equal(t, decodeEntries[2], int32(3))
}

func TestListLongCodec_Encode(t *testing.T) {
	// given
	message := proto.NewClientMessageForEncode()
	entries := make([]int64, 0)
	entries = append(entries, 1, 2, 3)

	// when
	ListLongCodec.Encode(message, entries)

	// then
	frame := message.FrameIterator().Next()
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 0), int64(1))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 8), int64(2))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 16), int64(3))
}

func TestListLongCodec_Decode(t *testing.T) {
	// given
	message := proto.NewClientMessageForEncode()
	entries := make([]int64, 0)
	entries = append(entries, 1, 2, 3)
	ListLongCodec.Encode(message, entries)

	// when
	result := ListLongCodec.Decode(message.FrameIterator())

	// then
	assert.Equal(t, result, entries)
}

func TestEntryListUUIDListIntegerCodec_Encode(t *testing.T) {
	// given
	clientMessage := proto.NewClientMessageForEncode()
	key := core.NewUUID()
	value := make([]int32, 0)
	value = append(value, 1, 2, 3)
	pair := proto.NewPair(key, value)
	entries := make([]proto.Pair, 0)
	entries = append(entries, pair)

	// when
	EntryListUUIDListIntegerCodec.Encode(clientMessage, entries)

	// then
	iterator := clientMessage.FrameIterator()
	assert.Equal(t, iterator.Next().IsBeginFrame(), true)
	integerValues := iterator.Next()
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(integerValues.Content, 0), int32(1))
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(integerValues.Content, 4), int32(2))
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(integerValues.Content, 8), int32(3))
	assert.Equal(t, iterator.Next().IsEndFrame(), true)
	uuid := FixSizedTypesCodec.DecodeUUID(iterator.Next().Content, 0)
	assert.Equal(t, uuid.ToString(), key.ToString())
}

func TestEntryListUUIDListIntegerCodec_Decode(t *testing.T) {
	// given
	clientMessage := proto.NewClientMessageForEncode()
	key := core.NewUUID()
	value := make([]int32, 0)
	value = append(value, 1, 2, 3)
	pair := proto.NewPair(key, value)
	entries := make([]proto.Pair, 0)
	entries = append(entries, pair)
	EntryListUUIDListIntegerCodec.Encode(clientMessage, entries)

	// when
	result := EntryListUUIDListIntegerCodec.Decode(clientMessage.FrameIterator())

	// then
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Key().([]core.UUID)[0].ToString(), key.ToString())
	assert.EqualValues(t, result[0].Value().([]int32), value)
}

func TestLongArrayCodec_Encode(t *testing.T) {
	// given
	clientMessage := proto.NewClientMessageForEncode()
	entries := make([]int64, 0)
	entries = append(entries, 1, 2, 3)

	// when
	LongArrayCodec.Encode(clientMessage, entries)

	// then
	frame := clientMessage.FrameIterator().Next()
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 0), int64(1))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 8), int64(2))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 16), int64(3))
}

func TestLongArrayCodec_Decode(t *testing.T) {
	// given
	clientMessage := proto.NewClientMessageForEncode()
	entries := make([]int64, 0)
	entries = append(entries, 1, 2, 3)
	LongArrayCodec.Encode(clientMessage, entries)

	// when
	result := LongArrayCodec.Decode(clientMessage.FrameIterator())

	// then
	assert.Equal(t, result[0], int64(1))
	assert.Equal(t, result[1], int64(2))
	assert.Equal(t, result[2], int64(3))
}

func TestStringCodec_Encode(t *testing.T) {
	//given

	value := "value-encode"
	frame := proto.NewFrame([]byte(""))
	clientMessage := proto.NewClientMessage(frame)

	//when
	StringCodec.Encode(clientMessage, value)

	//then
	content := clientMessage.EndFrame.Content
	assert.Equal(t, value, string(content))
}
