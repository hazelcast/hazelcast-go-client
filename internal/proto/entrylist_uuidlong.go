package proto

import (
	"github.com/hazelcast/hazelcast-go-client/core"
)

func EntryListUUIDLongCodec() {

}
const EntrySizeInBytes = UUIDSizeInBytes + LongSizeInBytes
//TODO: WIP
func EntryListUUIDLongCodecEncode(clientMessage *ClientMessage, collection []*Pair)  {
	itemCount := len(collection)
	frame := &Frame{Content: make([]byte, itemCount * EntrySizeInBytes)}

	for i, it := range collection{
		EncodeUUID(frame.Content, int32(i*EntrySizeInBytes), it.Key().(core.Uuid))
		EncodeLong(frame.Content, int32(i*EntrySizeInBytes+UUIDSizeInBytes), it.Value().(int64))
	}

	clientMessage.Add(frame)
}

func EntryListUUIDLongCodecDecode(iterator *ForwardFrameIterator) []*Pair {
	frame := iterator.Next()
	itemCount := len(frame.Content) / EntrySizeInBytes

	var result []*Pair

	for  i := 0; i < itemCount; i++ {
		 key := DecodeUUID(frame.Content, int32(i*EntrySizeInBytes))
		 value := DecodeLong(frame.Content, int32(i*EntrySizeInBytes+UUIDSizeInBytes))
		result[i] = NewPair(key,value)

	}
	return result
}
