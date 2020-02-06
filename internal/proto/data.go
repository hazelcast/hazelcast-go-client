package proto

import (
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

type DataCodec struct {
}

func DataCodecEncode(iterator *ClientMessage, data interface{})  {
	iterator.Add(&Frame{Content: data.(serialization.Data).Buffer()})
}


func DataCodecDecode(iterator *ForwardFrameIterator) serialization.Data {
	return spi.NewData(iterator.Next().Content)
}
