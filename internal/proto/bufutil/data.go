package bufutil

import (
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

type DataCodec struct {
}

func DataCodecEncode(iterator *ClientMessagex, data serialization.Data)  {
	iterator.Add(Frame{data.Buffer()})
}


func DataCodecDecodeFrame(iterator *ForwardFrameIterator)  serialization.Data {
	return spi.NewData(iterator.Next().Content)
}
