package bufutil

type DataCodec struct {
}
//TODO
func DataCodecEncode(iterator *ClientMessage, data interface{})  {
	iterator.Add(&Frame{Content:data.(Data).Payload})
}


func DataCodecDecode(iterator *ForwardFrameIterator) Data {
	return NewData(iterator.Next().Content)
}
