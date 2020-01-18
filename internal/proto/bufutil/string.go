package bufutil

type StringCodec struct {

}

func StringCodecEncode(iterator *ClientMessagex, value interface{})  {
	iterator.Add(&Frame{Content:[]byte(value.(string))}) //value.getBytes(Bits.UTF_8)
}
//TODO
func StringCodecDecode(iterator *ForwardFrameIterator) interface{}  { //
	return string(iterator.Next().Content)
}

