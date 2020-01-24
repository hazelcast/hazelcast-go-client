package bufutil

type StringCodec struct {

}

func StringCodecEncode(iterator *ClientMessage, value interface{})  {
	iterator.Add(&Frame{content:[]byte(value.(string))}) //value.getBytes(Bits.UTF_8)
}
//TODO
func StringCodecDecode(iterator *ForwardFrameIterator) string { //
	return string(iterator.Next().content)
}

