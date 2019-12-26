package bufutil

type StringCodec struct {

}

func StringCodecEncode(iterator *ClientMessagex, value string)  {
	iterator.Add(&Frame{[]byte(value)}) //value.getBytes(Bits.UTF_8)
}
//TODO
func StringCodecDecode(iterator *ForwardFrameIterator) string  { //
	return string(iterator.Next().Content)
}

