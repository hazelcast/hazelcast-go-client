package bufutil

type StringCodec struct {

}

func StringCodecEncode(iterator *ClientMessagex, value string)  {
	iterator.Add(Frame{[]byte(value)}) //value.getBytes(Bits.UTF_8)
}

func StringCodecDecode(iterator *Frame) string  { //
	return StringCodecDecodeFrame(*iterator)
}

func StringCodecDecodeFrame(iterator Frame)  string {
	return string(iterator.Content) //charset
}
