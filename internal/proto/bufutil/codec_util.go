package bufutil

type CodecUtil struct {

}
//TODO: check
func FastForwardToEndFrame(iterator *ForwardFrameIterator){
	// We are starting from 1 because of the BEGIN_FRAME we read
	// in the beginning of the decode method
	numberOfExpectedEndFrames := 1
	var frame *Frame
	for numberOfExpectedEndFrames != 0 {
		frame = iterator.Next()
		if frame.IsEndFrame() {
			numberOfExpectedEndFrames--
		} else if frame.IsBeginFrame() {
			numberOfExpectedEndFrames++
		}
	}
}

func EncodeNullable(clientMessage *ClientMessagex, T interface{}, encodeFunction func(messagex *ClientMessagex, T interface{})) {
	if T == nil {
		clientMessage.Add(NullFrame.Copy())
	} else {
		encodeFunction(clientMessage, T)
	}
}

func NextFrameIsDataStructureEndFrame(iterator *ForwardFrameIterator) bool {
	return iterator.PeekNext().IsEndFrame()
}

func NextFrameIsNullEndFrame(iterator *ForwardFrameIterator) bool {
	isNull := iterator.PeekNext().IsNullFrame()
	if isNull {
		iterator.Next()
	}
	return isNull
}

func DecodeNullable(iterator *ForwardFrameIterator, decodeFunction func(itFrame *ForwardFrameIterator) interface{} ) interface{} {
	if NextFrameIsDataStructureEndFrame(iterator) == false {
		return decodeFunction(iterator) //TODO: return check
	}else {
		return nil
	}
}

