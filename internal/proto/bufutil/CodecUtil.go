package bufutil

type CodecUtil struct {

}

type Block struct {
	Try     func()
	Finally func()
}

func (tcf Block) Do() {
	if tcf.Finally != nil {

		defer tcf.Finally()
	}
	tcf.Try()
}

func FastForwardToEndFrame(frame *Frame) { //ListIterator<ClientMessage.Frame>
	for !frame.next.IsEndFrame() {
		frame = frame.next
	}
}

type T CodecUtil //codecutil degil yaniiiii

func EncodeNullable(messagex ClientMessagex,value T /*,encode [][ClientMessagex,T]*/) {
	if (value == T(nil)) {
		messagex.Add(NullFrame);
	} else {
		//Encode(messagex,value)
		//encode.accept(clientMessage, value);
	}
}

func DecodeNullable(frame Frame /*,decode [][ClientMessagex,T]*/)  { //T
	if NextFrameIsNullEndFrame(frame) {
		return
	}else{
		return
	}
	//return NextFrameIsNullEndFrame(frame) ? null : decode.apply(iterator);
}

func NextFrameIsDataStructureEndFrame(frame Frame) bool {
	var output bool
	Block{
		Try: func() {
			output = frame.next.IsEndFrame()
		},
		Finally: func() {
			//frame.previous()
		},
	}.Do()
	return output
}

func NextFrameIsNullEndFrame(frame Frame) bool {
	isNull := frame.next.IsNullFrame()
	if !isNull {
		//frame.previous()
	}
	return isNull
}
