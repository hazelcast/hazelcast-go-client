package bufutil

import (
	"strconv"
)

const (
	TypeFieldOffset               = 0
	CorrelationIdFieldOffset      = TypeFieldOffset + Uint16SizeInBytes
	FragmentationIdOffset         = 0
	ResponseBackupAcksFieldOffset = CorrelationIdFieldOffset + Int64SizeInBytes
	PartitionIdFieldOffset        = CorrelationIdFieldOffset + Int64SizeInBytes
	DefaultFlags                  = 0
	//BeginFragmentFlag             = 1 << 15
	//EndFragmentFlag               = 1 << 14
	//UnfragmentedMessage           = BeginFragmentFlag | EndFragmentFlag
	//IsFinalFlag                   = 1 << 13
	//BeginDataStructureFlag        = 1 << 12
	//EndDataStructureFlag          = 1 << 11
	//IsNullFlag                    = 1 << 10
	//IsEventFlag                   = 1 << 9
	BackupAwareFlag               = 1 << 8
	BackupEventFlag               = 1 << 7

	SizeOfFrameLengthAndFlags = Int32SizeInBytes + Uint16SizeInBytes
)

var BeginFragmentFlag             = 1 << 15
var EndFragmentFlag               = 1 << 14
var UnfragmentedMessage           = uint8(BeginFragmentFlag | EndFragmentFlag)
var IsFinalFlag                   = 1 << 13
var BeginDataStructureFlag        = 1 << 12
var EndDataStructureFlag          = 1 << 11
var IsNullFlag                    = 1 << 10
var IsEventFlag                   = 1 << 9

var serialVersionUID = 1
var NullFrame = &Frame{Content: make([]byte, 0), Flags:uint8(IsNullFlag)}
var BeginFrame = &Frame{Content: make([]byte, 0), Flags:uint8(BeginDataStructureFlag)}
var EndFrame = &Frame{Content: make([]byte, 0), Flags:uint8(EndDataStructureFlag)}

type Frame struct {
	Content []byte
	Flags   uint8
	next    *Frame
}

type ForwardFrameIterator struct {
	nextFrame *Frame //TODO : check
}

type ClientMessage struct {
	startFrame       *Frame
	endFrame         *Frame
	isRetryable      bool
	operationName    string
	//connection       internal.Connection
}

/*

	Constructors

*/

// CLIENT MESSAGE

/*func NewClientMessage(buffer []byte, payloadSize int) *ClientMessage {
	clientMessage := new(ClientMessage)
	if buffer != nil {
		//Message has a buffer so it will be decoded.
		clientMessage.Buffer = buffer
		clientMessage.readIndex = bufutil.HeaderSize
	} else {
		//Client message that will be encoded.
		clientMessage.Buffer = make([]byte, bufutil.HeaderSize+payloadSize)
		clientMessage.SetDataOffset(bufutil.HeaderSize)
		clientMessage.writeIndex = bufutil.HeaderSize
	}
	clientMessage.SetRetryable(false)
	return clientMessage
}*/

func CreateForEncode() *ClientMessage {
	message := new(ClientMessage)
	return message
}

func CreateForDecode(frame *Frame) ClientMessage {
	message := ClientMessage{
		startFrame: frame,
		endFrame:   frame,
	}
	panic(frame.next == nil) //TODO : check
	return message
}

func (m *ClientMessage) StartFrame() *Frame {
	return m.startFrame
}

func (m *ClientMessage) Add(frame *Frame) *ClientMessage {
	frame.next = nil
	if m.startFrame.IsNullFrame() {
		m.startFrame = frame
		m.endFrame = frame
		return m
	}
	m.endFrame.next = frame //TODO : check : check
	m.endFrame = frame
	return m
}

func (m *ClientMessage) FrameIterator() *ForwardFrameIterator {
	message := &ForwardFrameIterator{m.startFrame}
	return message
}

func (m *ClientMessage) MessageType() int32 {
	return ReadInt32(m.startFrame.Content, TypeFieldOffset, false)
}

func (m *ClientMessage) SetMessageType(messageType int32) *ClientMessage {
	WriteInt32(m.startFrame.Content, TypeFieldOffset, messageType, false)
	return m
}

func (m *ClientMessage) CorrelationId() int64 {
	return ReadInt64(m.startFrame.Content, CorrelationIdFieldOffset, false)
}

func (m *ClientMessage) SetCorrelationId(CorrelationId int64) *ClientMessage { //TODO: check
	WriteInt64(m.startFrame.Content, CorrelationIdFieldOffset, CorrelationId, false)
	return m
}

func (m *ClientMessage) NumberOfBackupAcks() int64 {
	return ReadInt64(m.startFrame.Content, ResponseBackupAcksFieldOffset, false)
}

func (m *ClientMessage) SetNumberOfBackupAcks(numberOfAcks int64) *ClientMessage {
	//const TODO numberOfAcks
	WriteInt64(m.startFrame.Content, ResponseBackupAcksFieldOffset, numberOfAcks, false)
	return m
}

func (m *ClientMessage) PartitionId() int32 {
	return ReadInt32(m.startFrame.Content, PartitionIdFieldOffset, false)
}

func (m *ClientMessage) SetPartitionId(partitionId int32) *ClientMessage {
	WriteInt32(m.startFrame.Content, PartitionIdFieldOffset, partitionId, false)
	return m
}

func (m *ClientMessage) HeaderFlags() uint8 {
	return m.startFrame.Flags
}

func (m *ClientMessage) IsRetryable() bool {
	return m.isRetryable
}

func (m *ClientMessage) SetRetryable(isRetryable bool) {
	m.isRetryable = isRetryable
}

func (m *ClientMessage) SetOperationName(operationName string) {
	m.operationName = operationName
}

func (m *ClientMessage) OperationName() string {
	return m.operationName
}

func IsFlagSet(flags uint8, flagMask int) bool {
	i := int(flags) & flagMask
		if i == flagMask{
			return true
		}
	return false
}

func (m *ClientMessage) SetFlags(v uint8) {
	m.startFrame.Content[FlagsFieldOffset] = byte(v) //todo
}

/*
func (m *ClientMessage) SetConnection(connection internal.Connection) {
	m.Connection = connection
}

func (m *ClientMessage) Connection() internal.Connection {
	return m.Connection
}
*/
func (m *ClientMessage) FrameLength() int {
	frameLength := 0
	currentFrame := m.startFrame
	for currentFrame.IsNullFrame() {
		frameLength += currentFrame.Size() //TODO : check
		currentFrame = currentFrame.next     //TODO : check
	}
	return frameLength
}

func (m *ClientMessage) IsUrgent() bool {
	return false
}


func (m *ClientMessage) Merge(fragment *ClientMessage) {
	fragmentMessageStartFrame := fragment.startFrame.next
	m.endFrame.next = fragmentMessageStartFrame
	m.endFrame = fragment.endFrame
}

func (m *ClientMessage) ClientMessageToString() string {
	str := "ClientMessage{\n" + "connection=" //+ m.Connection.String() + "\n"
	if !m.startFrame.IsNullFrame() {
		str += ", length=" + string(m.FrameLength()) + "\n" +
			", correlationId=" + string(m.CorrelationId()) + "\n" +
			", operation=" + m.OperationName() + "\n" +
			", messageType=" + string(m.MessageType()) + "\n" +
			", isRetryable=" + strconv.FormatBool(m.IsRetryable()) + "\n" +
			", isEvent=" + strconv.FormatBool(IsFlagSet(m.startFrame.Flags, IsEventFlag)) + "\n" +
			", isFragmented=" + strconv.FormatBool(!IsFlagSet(m.startFrame.Flags, int(UnfragmentedMessage)))
	}

	str += "}"
	return str
}

func (m *ClientMessage) CopyWithNewCorrelationId(correlationId int64) *ClientMessage { //TODO : check
	initialFrameCopy := m.startFrame.DeepCopy() //TODO : check
	newMessage := &ClientMessage{startFrame:initialFrameCopy, endFrame:m.endFrame}
	newMessage.SetCorrelationId(correlationId)
	newMessage.isRetryable = m.isRetryable
	newMessage.operationName = m.operationName

	return newMessage
}

func (m *ClientMessage) CloneMessage() *ClientMessage {
	copiedMessage := &ClientMessage{startFrame:m.startFrame, endFrame:m.endFrame}
	copiedMessage.isRetryable = m.isRetryable
	copiedMessage.operationName = m.operationName
	return copiedMessage
}


// FORWARD FRAME ITERATOR

func (iterator *ForwardFrameIterator) Next() *Frame {
	result := iterator.nextFrame
	if !iterator.nextFrame.IsNullFrame() {
		iterator.nextFrame = iterator.nextFrame.next //TODO : check
	}
	return result
}

func (iterator *ForwardFrameIterator) HasNext() bool {
	return !iterator.nextFrame.IsNullFrame()
}

func (iterator *ForwardFrameIterator) PeekNext() *Frame {
	return iterator.nextFrame
}

// FRAME

func (frame *Frame) Copy() *Frame {
	cFrame := Frame{Content: frame.Content, Flags:frame.Flags}
	cFrame.next = frame.next
	return &cFrame
}

func (frame *Frame) DeepCopy() *Frame {
	newcontent := frame.Content //copyOf TODO : check
	cFrame := &Frame{Content: newcontent, Flags:frame.Flags}
	cFrame.next = frame.next
	return cFrame
}

func (frame *Frame) IsEndFrame() bool {
	return IsFlagSet(frame.Flags, EndDataStructureFlag)
}

func (frame *Frame) IsBeginFrame() bool {
	return IsFlagSet(frame.Flags, BeginDataStructureFlag)
}

func (frame *Frame) IsNullFrame() bool {
	return IsFlagSet(frame.Flags, IsNullFlag)
}

func (frame *Frame) Size() int {
	if frame.Content == nil {
		return SizeOfFrameLengthAndFlags
	} else {
		return SizeOfFrameLengthAndFlags + len(frame.Content)
	}
}
