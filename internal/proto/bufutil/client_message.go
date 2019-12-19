package bufutil

import (
	"github.com/gulcesirvanci/hazelcast-go-client/internal"
	"strconv"
)

const (
	TypeFieldOffset               = 0
	CorrelationIdFieldOffset      = TypeFieldOffset + Uint16SizeInBytes
	FragmentationIdOffset         = 0
	ResponseBackupAcksFieldOffset = CorrelationIdFieldOffset + Int64SizeInBytes
	PartitionIdFieldOffset        = CorrelationIdFieldOffset + Int64SizeInBytes
	DefaultFlags                  = 0
	BeginFragmentFlag             = 1 << 15
	EndFragmentFlag               = 1 << 14
	UnfragmentedMessage           = BeginFragmentFlag | EndFragmentFlag
	IsFinalFlag                   = 1 << 13
	BeginDataStructureFlag        = 1 << 12
	EndDataStructureFlag          = 1 << 11
	IsNullFlag                    = 1 << 10
	IsEventFlag                   = 1 << 9
	BackupAwareFlag               = 1 << 8
	BackupEventFlag               = 1 << 7

	SizeOfFrameLengthAndFlags = Int32SizeInBytes + Uint16SizeInBytes
)

var serialVersionUID = 1
var NullFrame = &Frame{make([]byte, 0), IsNullFlag}
var BeginFrame = &Frame{make([]byte, 0), BeginDataStructureFlag}
var EndFrame = &Frame{make([]byte, 0), EndDataStructureFlag}

type Frame struct {
	Content []byte
	Flags   int32
	next    *Frame
}

type ForwardFrameIterator struct {
	nextFrame *Frame //TODO : check
}

type ClientMessagex struct {
	StartFrame       *Frame
	EndFrame         *Frame
	Is_Retryable      bool
	acquiresResource bool
	OperationName    string
	Connection       internal.Connection
}

/*

	Constructors

*/

// CLIENT MESSAGE

func CreateForEncode() *ClientMessagex {
	message := new(ClientMessagex)
	return message
}

func CreateForDecode(frame *Frame) ClientMessagex {
	message := ClientMessagex{
		StartFrame: frame,
		EndFrame:   frame,
	}
	panic(frame.next == nil) //TODO : check
	return message
}

func (m *ClientMessagex) GetStartFrame() *Frame {
	return m.StartFrame
}

func (m *ClientMessagex) Add(frame *Frame) *ClientMessagex {
	frame.next = nil
	if m.StartFrame.IsNullFrame() {
		m.StartFrame = frame
		m.EndFrame = frame
		return m
	}
	m.EndFrame.next = frame //TODO : check : check
	m.EndFrame = frame
	return m
}

func (m *ClientMessagex) FrameIterator() *ForwardFrameIterator {
	message := &ForwardFrameIterator{m.StartFrame}
	return message
}

func (m *ClientMessagex) GetMessageType() int32 {
	return ReadInt32(m.StartFrame.Content, TypeFieldOffset, false)
}

func (m *ClientMessagex) SetMessageType(messageType int32) *ClientMessagex {
	WriteInt32(m.StartFrame.Content, TypeFieldOffset, messageType, false)
	return m
}

func (m *ClientMessagex) GetCorrelationId() int64 {
	return ReadInt64(m.StartFrame.Content, CorrelationIdFieldOffset, false)
}

func (m *ClientMessagex) SetCorrelationId(CorrelationId int64) *ClientMessagex { //TODO: check
	WriteInt64(m.StartFrame.Content, CorrelationIdFieldOffset, CorrelationId, false)
	return m
}

func (m *ClientMessagex) GetNumberOfBackupAcks() int64 {
	return ReadInt64(m.StartFrame.Content, ResponseBackupAcksFieldOffset, false)
}

func (m *ClientMessagex) SetNumberOfBackupAcks(numberOfAcks int64) *ClientMessagex {
	//const TODO numberOfAcks
	WriteInt64(m.StartFrame.Content, ResponseBackupAcksFieldOffset, numberOfAcks, false)
	return m
}

func (m *ClientMessagex) GetPartitionId() int32 {
	return ReadInt32(m.StartFrame.Content, PartitionIdFieldOffset, false)
}

func (m *ClientMessagex) SetPartitionId(partitionId int32) *ClientMessagex {
	WriteInt32(m.StartFrame.Content, PartitionIdFieldOffset, partitionId, false)
	return m
}

func (m *ClientMessagex) GetHeaderFlags() int32 {
	return m.StartFrame.Flags
}

func (m *ClientMessagex) IsRetryable() bool {
	return m.Is_Retryable
}

func (m *ClientMessagex) SetRetryable(isRetryable bool) {
	m.Is_Retryable = isRetryable
}

func (m *ClientMessagex) AcquiresResource() bool {
	return m.acquiresResource
}

func (m *ClientMessagex) SetAcquiresResource(acquiresResource bool) {
	m.acquiresResource = acquiresResource
}

func (m *ClientMessagex) SetOperationName(operationName string) {
	m.OperationName = operationName
}

func (m *ClientMessagex) GetOperationName() string {
	return m.OperationName
}

func IsFlagSet(flags int32, flagMask int32) bool {
	i := flags & flagMask
	return i == flagMask
}

func (m *ClientMessagex) SetConnection(connection internal.Connection) {
	m.Connection = connection
}

func (m *ClientMessagex) GetConnection() internal.Connection {
	return m.Connection
}

func (m *ClientMessagex) GetFrameLength() int {
	frameLength := 0
	currentFrame := m.StartFrame
	for currentFrame.IsNullFrame() {
		frameLength += currentFrame.GetSize() //TODO : check
		currentFrame = currentFrame.next     //TODO : check
	}
	return frameLength
}

func (m *ClientMessagex) IsUrgent() bool {
	return false
}


func (m *ClientMessagex) Merge(fragment *ClientMessagex) {
	fragmentMessageStartFrame := fragment.StartFrame.next
	m.EndFrame.next = fragmentMessageStartFrame
	m.EndFrame = fragment.EndFrame
}

func (m *ClientMessagex) ClientMessageToString() string {
	str := "ClientMessage{\n" + "connection=" + m.Connection.String() + "\n"
	if !m.StartFrame.IsNullFrame() {
		str += ", length=" + string(m.GetFrameLength()) + "\n" +
			", correlationId=" + string(m.GetCorrelationId()) + "\n" +
			", operation=" + m.GetOperationName() + "\n" +
			", messageType=" + string(m.GetMessageType()) + "\n" +
			", isRetryable=" + strconv.FormatBool(m.IsRetryable()) + "\n" +
			", isEvent=" + strconv.FormatBool(IsFlagSet(m.StartFrame.Flags, IsEventFlag)) + "\n" +
			", isFragmented=" + strconv.FormatBool(!IsFlagSet(m.StartFrame.Flags, UnfragmentedMessage))
	}

	str += "}"
	return str
}

func (m *ClientMessagex) CopyWithNewCorrelationId(correlationId int64) *ClientMessagex { //TODO : check
	initialFrameCopy := m.StartFrame.DeepCopy() //TODO : check
	newMessage := &ClientMessagex{initialFrameCopy, m.EndFrame}
	newMessage.SetCorrelationId(correlationId)
	newMessage.Is_Retryable = m.Is_Retryable
	newMessage.acquiresResource = m.acquiresResource
	newMessage.OperationName = m.OperationName

	return newMessage
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
	cFrame := Frame{frame.Content, frame.Flags}
	cFrame.next = frame.next
	return &cFrame
}

func (frame *Frame) DeepCopy() *Frame {
	newContent := frame.Content //copyOf TODO : check
	cFrame := &Frame{newContent, frame.Flags}
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

func (frame *Frame) GetSize() int {
	if frame.Content == nil {
		return SizeOfFrameLengthAndFlags
	} else {
		return SizeOfFrameLengthAndFlags + len(frame.Content)
	}
}
