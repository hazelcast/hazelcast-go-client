package bufutil
/*
package bufutil

import (
	_ "bytes"
	"github.com/gulcesirvanci/hazelcast-go-client/internal"
	"go/ast"
	"strconv"
)
const (
	TYPE_FIELD_OFFSET           = 0
	CorrelationIdFieldOffset = TYPE_FIELD_OFFSET + Uint16SizeInBytes
	//Bits.SHORT_SIZE_IN_BYTES
	//offSet valid for fragmentation frames only
	FRAGMENTATION_ID_OFFSET = 0
	//optional fixed partition id field offSet
	PartitionIdFieldOffset = CorrelationIdFieldOffset + Int64SizeInBytes
	//Bits.LONG_SIZE_IN_BYTES
	DEFAULT_FLAGS                  = 0
	BEGIN_FRAGMENT_FLAG            = 1 << 15
	END_FRAGMENT_FLAG              = 1 << 14
	UnfragmentedMessage           = BEGIN_FRAGMENT_FLAG | END_FRAGMENT_FLAG
	IS_FINAL_FLAG                  = 1 << 13
	BEGIN_DATA_STRUCTURE_FLAG      = 1 << 12
	END_DATA_STRUCTURE_FLAG        = 1 << 11
	IS_NULL_FLAG                   = 1 << 10
	IS_EVENT_FLAG                  = 1 << 9
	BACKUP_AWARE_FLAG = 1 << 8;
	BACKUP_EVENT_FLAG = 1 << 7;

	SIZE_OF_FRAME_LENGTH_AND_FLAGS = Int32SizeInBytes + Uint16SizeInBytes

	//frame length + flags
)

var serialVersionUID = 1
var NullFrame = Frame{make([]byte, 0), IS_NULL_FLAG}
var BeginFrame = Frame{make([]byte, 0), BEGIN_DATA_STRUCTURE_FLAG}
var EndFrame = Frame{make([]byte, 0), END_DATA_STRUCTURE_FLAG}

type Frame struct {
	Content []byte
	Flags   int32
	next   *Frame
}

type ForwardFrameIterator struct {
	nextFrame	Frame	//TODO
}

type LinkedListFrame struct {
	StartFrame Frame
	EndFrame Frame
}

type ClientMessagex struct {
	Frames			 LinkedListFrame
	isRetryable      bool
	acquiresResource bool
	OperationName    string
	Connection       internal.Connection
	Frame			*Frame
}


// CLIENT MESSAGE

func CreateForEncode() * ClientMessagex {
message := new(ClientMessagex)
return message
}

func (m *ClientMessagex)CreateForDecode(frames LinkedListFrame) ClientMessagex {

message := ClientMessagex{
Frames: LinkedListFrame{StartFrame:frames.StartFrame,EndFrame:frames.StartFrame},
}
panic(frames.StartFrame.next == nil)
return message
}

func (m ClientMessagex)GetStartFrame() Frame {
return m.Frames.StartFrame
}

func (m *ClientMessagex)Add(frame Frame) *ClientMessagex {
frame.next = nil
if m.Frames.StartFrame.IsNullFrame() {
m.Frames.StartFrame = frame
m.Frames.EndFrame = frame
return m
}
m.Frames.EndFrame.next = &frame //TODO : check
m.Frames.EndFrame = frame
return m
}

func (m *ClientMessagex)FrameIterator() ForwardFrameIterator {
message := ForwardFrameIterator{m.Frames.StartFrame}
return message
}

func (m *ClientMessagex)GetMessageType() int32 { //short
return 1//Bits.readShortL(Get(0).content, ClientMessage.TYPE_FIELD_OFFSET);
}

func (m *ClientMessagex)SetMessageType(messageType int32) *ClientMessagex {
//Bits.writeShortL(Get(0).content, TYPE_FIELD_OFFSET, messageType);
return new(ClientMessagex)
}

func (m *ClientMessagex)GetCorrelationId() int64 { //long
return 1 //Bits.readLongL(Get(0).content, CorrelationIdFieldOffset);
}

func (m *ClientMessagex)SetCorrelationId(CorrelationIdFieldOffset int64) *ClientMessagex {
//Bits.writeLongL(Get(0).content, CorrelationIdFieldOffset, CorrelationIdFieldOffset);
return new(ClientMessagex) //this;
}

func (m *ClientMessagex)GetNumberOfBackupAcks() int64 {
return 1 //Bits.readIntL(getStartFrame().content, RESPONSE_BACKUP_ACKS_FIELD_OFFSET)
}

func (m *ClientMessagex)SetNumberOfBackupAcks(numberOfAcks  int64) *ClientMessagex { //const TODO
//Bits.writeIntL(getStartFrame().content, RESPONSE_BACKUP_ACKS_FIELD_OFFSET, numberOfAcks);
return new(ClientMessagex) //this;
}

func (m *ClientMessagex)GetPartitionId() int32 {
return 1 //Bits.readIntL(Get(0).content, PARTITION_ID_FIELD_OFFSET);
}

func (m *ClientMessagex)SetPartitionId(partitionId int32) *ClientMessagex {
//Bits.writeIntL(Get(0).content, PARTITION_ID_FIELD_OFFSET, partitionId);
return new(ClientMessagex) //this;
}

func (m *ClientMessagex)GetHeaderFlags() int32 {
return 1 //Get(0).flags;
}

func (m *ClientMessagex)IsRetryable() bool {
return m.isRetryable
}

func (m *ClientMessagex)SetRetryable(isRetryable bool) {
m.isRetryable = isRetryable
}

func (m *ClientMessagex)AcquiresResource() bool {
return m.acquiresResource //acquiresResource;
}

func (m *ClientMessagex)SetAcquiresResource(acquiresResource bool) {
m.acquiresResource=acquiresResource
}

func (m *ClientMessagex)SetOperationName(operationName string) {
m.OperationName = operationName
}

func (m *ClientMessagex)GetOperationName() string {
return m.OperationName
}

func IsFlagSet(flags int32, flagMask int32) bool {
i := flags & flagMask
return i == flagMask
}

func (m *ClientMessagex)SetConnection(connection internal.Connection) {
m.Connection = connection
}

func (m *ClientMessagex)GetConnection() internal.Connection { //yoksa ClientMessage.connection mi? TODO
return m.Connection
}

func (m *ClientMessagex)GetFrameLength() int {
frameLength := 0
currentFrame  := m.Frames.StartFrame
for currentFrame.IsNullFrame() {
frameLength += currentFrame.GetSize() //TODO
currentFrame = *currentFrame.next //TODO
}
return frameLength
}

func (m *ClientMessagex)IsUrgent() bool {
return false
}

func (m *ClientMessagex)ToString() string {
return ""
}

func (m *ClientMessagex)merge(fragment ClientMessagex){
fragmentMessageStartFrame := fragment.Frames.StartFrame.next
m.Frames.EndFrame.next = fragmentMessageStartFrame
m.Frames.EndFrame = fragment.Frames.EndFrame
}

func (m *ClientMessagex)ClientMessageToString() string {
str := "ClientMessage{\n" + "connection=" + m.Connection.String() + "\n"
if(!m.Frames.StartFrame.IsNullFrame()){
str += ", length=" + string(m.GetFrameLength()) + "\n" +
", correlationId=" + string(m.GetCorrelationId()) + "\n" +
", operation=" + m.GetOperationName() + "\n" +
", messageType=" + string(m.GetMessageType()) + "\n" +
", isRetryable=" + strconv.FormatBool(m.IsRetryable()) + "\n" +
", isEvent=" + strconv.FormatBool(IsFlagSet(m.Frames.StartFrame.Flags, IS_EVENT_FLAG)) + "\n" +
", isFragmented=" + strconv.FormatBool(!IsFlagSet(m.Frames.StartFrame.Flags, UnfragmentedMessage)) }

str += "}"
return str
}

func (m *ClientMessagex)CopyWithNewCorrelationId(correlationId int64) ClientMessagex{ //TODO
initialFrameCopy := m.Frames.StartFrame.DeepCopy() //TODO
newMessage := ClientMessagex{
Frames:           LinkedListFrame{initialFrameCopy,m.Frames.EndFrame},
}
newMessage.SetCorrelationId(correlationId)
newMessage.isRetryable = m.isRetryable
newMessage.acquiresResource = m.acquiresResource
newMessage.OperationName = m.OperationName

return newMessage
}

func (m *ClientMessagex)ClientMessageEquals(o ast.Object) bool {
return true //TODO
}

func (m *ClientMessagex)ClientMessageHashCode() int32 {
result := 1
return int32(result) //TODO
}

// FORWARD FRAME ITERATOR

func (iterator *ForwardFrameIterator)Next() Frame {
result := iterator.nextFrame
if !iterator.nextFrame.IsNullFrame() {
iterator.nextFrame = *iterator.nextFrame.next //TODO
}
return result
}

func (iterator *ForwardFrameIterator)IteratorHasNext() bool {
return !iterator.nextFrame.IsNullFrame()
}

func (iterator *ForwardFrameIterator)IteratorPeekNext() Frame {
return iterator.nextFrame
}

// FRAME

func (frame *Frame)Copy() Frame {
cFrame :=  Frame{frame.Content, frame.Flags}
cFrame.next = frame.next
return cFrame
}

func (frame *Frame)DeepCopy() Frame {
newContent := frame.Content //copyOf TODO
cFrame :=  Frame{newContent, frame.Flags}
cFrame.next = frame.next
return cFrame
}

func (frame *Frame)IsEndFrame() bool {
return IsFlagSet(frame.Flags,END_DATA_STRUCTURE_FLAG)
}

func (frame *Frame)IsBeginFrame() bool {
return IsFlagSet(frame.Flags,BEGIN_DATA_STRUCTURE_FLAG)
}

func (frame *Frame)IsNullFrame() bool {
return IsFlagSet(frame.Flags,IS_NULL_FLAG)
}

func (frame *Frame)GetSize() int {
if frame.Content == nil {
return SIZE_OF_FRAME_LENGTH_AND_FLAGS
}else {
return SIZE_OF_FRAME_LENGTH_AND_FLAGS + len(frame.Content)
}
}

func (m *ClientMessagex)FrameEquals(o ast.Object) bool {
return true //TODO
}

func (m *ClientMessagex)FrameHashCode() int32 {
result := 1
return int32(result) //TODO
}



*/