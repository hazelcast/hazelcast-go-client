package bufutil
/*
WIP
TODO: Need to fill functions.
*/
import (
	_ "bytes"
	"github.com/gulcesirvanci/hazelcast-go-client/internal"
	"go/ast"
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
	SIZE_OF_FRAME_LENGTH_AND_FLAGS = Int32SizeInBytes + Uint16SizeInBytes

//frame length + flags
)

var serialVersionUID = 1
var NullFrame = Frame{make([]byte, 0), IS_NULL_FLAG} //how to use new for Frame
var BeginFrame = Frame{make([]byte, 0), BEGIN_DATA_STRUCTURE_FLAG}
var EndFrame = Frame{make([]byte, 0), END_DATA_STRUCTURE_FLAG}


type Frame struct {
	Content []byte
	Flags   int
	next   *Frame
	//transient

}

type ClientMessagex struct {
	Frames			[]Frame
	IsRetryable      bool
	acquiresResource bool
	operationName    string
	connection       internal.Connection
	Frame			*Frame
}

func CreateForEncode() * ClientMessagex {
	message := new(ClientMessagex)
	return message
}

func (cMessage *ClientMessagex)CreateForDecode(frames []Frame) *ClientMessagex {
	//message := make(ClientMessagex,frames)
	return new(ClientMessagex) //new ClientMessage(frames)
}

func (cMessage *ClientMessagex)GetMessaGeType() int32 { //short
	return 1 //Bits.readShortL(Get(0).content, ClientMessage.TYPE_FIELD_OFFSET);
}

func (cMessage *ClientMessagex)SetMessaGeType(messageType int32) *ClientMessagex {
	//Bits.writeShortL(Get(0).content, TYPE_FIELD_OFFSET, messageType);
	return new(ClientMessagex)
}

func (cMessage *ClientMessagex)GetCorrelationIdFieldOffset() int64 { //long
	return 1 //Bits.readLongL(Get(0).content, CorrelationIdFieldOffset);
}

func (cMessage *ClientMessagex)SetCorrelationIdFieldOffset(CorrelationIdFieldOffset int64) *ClientMessagex {
	//Bits.writeLongL(Get(0).content, CorrelationIdFieldOffset, CorrelationIdFieldOffset);
	return new(ClientMessagex) //this;
}

func (cMessage *ClientMessagex)GetPartitionId() int32 {
	return 1 //Bits.readIntL(Get(0).content, PARTITION_ID_FIELD_OFFSET);
}

func (cMessage *ClientMessagex)SetPartitionId(partitionId int32) *ClientMessagex {
	//Bits.writeIntL(Get(0).content, PARTITION_ID_FIELD_OFFSET, partitionId);
	return new(ClientMessagex) //this;
}

func (cMessage *ClientMessagex)GetHeaderFlags() int32 {
	return 1 //Get(0).flags;
}

func (cMessage *ClientMessagex)IisRetryable() bool {
	return true //isRetryable;
}

func (cMessage *ClientMessagex)AcquiresResource() bool {
	return true //acquiresResource;
}

func (cMessage *ClientMessagex)SetAcquiresResource(acquiresResource bool) {
	cMessage.acquiresResource=acquiresResource
}

func (cMessage *ClientMessagex)SetRetryable(isRetryable bool) {
	//this.isRetryable = isRetryable;
}

func (cMessage *ClientMessagex)SetOperationName(operationName string) {
	cMessage.operationName = operationName
}

func (cMessage *ClientMessagex)GetOperationName() string {
	return "" //operationName;
}

func (cMessage *ClientMessagex)IsFlagSet(flags int32, flagMask int32) bool {
	i := flags & flagMask
	return i == flagMask
}

func (cMessage *ClientMessagex)SetConnection(connection internal.Connection) {
	//connection = connection;
}

func (cMessage *ClientMessagex)GetConnection() internal.Connection {
	connect := internal.Connection{}
	return  connect//connection
}

func (cMessage *ClientMessagex)GetFrameLength() int {
	frameLength := 0
	return frameLength
}

func (cMessage *ClientMessagex)IsUrgent() bool {
	return false
}

func (cMessage *ClientMessagex)ToString() string {
	return ""
}

/**
 * Copies the clientMessage efficiently with correlation id
 * Only initialFrame is duplicated, rest of the frames are shared
 *
 * @param CorrelationIdFieldOffset new id
 * @return the copy message
 */
func (cMessage *ClientMessagex)CopyWithNewCorrelationIdFieldOffset(CorrelationIdFieldOffset int64) *ClientMessagex {
	newMessage := new(ClientMessagex)

	return newMessage
}

func (cMessage *ClientMessagex)Equals(o ast.Object) bool {

	return false
}

func (cMessage *ClientMessagex)HashCode() int32 {
	result := 1
	return int32(result)
}

func (frame *Frame)IsEndFrame() bool {
	return frame.IsNullFrame() //bos doto
}

func (frame *Frame)IsNullFrame() bool {
	return true
}

func (cMessage *ClientMessagex)GetSize() int32 {
	return SIZE_OF_FRAME_LENGTH_AND_FLAGS
}

func (cMessage *ClientMessagex)Add(frame Frame) {
}

func (cMessage *ClientMessagex)Next() *Frame {
	cMessage.Frame = cMessage.Frame.next
	return cMessage.Frame
}

func (cMessage *ClientMessagex)FrameIterator() *ClientMessagex{
	return cMessage //yoksa burasi frame mi olmaliydi
}

func (frame *Frame)NextFrame() *Frame {
	frame = frame.next
	return frame
}
