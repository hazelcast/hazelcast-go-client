// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proto

import (
	"encoding/binary"
)

const (
	TypeFieldOffset    = 0
	MessageTypeOffset  = 0
	ByteSizeInBytes    = 1
	BooleanSizeInBytes = 1
	ShortSizeInBytes   = 2
	CharSizeInBytes    = 2
	IntSizeInBytes     = 4
	FloatSizeInBytes   = 4
	LongSizeInBytes    = 8
	DoubleSizeInBytes  = 8
	UUIDSizeInBytes    = 17
	UuidSizeInBytes    = 17
	EntrySizeInBytes   = UUIDSizeInBytes + LongSizeInBytes

	CorrelationIDFieldOffset   = TypeFieldOffset + IntSizeInBytes
	CorrelationIDOffset        = MessageTypeOffset + IntSizeInBytes
	FragmentationIDOffset      = 0
	PartitionIDOffset          = CorrelationIDOffset + LongSizeInBytes
	RequestThreadIdOffset      = PartitionIDOffset + IntSizeInBytes
	RequestTtlOffset           = RequestThreadIdOffset + LongSizeInBytes
	RequestIncludeValueOffset  = PartitionIDOffset + IntSizeInBytes
	RequestListenerFlagsOffset = RequestIncludeValueOffset + BooleanSizeInBytes
	RequestLocalOnlyOffset     = RequestListenerFlagsOffset + IntSizeInBytes
	RequestReferenceIdOffset   = RequestTtlOffset + LongSizeInBytes
	ResponseBackupAcksOffset   = CorrelationIDOffset + LongSizeInBytes
	UnfragmentedMessage        = BeginFragmentFlag | EndFragmentFlag

	DefaultFlags              = 0
	BeginFragmentFlag         = 1 << 15
	EndFragmentFlag           = 1 << 14
	IsFinalFlag               = 1 << 13
	BeginDataStructureFlag    = 1 << 12
	EndDataStructureFlag      = 1 << 11
	IsNullFlag                = 1 << 10
	IsEventFlag               = 1 << 9
	BackupEventFlag           = 1 << 7
	SizeOfFrameLengthAndFlags = IntSizeInBytes + ShortSizeInBytes
)

var (
	EmptyArray = make([]byte, 0)
	NullFrame  = NewFrameWith(EmptyArray, IsNullFlag)
	BeginFrame = NewFrameWith(EmptyArray, BeginDataStructureFlag)
	EndFrame   = NewFrameWith(EmptyArray, EndDataStructureFlag)
)

// ClientMessage
type ClientMessage struct {
	StartFrame *Frame
	EndFrame   *Frame
	Retryable  bool
	Err        error
}

func NewClientMessage(startFrame *Frame) *ClientMessage {
	return &ClientMessage{StartFrame: startFrame, EndFrame: startFrame}
}

func NewClientMessageWithStartAndEndFrame(startFrame *Frame, endFrame *Frame) *ClientMessage {
	return &ClientMessage{StartFrame: startFrame, EndFrame: endFrame}
}

func NewClientMessageForEncode() *ClientMessage {
	return &ClientMessage{}
}

func NewClientMessageForDecode(frame *Frame) *ClientMessage {
	return NewClientMessage(frame)
}

func (clientMessage *ClientMessage) CopyWithNewCorrelationId(correlationID int64) *ClientMessage {
	initialFrameCopy := clientMessage.StartFrame.DeepCopy()
	frame := NewClientMessageWithStartAndEndFrame(initialFrameCopy, clientMessage.EndFrame)
	frame.SetCorrelationID(correlationID)
	frame.SetRetryable(clientMessage.IsRetryable())
	return frame
}

func (clientMessage *ClientMessage) IsRetryable() bool {
	return clientMessage.Retryable
}

func (clientMessage *ClientMessage) SetRetryable(retryable bool) {
	clientMessage.Retryable = retryable
}

func (clientMessage *ClientMessage) FrameIterator() *ForwardFrameIterator {
	return NewForwardFrameIterator(clientMessage.StartFrame)
}

func (clientMessage *ClientMessage) AddFrame(frame *Frame) {
	frame.next = nil
	if clientMessage.StartFrame == nil {
		clientMessage.StartFrame = frame
		clientMessage.EndFrame = frame
	} else {
		clientMessage.EndFrame.next = frame
		clientMessage.EndFrame = frame
	}
}

func (clientMessage *ClientMessage) GetMessageType() int32 {
	return int32(binary.LittleEndian.Uint32(clientMessage.StartFrame.Content[TypeFieldOffset:]))
}

func (clientMessage *ClientMessage) SetCorrelationID(correlationID int64) {
	binary.LittleEndian.PutUint64(clientMessage.StartFrame.Content[CorrelationIDFieldOffset:], uint64(correlationID))
}

func (clientMessage *ClientMessage) GetCorrelationID() int64 {
	return int64(binary.LittleEndian.Uint64(clientMessage.StartFrame.Content[CorrelationIDFieldOffset:]))
}

func (clientMessage *ClientMessage) GetFragmentationID() int64 {
	return int64(binary.LittleEndian.Uint64(clientMessage.StartFrame.Content[FragmentationIDOffset:]))
}

func (clientMessage *ClientMessage) GetNumberOfBackupAcks() uint8 {
	return clientMessage.StartFrame.Content[ResponseBackupAcksOffset]
}

func (clientMessage *ClientMessage) SetMessageType(messageType int32) {
	binary.LittleEndian.PutUint32(clientMessage.StartFrame.Content[MessageTypeOffset:], uint32(messageType))
}

func (clientMessage *ClientMessage) SetPartitionId(partitionId int32) {
	binary.LittleEndian.PutUint32(clientMessage.StartFrame.Content[PartitionIDOffset:], uint32(partitionId))
}

func (clientMessage *ClientMessage) GetTotalLength() int {
	totalLength := 0
	currentFrame := clientMessage.StartFrame
	for currentFrame != nil {
		totalLength += currentFrame.GetLength()
		currentFrame = currentFrame.next
	}
	return totalLength
}

func (clientMessage *ClientMessage) GetBytes(bytes []byte) int {
	pos := 0
	currentFrame := clientMessage.StartFrame
	for currentFrame != nil {
		isLastFrame := currentFrame.next == nil
		binary.LittleEndian.PutUint32(bytes[pos:], uint32(len(currentFrame.Content)+SizeOfFrameLengthAndFlags))
		if isLastFrame {
			binary.LittleEndian.PutUint16(bytes[pos+IntSizeInBytes:], currentFrame.flags|IsFinalFlag)
		} else {
			binary.LittleEndian.PutUint16(bytes[pos+IntSizeInBytes:], currentFrame.flags)
		}
		pos += SizeOfFrameLengthAndFlags
		copy(bytes[pos:], currentFrame.Content)
		pos += len(currentFrame.Content)
		currentFrame = currentFrame.next
	}
	return pos
}

func (clientMessage *ClientMessage) DropFragmentationFrame() {
	clientMessage.StartFrame = clientMessage.StartFrame.next
}

func (m *ClientMessage) DecodeError() *ServerError {
	return ErrorCodecDecode(m)
}

// ForwardFrameIterator
type ForwardFrameIterator struct {
	nextFrame *Frame
}

func NewForwardFrameIterator(frame *Frame) *ForwardFrameIterator {
	return &ForwardFrameIterator{frame}
}

func (forwardFrameIterator *ForwardFrameIterator) Next() *Frame {
	result := forwardFrameIterator.nextFrame
	if result != nil {
		forwardFrameIterator.nextFrame = forwardFrameIterator.nextFrame.next
	}
	return result
}

func (forwardFrameIterator *ForwardFrameIterator) HasNext() bool {
	return forwardFrameIterator.nextFrame != nil
}

func (forwardFrameIterator *ForwardFrameIterator) PeekNext() *Frame {
	return forwardFrameIterator.nextFrame
}

// Frame
type Frame struct {
	Content []byte
	flags   uint16
	next    *Frame
}

// NewFrame create with content
func NewFrame(content []byte) *Frame {
	return &Frame{Content: content, flags: DefaultFlags}
}

// NewFrame create with content with flags
func NewFrameWith(content []byte, flags uint16) *Frame {
	return &Frame{Content: content, flags: flags}
}

// Copy frame
func (frame Frame) Copy() *Frame {
	newFrame := NewFrameWith(frame.Content, frame.flags)
	newFrame.next = frame.next
	return newFrame
}

func (frame Frame) DeepCopy() *Frame {
	newContent := make([]byte, len(frame.Content))
	copy(newContent, frame.Content)
	newFrame := NewFrameWith(newContent, frame.flags)
	newFrame.next = frame.next
	return newFrame
}

// IsEndFrame is checking last frame
func (frame Frame) IsEndFrame() bool {
	return frame.IsFlagSet(EndDataStructureFlag)
}

func (frame Frame) IsBeginFrame() bool {
	return frame.IsFlagSet(BeginDataStructureFlag)
}

func (frame Frame) IsNullFrame() bool {
	return frame.IsFlagSet(IsNullFlag)
}

func (frame Frame) HasEventFlag() bool {
	return frame.IsFlagSet(IsEventFlag)
}

func (frame Frame) HasBackupEventFlag() bool {
	return frame.IsFlagSet(BackupEventFlag)
}

func (frame Frame) HasUnFragmentedMessageFlags() bool {
	return frame.IsFlagSet(UnfragmentedMessage)
}

func (frame Frame) HasBeginFragmentFlag() bool {
	return frame.IsFlagSet(BeginFragmentFlag)
}

func (frame Frame) HasEndFragmentFlag() bool {
	return frame.IsFlagSet(EndFragmentFlag)
}

func (frame Frame) IsFinalFrame() bool {
	return frame.IsFlagSet(IsFinalFlag)
}

func (frame Frame) IsFlagSet(flagMask uint16) bool {
	return frame.flags&flagMask == flagMask
}

func (frame Frame) GetLength() int {
	return SizeOfFrameLengthAndFlags + len(frame.Content)
}
