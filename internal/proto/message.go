/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package proto

import (
	"encoding/binary"
	"io"
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
	//EmptyArray = make([]byte, 0)
	NullFrame  = NewFrameWith([]byte{}, IsNullFlag)
	BeginFrame = NewFrameWith([]byte{}, BeginDataStructureFlag)
	EndFrame   = NewFrameWith([]byte{}, EndDataStructureFlag)
)

func NewNullFrame() *Frame {
	return NewFrameWith([]byte{}, IsNullFlag)
}

func NewBeginFrame() *Frame {
	return NewFrameWith([]byte{}, BeginDataStructureFlag)
}

func NewEndFrame() *Frame {
	return NewFrameWith([]byte{}, EndDataStructureFlag)
}

// ClientMessage
type ClientMessage struct {
	Err       error
	Frames    []*Frame
	Retryable bool
}

func NewClientMessage(startFrame *Frame) *ClientMessage {
	// initial backing array size is kept large enough
	// for basic incoming messages, like map.Get()
	m := NewClientMessageForEncode()
	m.Frames = append(m.Frames, startFrame)
	return m
}

func NewClientMessageForEncode() *ClientMessage {
	// initial backing array size is kept large enough
	// for basic outbound messages, like map.Set()
	return &ClientMessage{Frames: make([]*Frame, 0, 4)}
}

func NewClientMessageForDecode(frame *Frame) *ClientMessage {
	return NewClientMessage(frame)
}

func (m *ClientMessage) Copy() *ClientMessage {
	frames := make([]*Frame, len(m.Frames))
	frames[0] = m.Frames[0].DeepCopy()
	copy(frames[1:], m.Frames[1:])
	return &ClientMessage{
		Frames:    frames,
		Retryable: m.Retryable,
		Err:       m.Err,
	}
}

// Merge Should be called after calling ClientMessage.DropFragmentationFrame on argument
func (m *ClientMessage) Merge(fragment *ClientMessage) {
	for _, frame := range fragment.Frames {
		m.Frames = append(m.Frames, frame)
	}
}

func (m *ClientMessage) SetRetryable(retryable bool) {
	m.Retryable = retryable
}

func (m *ClientMessage) FrameIterator() *ForwardFrameIterator {
	return NewForwardFrameIterator(m.Frames)
}

func (m *ClientMessage) AddFrame(frame *Frame) {
	m.Frames = append(m.Frames, frame)
}

func (m *ClientMessage) Type() int32 {
	return int32(binary.LittleEndian.Uint32(m.Frames[0].Content[TypeFieldOffset:]))
}

func (m *ClientMessage) CorrelationID() int64 {
	return int64(binary.LittleEndian.Uint64(m.Frames[0].Content[CorrelationIDFieldOffset:]))
}

func (m *ClientMessage) FragmentationID() int64 {
	return int64(binary.LittleEndian.Uint64(m.Frames[0].Content[FragmentationIDOffset:]))
}

func (m *ClientMessage) NumberOfBackupAcks() uint8 {
	return m.Frames[0].Content[ResponseBackupAcksOffset]
}

func (m *ClientMessage) PartitionID() int32 {
	return int32(binary.LittleEndian.Uint32(m.Frames[0].Content[PartitionIDOffset:]))
}

func (m *ClientMessage) SetCorrelationID(correlationID int64) {
	binary.LittleEndian.PutUint64(m.Frames[0].Content[CorrelationIDFieldOffset:], uint64(correlationID))
}

func (m *ClientMessage) SetMessageType(messageType int32) {
	binary.LittleEndian.PutUint32(m.Frames[0].Content[MessageTypeOffset:], uint32(messageType))
}

func (m *ClientMessage) SetPartitionId(partitionId int32) {
	binary.LittleEndian.PutUint32(m.Frames[0].Content[PartitionIDOffset:], uint32(partitionId))
}

func (m *ClientMessage) TotalLength() int {
	totalLength := 0
	for _, frame := range m.Frames {
		totalLength += frame.GetLength()
	}
	return totalLength
}

func (m *ClientMessage) Write(w io.Writer) error {
	lastIndex := len(m.Frames) - 1
	header := make([]byte, SizeOfFrameLengthAndFlags)
	for i, frame := range m.Frames {
		binary.LittleEndian.PutUint32(header, uint32(len(frame.Content)+SizeOfFrameLengthAndFlags))
		flags := frame.Flags
		if i == lastIndex {
			flags |= IsFinalFlag
		}
		binary.LittleEndian.PutUint16(header[IntSizeInBytes:], flags)
		if _, err := w.Write(header); err != nil {
			return err
		}
		if _, err := w.Write(frame.Content); err != nil {
			return err
		}
	}
	return nil
}

func (m *ClientMessage) DropFragmentationFrame() {
	m.Frames = m.Frames[1:]
}

func (m *ClientMessage) HasEventFlag() bool {
	return m.Frames[0].HasEventFlag()
}

func (m *ClientMessage) HasBackupEventFlag() bool {
	return m.Frames[0].HasBackupEventFlag()
}

func (m *ClientMessage) HasFinalFrame() bool {
	return m.Frames[len(m.Frames)-1].IsFinalFrame()
}

func (m *ClientMessage) HasUnFragmentedMessageFlags() bool {
	return m.Frames[0].HasUnFragmentedMessageFlags()
}

// ForwardFrameIterator
type ForwardFrameIterator struct {
	frames []*Frame
	next   int
}

func NewForwardFrameIterator(frames []*Frame) *ForwardFrameIterator {
	return &ForwardFrameIterator{frames: frames}
}

func (it *ForwardFrameIterator) Next() *Frame {
	if it.next >= len(it.frames) {
		return nil
	}
	result := it.frames[it.next]
	it.next++
	return result
}

func (it *ForwardFrameIterator) HasNext() bool {
	return it.next < len(it.frames)
}

func (it *ForwardFrameIterator) PeekNext() *Frame {
	return it.frames[it.next]
}

type Frame struct {
	Content []byte
	Flags   uint16
}

// NewFrame creates a Frame with content
func NewFrame(content []byte) *Frame {
	return &Frame{Content: content, Flags: DefaultFlags}
}

// NewFrameWith creates a Frame with content and Flags
func NewFrameWith(content []byte, flags uint16) *Frame {
	return &Frame{Content: content, Flags: flags}
}

// Copy frame
func (frame *Frame) Copy() *Frame {
	// TODO: Remove this function
	// Copying is not required except the first frame.
	//T his is a placeholder function until the protocol generator is changed to reflect that.
	return frame
}

func (frame *Frame) DeepCopy() *Frame {
	newContent := make([]byte, len(frame.Content))
	copy(newContent, frame.Content)
	return NewFrameWith(newContent, frame.Flags)
}

// IsEndFrame returns true if this is the last frame
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
	return frame.Flags&flagMask == flagMask
}

func (frame Frame) GetLength() int {
	return SizeOfFrameLengthAndFlags + len(frame.Content)
}
