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

package cluster

import (
	"bytes"
	"encoding/binary"

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const messageBufferSize = 16 * 1024

type clientMessageReader struct {
	src                *bytes.Buffer
	clientMessage      *proto.ClientMessage
	currentFrameLength uint32
	currentFlags       uint16
	readHeader         bool
}

func newClientMessageReader() *clientMessageReader {
	return &clientMessageReader{
		src: bytes.NewBuffer(make([]byte, 0, messageBufferSize)),
	}
}

func (c *clientMessageReader) Append(buf []byte) {
	c.src.Write(buf)
}

func (c *clientMessageReader) Read() *proto.ClientMessage {
	for {
		if c.readFrame() {
			if c.clientMessage.HasFinalFrame() {
				return c.clientMessage
			}
		} else {
			return nil
		}
	}
}

func (c *clientMessageReader) readFrame() bool {
	if !c.readHeader {
		if c.src.Len() < proto.SizeOfFrameLengthAndFlags {
			// we don't have even the frame length and flags ready
			return false
		}
		frameLength := binary.LittleEndian.Uint32(c.src.Next(proto.IntSizeInBytes))
		if frameLength < proto.SizeOfFrameLengthAndFlags {
			panic("frame length is less than SizeOfFrameLengthAndFlags")
		}
		c.currentFrameLength = frameLength
		c.currentFlags = binary.LittleEndian.Uint16(c.src.Next(2))
		c.readHeader = true
	}
	if c.readHeader {
		size := int(c.currentFrameLength) - proto.SizeOfFrameLengthAndFlags
		if c.src.Len() < size {
			return false
		}
		frameContent := c.src.Next(size)
		frame := proto.NewFrameWith(frameContent, c.currentFlags)
		if c.clientMessage == nil {
			c.clientMessage = proto.NewClientMessageForDecode(frame)
		} else {
			c.clientMessage.AddFrame(frame)
		}
		c.readHeader = false
		return true
	}
	return false
}

func (c *clientMessageReader) Reset() {
	c.clientMessage = nil
	c.resetBufferIfNeeded()
}

// TODO: maybe it's better to copy the memory and release the buffer???
func (c *clientMessageReader) resetBufferIfNeeded() {
	// Note: we can't reuse underlying byte array from the src buffer.
	// that's because slices from that array are propagated to client
	// message frames, then, sometimes, to user code as []byte slices
	// and strings.
	if c.src.Cap() <= messageBufferSize {
		return // no need to resize for now
	}
	// read the remaining data
	all := c.src.Next(c.src.Len())
	// reset the buffer
	c.src = bytes.NewBuffer(make([]byte, 0, messageBufferSize))
	// write the remaining data back
	if len(all) > 0 {
		c.src.Write(all)
	}
}
