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

package types

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io"
)

type UUID struct {
	mostSigBits  uint64
	leastSigBits uint64
}

// NewUUID is used to generate a random UUID v4 using rand.Reader as the CSRNG.
func NewUUID() UUID {
	buf := make([]byte, 16)
	_, _ = io.ReadFull(rand.Reader, buf)
	buf[6] &= 0x0f // clear version
	buf[6] |= 0x40 // set to version 4
	buf[8] &= 0x3f // clear variant
	buf[8] |= 0x80 // set to IETF variant

	return UUID{binary.BigEndian.Uint64(buf[0:8]),
		binary.BigEndian.Uint64(buf[8:])}
}

func NewUUIDWith(mostSigBits, leastSigBits uint64) UUID {
	return UUID{mostSigBits, leastSigBits}
}

func (u UUID) String() string {
	return string(u.marshalText())
}

func (u UUID) MostSignificantBits() uint64 {
	return u.mostSigBits
}

func (u UUID) LeastSignificantBits() uint64 {
	return u.leastSigBits
}

func (u UUID) Default() bool {
	return u.mostSigBits == 0 && u.leastSigBits == 0
}

func (u UUID) marshalText() []byte {
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[0:8], u.mostSigBits)
	binary.BigEndian.PutUint64(data[8:16], u.leastSigBits)
	dst := make([]byte, 36)
	hex.Encode(dst, data[:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], data[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], data[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], data[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:], data[10:])
	return dst
}
