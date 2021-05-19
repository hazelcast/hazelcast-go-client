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

type UUID [16]byte

// NilUUID is empty UUID, all zeros
var NilUUID UUID

// NewUUID is used to generate a random UUID
func NewUUID() UUID {
	var uuid UUID
	if _, err := io.ReadFull(rand.Reader, uuid[:]); err != nil {
		panic(err)
	}
	uuid[6] &= 0x0f // clear version
	uuid[6] |= 0x40 // set to version 4
	uuid[8] &= 0x3f // clear variant
	uuid[8] |= 0x80 // set to IETF variant

	return uuid
}

func NewUUIDWith(mostSigBits, leastSigBits uint64) UUID {
	var uuid UUID
	binary.BigEndian.PutUint64(uuid[0:8], mostSigBits)
	binary.BigEndian.PutUint64(uuid[8:16], leastSigBits)
	return uuid
}

func (u UUID) String() string {
	return string(u.marshalText())
}

func (u UUID) MostSignificantBits() uint64 {
	return binary.BigEndian.Uint64(u[0:8])
}

func (u UUID) LeastSignificantBits() uint64 {
	return binary.BigEndian.Uint64(u[8:16])
}

func (u UUID) Default() bool {
	return u.MostSignificantBits() == 0 && u.LeastSignificantBits() == 0
}

func (u UUID) marshalText() []byte {
	dst := make([]byte, 36)
	hex.Encode(dst, u[:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], u[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], u[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], u[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:], u[10:])
	return dst
}
