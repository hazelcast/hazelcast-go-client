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

package serialization

import (
	"math"
	"math/big"
	"math/bits"
)

// BigIntToJavaBytes returns a Java BigInteger compatible byte array.
func BigIntToJavaBytes(b *big.Int) []byte {
	i := b.BitLen()/8 + 1
	buf := make([]byte, i)
	sign := b.Sign()
	for _, d := range b.Bits() {
		for j := 0; j < bits.UintSize/8; j++ {
			i--
			if i >= 0 {
				if sign >= 0 {
					buf[i] = byte(d)
				} else if j == 0 {
					buf[i] = byte(-d)
				} else {
					buf[i] = ^byte(d)
				}
			} else if byte(d) != 0 {
				panic("serialization: big.Int buffer too small to fit value")
			}
			d >>= 8
		}
	}
	return buf
}

func JavaBytesToBigInt(bs []byte) *big.Int {
	if len(bs) == 0 {
		panic("serialization: zero length big.Int byte array")
	}
	b := big.NewInt(0)
	if bs[0] >= 128 {
		b.SetBits(makePositive(bs))
		b.Neg(b)
	} else {
		b.SetBits(stripLeadingZeroBytes(bs))
	}
	return b
}

func makePositive(a []byte) []big.Word {
	blen := len(a)
	// Find first non-sign (0xff) byte of input
	keep := 0
	for ; keep < blen && a[keep] == 0xff; keep++ {
	}
	// Allocate output array.  If all non-sign bytes are 0x00, we must allocate space for one extra output byte.
	k := keep
	for ; k < blen && a[k] == 0; k++ {
	}
	extraByte := 1
	if k != blen {
		extraByte = 0
	}
	intLen := ((blen - keep + extraByte) + 3) >> 2
	res := make([]big.Word, intLen)
	// Copy one's complement of input into output, leaving extra byte (if it exists) == 0x00
	b := blen - 1
	for i := intLen - 1; i >= 0; i-- {
		res[i] = (big.Word)(a[b] & 0xff)
		b--
		numBytesToTransfer := b - keep + 1
		if 3 < numBytesToTransfer {
			numBytesToTransfer = 3
		}
		if numBytesToTransfer < 0 {
			numBytesToTransfer = 0
		}
		for j := 8; j <= 8*numBytesToTransfer; j += 8 {
			res[i] |= (big.Word)(a[b]&0xff) << j
			b--
		}
		// Mask indicates which bits must be complemented
		mask := int64(math.MaxUint32 >> (8 * (3 - numBytesToTransfer)))
		res[i] = ^res[i] & big.Word(mask)
	}
	// Add one to one's complement to generate two's complement.
	for i := intLen - 1; i >= 0; i-- {
		res[i] = res[i] + 1
		if res[i] != 0 {
			break
		}
	}
	return res
}

func stripLeadingZeroBytes(a []byte) []big.Word {
	blen := len(a)
	keep := 0
	// Find first nonzero byte
	for ; keep < blen && a[keep] == 0; keep++ {
	}
	// Allocate new array and copy relevant part of input array
	ilen := ((blen - keep) + 3) >> 2
	res := make([]big.Word, ilen)
	b := blen - 1
	for i := ilen - 1; i >= 0; i-- {
		res[i] = (big.Word)(a[b] & 0xff)
		b--
		bytesRemaining := b - keep + 1
		bytesToTransfer := bytesRemaining
		if 3 < bytesToTransfer {
			bytesToTransfer = 3
		}
		for j := 8; j <= bytesToTransfer<<3; j += 8 {
			res[i] |= (big.Word)(a[b]&0xff) << j
			b--
		}
	}
	return res
}
