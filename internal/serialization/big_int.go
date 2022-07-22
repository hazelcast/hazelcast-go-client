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
	"fmt"
	"math"
	"math/big"
	"math/bits"
)

const (
	maxMagLength = math.MaxInt32/32 + 1
	longMask     = int64(0xffffffff)
)

// BigInt is a port of java.math.BigInteger with only the necessary bits.
type BigInt struct {
	mag    []int32
	signum int32
}

// NewBigInt creates a BigInt from the given big.Int
func NewBigInt(b *big.Int) BigInt {
	mag := wordsToInt32(b.Bits())
	return BigInt{signum: int32(b.Sign()), mag: mag}
}

// GoBigInt returns a big.Int from this BigInt
func (b BigInt) GoBigInt() *big.Int {
	bb := new(big.Int)
	if len(b.mag) == 0 {
		return bb
	}
	bb.SetBits(int32ArrToWords(b.mag))
	if b.signum < 0 {
		bb.Neg(bb)
	}
	return bb
}

// NewBigIntFromByteArray creates a BigInt from the given byte array.
func NewBigIntFromByteArray(val []byte) (BigInt, error) {
	var mag []int32
	var signum int32
	if len(val) == 0 {
		return BigInt{}, fmt.Errorf("zero length BigInt")
	}
	if val[0] >= 128 {
		mag = makePositive(val)
		signum = -1
	} else {
		mag = stripLeadingZeroBytes(val)
		signum = 0
		if len(mag) > 0 {
			signum = 1
		}
	}
	if len(mag) > maxMagLength || len(mag) == maxMagLength && mag[0] < 0 {
		return BigInt{}, fmt.Errorf("big int overflow")
	}
	return BigInt{signum: signum, mag: mag}, nil
}

// Bytes returns a java.math.BigInteger compatible byte array
func (b BigInt) Bytes() []byte {
	byteLen := b.bitLength()/8 + 1
	byteArr := make([]byte, byteLen)
	copied := 4
	nextInt := int32(0)
	intIndex := 0
	for i := byteLen - 1; i >= 0; i-- {
		if copied == 4 {
			nextInt = b.getInt(intIndex)
			intIndex++
			copied = 1
		} else {
			nextInt >>= 8
			copied++
		}
		byteArr[i] = byte(nextInt)
	}
	return byteArr
}

func (b BigInt) bitLength() int {
	m := b.mag
	l := len(m)
	if l == 0 {
		return 0
	}
	magBitLen := ((l - 1) << 5) + bits.Len32(uint32(m[0]))
	if b.signum >= 0 {
		return magBitLen
	}
	pow2 := bits.OnesCount32(uint32(m[0])) == 1
	for i := 1; i < l && pow2; i++ {
		pow2 = m[i] == 0
	}
	n := magBitLen
	if pow2 {
		n--
	}
	return n
}

func (b BigInt) getInt(n int) int32 {
	if n < 0 {
		return 0
	}
	if n >= len(b.mag) {
		if b.signum < 0 {
			return -1
		}
		return 0
	}
	magInt := b.mag[len(b.mag)-n-1]
	if b.signum >= 0 {
		return magInt
	}
	if n <= b.firstNonZeroIntNum() {
		return -magInt
	}
	return ^magInt
}

func (b BigInt) firstNonZeroIntNum() int {
	// Search for the first nonzero int
	var i int
	mlen := len(b.mag)
	for i = mlen - 1; i >= 0 && b.mag[i] == 0; i-- {
	}
	return mlen - i - 1
}

func makePositive(a []byte) []int32 {
	var keep, k int32
	byteLen := int32(len(a))
	// Find first non-sign (0xff) byte of input
	keep = 0
	for ; keep < byteLen && a[keep] == 0xff; keep++ {
	}
	// Allocate output array.
	// If all non-sign bytes are 0x00, we must allocate space for one extra output byte.
	k = keep
	for ; k < byteLen && a[k] == 0; k++ {
	}
	extraByte := int32(0)
	if k == byteLen {
		extraByte = 1
	}
	intLen := ((byteLen - keep + extraByte) + 3) >> 2
	result := make([]int32, intLen)
	// Copy one's complement of input into output, leaving extra byte (if it exists) == 0x00
	b := byteLen - 1
	for i := intLen - 1; i >= 0; i-- {
		result[i] = int32(a[b]) & 0xff
		b--
		numBytesToTransfer := b - keep + 1
		if 3 < numBytesToTransfer {
			numBytesToTransfer = 3
		}
		if numBytesToTransfer < 0 {
			numBytesToTransfer = 0
		}
		for j := 8; int32(j) <= 8*numBytesToTransfer; j += 8 {
			result[i] |= (int32(a[b]) & 0xff) << j
			b--
		}
		// Mask indicates which bits must be complemented
		mask := uint32(longMask) >> (8 * (3 - numBytesToTransfer))
		result[i] = ^result[i] & int32(mask)
	}
	// Add one to one's complement to generate two's complement
	for i := len(result) - 1; i >= 0; i-- {
		result[i] = int32((int64(result[i]) & longMask) + 1)
		if result[i] != 0 {
			break
		}
	}
	return result
}

func stripLeadingZeroBytes(a []byte) []int32 {
	byteLen := int32(len(a))
	keep := int32(0)
	// Find first nonzero byte
	for ; keep < byteLen && a[keep] == 0; keep++ {
	}
	// Allocate new array and copy relevant part of input array
	intLen := ((byteLen - keep) + 3) >> 2
	result := make([]int32, intLen)
	b := byteLen - 1
	for i := intLen - 1; i >= 0; i-- {
		result[i] = int32(a[b]) & 0xff
		b--
		bytesRemaining := b - keep + 1
		bytesToTransfer := bytesRemaining
		if 3 < bytesToTransfer {
			bytesToTransfer = 3
		}
		for j := int32(8); j <= bytesToTransfer<<3; j += 8 {
			result[i] |= (int32(a[b]) & 0xff) << j
			b--
		}
	}
	return result
}

// wordsToInt32 converts the given big.Int bits to java.math.BigInteger magnitude
func wordsToInt32(ws []big.Word) []int32 {
	if bits.UintSize == 32 {
		return wordsToInt32Arr32(ws)
	}
	return wordsToInt32Arr64(ws)
}

func wordsToInt32Arr32(ws []big.Word) []int32 {
	bs := make([]int32, len(ws))
	l := len(bs)
	for i := l - 1; i >= 0; i-- {
		bs[l-i-1] = int32(ws[i])
	}
	return bs
}

func wordsToInt32Arr64(ws []big.Word) []int32 {
	bs := make([]int32, len(ws)*2)
	l := len(bs)
	x := 0
	for i := len(ws) - 1; i >= 0; i-- {
		w := ws[i]
		bs[x+1] = int32(w)
		bs[x] = int32(w >> 32)
		x += 2
	}
	// remove leading zeros
	i := 0
	for ; i < l && bs[i] == 0; i++ {
	}
	if i != 0 {
		bs = bs[i:]
	}
	return bs
}

// int32ArrToWords converts the given java.math.BigInteger magnitude to big.Int bits.
func int32ArrToWords(a []int32) []big.Word {
	if bits.UintSize == 32 {
		return int32ArrToWords32(a)
	}
	return int32ArrToWords64(a)
}

func int32ArrToWords32(a []int32) []big.Word {
	l := len(a)
	ws := make([]big.Word, l)
	for i, b := range a {
		ws[l-1-i] = big.Word(b)
	}
	return ws
}

func int32ArrToWords64(a []int32) []big.Word {
	l := len(a)
	ws := make([]big.Word, l/2+l%2)
	if l%2 == 1 {
		// prepend the extra 0 to make the length even
		a2 := make([]int32, l+1)
		for i, x := range a {
			a2[i+1] = x
		}
		a = a2
		l = len(a)
	}
	x := l - 1
	for i := 0; i < len(ws); i++ {
		ws[i] = big.Word((int64(a[x-1]) << 32) | (int64(a[x]) & 0xffffffff))
		x -= 2
	}
	return ws
}
