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
	m := b.Bits()
	sign := b.Sign()
	bl := bitCount(m, sign)/8 + 1
	if bl == 0 {
		return []byte{0x0}
	}
	arr := make([]byte, bl)
	i := bl - 1
	copied := 4
	nextInt := 0
	intIndex := 0
	for ; i >= 0; i-- {
		if copied == 4 {
			nextInt = getInt(intIndex, m, sign)
			intIndex++
			copied = 1
		} else {
			nextInt >>= 8
			copied++
		}
		arr[i] = byte(nextInt)
	}
	return arr
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

func bitCount(m []big.Word, signum int) int {
	n := 0
	l := len(m)
	if l == 0 {
		return n
	}
	magBitLen := ((l - 1) << 5) + bits.Len32(uint32(m[0]))
	if signum >= 0 {
		return magBitLen
	}
	pow2 := bits.OnesCount32(uint32(m[0])) == 1
	for i := 1; i < l && pow2; i++ {
		pow2 = m[i] == 0
	}
	n = magBitLen
	if pow2 {
		n--
	}
	return n
}

func getInt(n int, m []big.Word, signum int) int {
	if n < 0 {
		return 0
	}
	if n >= len(m) {
		if signum < 0 {
			return -1
		}
		return 0
	}
	magInt := int(m[len(m)-n-1])
	if signum >= 0 {
		return magInt
	}
	if n <= firstNonzeroIntNum(m) {
		return -magInt
	}
	return ^magInt
}

func firstNonzeroIntNum(m []big.Word) int {
	mlen := len(m)
	i := mlen - 1
	for ; i >= 0 && m[i] == 0; i-- {
	}
	return mlen - i - 1
}
