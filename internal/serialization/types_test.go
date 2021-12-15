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

package serialization_test

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type BigIntTestCase struct {
	BigInt    *big.Int
	ByteArray []byte
}

func TestBigIntToJavaBytes(t *testing.T) {
	for _, tc := range bigIntTestCases() {
		t.Run(tc.BigInt.String(), func(t *testing.T) {
			bs := serialization.BigIntToJavaBytes(tc.BigInt)
			assert.Equal(t, tc.ByteArray, bs)
		})
	}
}

func TestJavaBytesToBigInt(t *testing.T) {
	for _, tc := range bigIntTestCases() {
		t.Run(tc.BigInt.String(), func(t *testing.T) {
			b := serialization.JavaBytesToBigInt(tc.ByteArray)
			assert.Equal(t, tc.BigInt.String(), b.String())
		})
	}
}

func bigIntTestCases() []BigIntTestCase {
	testCases := []BigIntTestCase{
		{
			BigInt:    big.NewInt(0),
			ByteArray: []byte{0},
		},
		{
			BigInt:    big.NewInt(-1),
			ByteArray: []byte{0xff},
		},
		{
			BigInt:    big.NewInt(-10),
			ByteArray: []byte{246},
		},
		{
			BigInt:    big.NewInt(-100),
			ByteArray: []byte{156},
		},
		{
			BigInt:    big.NewInt(-1000),
			ByteArray: []byte{0xfc, 0x18},
		},
		{
			BigInt:    big.NewInt(-10_000),
			ByteArray: []byte{0xd8, 0xf0},
		},
		{
			BigInt:    big.NewInt(-100_000),
			ByteArray: []byte{0xfe, 0x79, 0x60},
		},
		{
			BigInt:    big.NewInt(-1_000_000),
			ByteArray: []byte{0xf0, 0xbd, 0xc0},
		},
		{
			BigInt:    big.NewInt(-10_000_000),
			ByteArray: []byte{0xff, 0x67, 0x69, 0x80},
		},
		{
			BigInt:    big.NewInt(1),
			ByteArray: []byte{1},
		},
		{
			BigInt:    big.NewInt(10),
			ByteArray: []byte{10},
		},
		{
			BigInt:    big.NewInt(100),
			ByteArray: []byte{100},
		},
		{
			BigInt:    big.NewInt(1000),
			ByteArray: []byte{0x03, 0xe8},
		},
		{
			BigInt:    big.NewInt(10_000),
			ByteArray: []byte{0x27, 0x10},
		},
		{
			BigInt:    big.NewInt(100_000),
			ByteArray: []byte{0x01, 0x86, 0xa0},
		},
		{
			BigInt:    big.NewInt(1_000_000),
			ByteArray: []byte{0x0f, 0x42, 0x40},
		},
		{
			BigInt:    big.NewInt(10_000_000),
			ByteArray: []byte{0x00, 0x98, 0x96, 0x80},
		},
	}
	return testCases
}
