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
			if !assert.Equal(t, tc.ByteArray, bs) {
				t.FailNow()
			}
		})
	}
}

func TestJavaBytesToBigInt(t *testing.T) {
	for _, tc := range bigIntTestCases() {
		t.Run(tc.BigInt.String(), func(t *testing.T) {
			b, err := serialization.JavaBytesToBigInt(tc.ByteArray)
			if err != nil {
				t.Fatal(err)
			}
			if !assert.Equal(t, tc.BigInt.String(), b.String()) {
				t.FailNow()
			}
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
			BigInt:    big.NewInt(-2),
			ByteArray: []byte{0xfe},
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
			BigInt:    big.NewInt(-128),
			ByteArray: []byte{0x80},
		},
		{
			BigInt:    big.NewInt(-129),
			ByteArray: []byte{0xff, 0x7f},
		},
		{
			BigInt:    big.NewInt(-256),
			ByteArray: []byte{0xff, 0x0},
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
			BigInt:    big.NewInt(-123_456),
			ByteArray: []byte{254, 29, 192},
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
			BigInt:    big.NewInt(-2_147_483_647),
			ByteArray: []byte{0x80, 0x0, 0x0, 0x01},
		},
		{
			BigInt:    big.NewInt(-2_147_483_648),
			ByteArray: []byte{0x80, 0x0, 0x0, 0x0},
		},
		{
			BigInt:    mustBigString("-9223372036854775808"),
			ByteArray: []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		},
		{
			BigInt:    mustBigString("-12346578912345678912345674891234567891346798"),
			ByteArray: []byte{255, 114, 68, 170, 100, 50, 133, 81, 93, 56, 50, 7, 244, 30, 237, 238, 97, 154, 146},
		},
		{
			BigInt:    mustBigString("-23154266667777888899991234566543219999888877776666245132"),
			ByteArray: []byte{255, 14, 66, 23, 217, 124, 4, 118, 195, 221, 232, 202, 92, 139, 47, 170, 241, 190, 215, 81, 135, 245, 199, 244},
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
			BigInt:    big.NewInt(128),
			ByteArray: []byte{0x0, 0x80},
		},
		{
			BigInt:    big.NewInt(129),
			ByteArray: []byte{0x0, 0x81},
		},
		{
			BigInt:    big.NewInt(256),
			ByteArray: []byte{0x01, 0x0},
		},
		{
			BigInt:    big.NewInt(999),
			ByteArray: []byte{3, 231},
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
		{
			BigInt:    mustBigString("10000000"),
			ByteArray: []byte{0x00, 0x98, 0x96, 0x80},
		},
		{
			BigInt:    mustBigString("2147483647"),
			ByteArray: []byte{127, 255, 255, 255},
		},
		{
			BigInt:    mustBigString("9223372036854775807"),
			ByteArray: []byte{127, 255, 255, 255, 255, 255, 255, 255},
		},
		{
			BigInt:    mustBigString("12346578912345678912345674891234567891346798"),
			ByteArray: []byte{0, 141, 187, 85, 155, 205, 122, 174, 162, 199, 205, 248, 11, 225, 18, 17, 158, 101, 110},
		},
		{
			BigInt:    mustBigString("23154266667777888899991234566543219999888877776666245132"),
			ByteArray: []byte{0, 241, 189, 232, 38, 131, 251, 137, 60, 34, 23, 53, 163, 116, 208, 85, 14, 65, 40, 174, 120, 10, 56, 12},
		},
	}
	return testCases
}

func mustBigString(s string) *big.Int {
	b := new(big.Int)
	_, ok := b.SetString(s, 10)
	if !ok {
		panic("cannot set string for big.Int")
	}
	return b
}
