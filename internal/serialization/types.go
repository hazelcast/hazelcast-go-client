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
	"math/big"
)

// BigIntToJavaBytes returns a Java BigInteger compatible byte array.
func BigIntToJavaBytes(b *big.Int) []byte {
	return NewBigInt(b).Bytes()
}

// JavaBytesToBigInt creates a Go big.Int value from the given byte array.
func JavaBytesToBigInt(bs []byte) (*big.Int, error) {
	bb, err := NewBigIntFromByteArray(bs)
	if err != nil {
		return nil, fmt.Errorf("creating BigInt from byte array: %w", err)
	}
	return bb.GoBigInt(), nil
}
