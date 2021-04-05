// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compatibility

import (
	serialization "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

const (
	// PORTABLE IDS
	portableFactoryID    = 1
	portableClassID      = 1
	innerPortableClassID = 2

	// IDENTIFIED DATA SERIALIZABLE IDS
	identifiedDataSerializableFactoryID = 1
	dataSerializableClassID             = 1

	// CUSTOM SERIALIZER IDS
	// customStreamSerializableID    = 1
	// customByteArraySerializableID = 2
)

func getAllTestObjects() []interface{} {
	var aNullObject interface{}
	var aBoolean = true
	var aByte byte = 113
	var aChar uint16 = 'x'
	var aDouble = -897543.3678909
	var aShort int16 = -500
	var aFloat float32 = 900.5678
	var anInt int32 = 56789
	var aLong int64 = -50992225
	var aString = "Pijamalı hasta, yağız şoföre çabucak güvendi.イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム" +
		"The quick brown fox jumps over the lazy dog"

	var booleans = []bool{true, false, true}

	// byte is signed in Java but unsigned in Go!
	var bytes = []byte{112, 4, 255, 4, 112, 221, 43}
	var chars = []uint16{'a', 'b', 'c'}
	var doubles = []float64{-897543.3678909, 11.1, 22.2, 33.3}
	var shorts = []int16{-500, 2, 3}
	var floats = []float32{900.5678, 1.0, 2.1, 3.4}
	var ints = []int32{56789, 2, 3}
	var longs = []int64{-50992225, 1231232141, 2, 3}
	w1 := "Pijamalı hasta, yağız şoföre çabucak güvendi."
	w2 := "イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム"
	w3 := "The quick brown fox jumps over the lazy dog"
	var strings = []string{w1, w2, w3}

	anInnerPortable := &AnInnerPortable{anInt, aFloat}
	var portables = []serialization.Portable{anInnerPortable, anInnerPortable, anInnerPortable}
	anIdentified := &anIdentifiedDataSerializable{aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, aString,
		booleans, bytes, chars, doubles, shorts, floats, ints, longs, strings,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, anInnerPortable,
		nil}
	aPortable := &aPortable{aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, aString, anInnerPortable,
		booleans, bytes, chars, doubles, shorts, floats, ints, longs, strings, portables, nil, nil,
		nil, nil, nil, nil, nil, nil, nil}

	return []interface{}{aNullObject, aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, aString, anInnerPortable,
		booleans, bytes, chars, doubles, shorts, floats, ints, longs, strings,
		anIdentified, aPortable}

}
