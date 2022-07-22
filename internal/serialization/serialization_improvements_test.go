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
	"encoding/binary"
	"math"
	"math/big"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"

	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// See: https://hazelcast.atlassian.net/wiki/spaces/IMDG/pages/1650294837/Hazelcast+Serialization+Improvements

func TestSerializationImprovements_1_UTFString(t *testing.T) {
	ss := mustSerializationService(iserialization.NewService(&serialization.Config{}))
	target := "\x60\xf0\x9f\x98\xad\xe2\x80\x8d\xf0\x9f\x98\xad\xe2\x80\x8d\xf0\x9f\x98\xad\xe2\x80\x8d\x60" // 😭‍😭‍😭
	data, err := ss.ToData(target)
	if err != nil {
		t.Fatal(err)
	}
	value, err := ss.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, target, value)
}

func TestSerializationImprovements_2_StringLength(t *testing.T) {
	ss := mustSerializationService(iserialization.NewService(&serialization.Config{}))
	// the following text has 23 bytes but have 8 Unicode runes.
	text := "\x60\xf0\x9f\x98\xad\xe2\x80\x8d\xf0\x9f\x98\xad\xe2\x80\x8d\xf0\x9f\x98\xad\xe2\x80\x8d\x60"
	assert.Equal(t, 8, utf8.RuneCountInString(text))
	data, err := ss.ToData(text)
	if err != nil {
		t.Fatal(err)
	}
	slen := binary.BigEndian.Uint32(data.ToByteArray()[8:])
	assert.Equal(t, uint32(23), slen)
}

func TestSerializationImprovements_4_UUID(t *testing.T) {
	config := &serialization.Config{}
	config.SetGlobalSerializer(&PanicingGlobalSerializer{})
	ss := mustSerializationService(iserialization.NewService(config))
	target := types.NewUUIDWith(math.MaxUint64, math.MaxUint64)
	data, err := ss.ToData(target)
	if err != nil {
		t.Fatal(err)
	}
	value, err := ss.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, target, value)
}

func TestSerializationImprovements_JavaDate(t *testing.T) {
	config := &serialization.Config{}
	config.SetGlobalSerializer(&PanicingGlobalSerializer{})
	ss := mustSerializationService(iserialization.NewService(config))
	target := time.Date(2021, 2, 1, 9, 1, 15, 11000, time.Local)
	data, err := ss.ToData(target)
	if err != nil {
		t.Fatal(err)
	}
	value, err := ss.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, target, value)
}

func TestSerializationImprovements(t *testing.T) {
	serializationImprovementsTester(func(ss *iserialization.Service) {
		var dec types.Decimal
		testCases := []struct {
			input  interface{}
			target interface{}
			name   string
		}{
			{
				input:  types.LocalDate(time.Date(2021, 2, 10, 0, 0, 0, 0, time.Local)),
				name:   "JavaLocalDate",
				target: types.LocalDate(time.Date(2021, 2, 10, 0, 0, 0, 0, time.Local)),
			},
			{
				input:  types.LocalTime(time.Date(0, 1, 1, 1, 2, 3, 50, time.Local)),
				name:   "JavaLocalTime",
				target: types.LocalTime(time.Date(0, 1, 1, 1, 2, 3, 50, time.Local)),
			},
			{
				input:  types.LocalDateTime(time.Date(2021, 2, 10, 1, 2, 3, 4, time.Local)),
				name:   "JavaLocalDateTime",
				target: types.LocalDateTime(time.Date(2021, 2, 10, 1, 2, 3, 4, time.Local)),
			},
			{
				input:  types.OffsetDateTime(time.Date(2021, 2, 10, 1, 2, 3, 4, time.FixedZone("", -3*60*60))),
				name:   "JavaOffsetDateTime",
				target: types.OffsetDateTime(time.Date(2021, 2, 10, 1, 2, 3, 4, time.FixedZone("", -3*60*60))),
			},
			{
				input:  []interface{}{"foo", int64(22)},
				name:   "JavaArrayList",
				target: []interface{}{"foo", int64(22)},
			},
			{
				input:  []interface{}{},
				name:   "JavaArrayList-empty",
				target: []interface{}{},
			},
			{
				input:  big.NewInt(-10_000_000),
				name:   "JavaBigInteger",
				target: big.NewInt(-10_000_000),
			},
			{
				input:  (*big.Int)(nil),
				name:   "JavaBigInteger-nil",
				target: new(big.Int),
			},
			{
				input:  types.NewDecimal(big.NewInt(111_111_111), 222_222_222),
				name:   "JavaBigDecimal",
				target: types.NewDecimal(big.NewInt(111_111_111), 222_222_222),
			},
			{
				input:  dec,
				name:   "JavaBigDecimal-nil",
				target: types.NewDecimal(new(big.Int), 0),
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				data, err := ss.ToData(tc.input)
				if err != nil {
					t.Fatal(err)
				}
				value, err := ss.ToObject(data)
				if err != nil {
					t.Fatal(err)
				}
				if tt, ok := tc.target.(time.Time); ok {
					ii := value.(time.Time)
					if !tt.Equal(ii) {
						t.Fatalf("%s != %s", tt.String(), ii.String())
					}
					return
				}
				assert.Equal(t, tc.target, value)
			})
		}

	})
}

func serializationImprovementsTester(f func(ss *iserialization.Service)) {
	config := &serialization.Config{}
	config.SetGlobalSerializer(&PanicingGlobalSerializer{})
	ss := mustSerializationService(iserialization.NewService(config))
	f(ss)
}

func mustSerializationService(ss *iserialization.Service, err error) *iserialization.Service {
	if err != nil {
		panic(err)
	}
	return ss
}

type PanicingGlobalSerializer struct{}

func (p PanicingGlobalSerializer) ID() (id int32) {
	return 1000
}

func (p PanicingGlobalSerializer) Read(input serialization.DataInput) interface{} {
	panic("panicing global serializer: read")
}

func (p PanicingGlobalSerializer) Write(output serialization.DataOutput, object interface{}) {
	panic("panicing global serializer: write")
}
