/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestCompactConfig_SetSerializers_OverrideDefaultFails(t *testing.T) {
	s := int32Serializer{}
	var cfg serialization.CompactConfig
	cfg.SetSerializers(s)
	err := cfg.Validate()
	require.True(t, errors.Is(err, hzerrors.ErrIllegalArgument))
}

type int32Serializer struct{}

func (s int32Serializer) Type() reflect.Type {
	var i int32
	return reflect.TypeOf(i)
}

func (s int32Serializer) TypeName() string {
	return "int32"
}

func (i int32Serializer) Read(reader serialization.CompactReader) interface{} {
	return reader.ReadInt32("field")
}

func (i int32Serializer) Write(writer serialization.CompactWriter, value interface{}) {
	writer.WriteInt32("field", value.(int32))
}
