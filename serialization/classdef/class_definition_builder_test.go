// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package classdef

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClassDefinitionBuilder_AddFieldAfterBuildError(t *testing.T) {
	n := NewClassDefinitionBuilder(1, 1, 1)
	n.Build()
	var err error
	err = n.AddByteField("name")
	assert.Error(t, err)
	err = n.AddBoolField("name")
	assert.Error(t, err)
	err = n.AddUInt16Field("name")
	assert.Error(t, err)
	err = n.AddInt16Field("name")
	assert.Error(t, err)
	err = n.AddInt32Field("name")
	assert.Error(t, err)
	err = n.AddInt64Field("name")
	assert.Error(t, err)
	err = n.AddFloat32Field("name")
	assert.Error(t, err)
	err = n.AddFloat64Field("name")
	assert.Error(t, err)
	err = n.AddUTFField("name")
	assert.Error(t, err)
	err = n.AddPortableField("name", nil)
	assert.Error(t, err)
	err = n.AddByteArrayField("name")
	assert.Error(t, err)
	err = n.AddBoolArrayField("name")
	assert.Error(t, err)
	err = n.AddUInt16ArrayField("name")
	assert.Error(t, err)
	err = n.AddInt32ArrayField("name")
	assert.Error(t, err)
	err = n.AddInt64ArrayField("name")
	assert.Error(t, err)
	err = n.AddFloat32ArrayField("name")
	assert.Error(t, err)
	err = n.AddFloat64ArrayField("name")
	assert.Error(t, err)
	err = n.AddUTFArrayField("name")
	assert.Error(t, err)
	err = n.AddPortableArrayField("name", nil)
	assert.Error(t, err)
	err = n.AddField(nil)
	assert.Error(t, err)

}
