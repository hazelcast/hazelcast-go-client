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
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestJSON_String(t *testing.T) {
	j := serialization.JSON(`{"foo": 4}`)
	if !assert.Equal(t, `{"foo": 4}`, j.String()) {
		t.FailNow()
	}
}

// Test compatibility with `encoding/json` package
func TestJSON_Marshal(t *testing.T) {
	j := serialization.JSON(`{"foo":4}`)
	b, err := json.Marshal(j)
	assert.NoError(t, err)
	assert.Equal(t, []byte(j), b)
}

// Test compatibility with `encoding/json` package
func TestJSON_Unmarshal(t *testing.T) {
	b := []byte(`{"foo":4}`)
	var j serialization.JSON
	err := json.Unmarshal(b, &j)
	assert.NoError(t, err)
	assert.Equal(t, serialization.JSON(b), j)
}

func TestClassDefinitionAddDuplicateField(t *testing.T) {
	cd := serialization.NewClassDefinition(1, 2, 1)
	if err := cd.AddBoolField("foo"); err != nil {
		t.Fatal(err)
	}
	err := cd.AddByteField("foo")
	if err == nil {
		t.Fatalf("should have failed")
	}
	assert.True(t, errors.Is(err, hzerrors.ErrIllegalArgument))
}
