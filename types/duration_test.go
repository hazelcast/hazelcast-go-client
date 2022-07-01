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

package types_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestDuration_String(t *testing.T) {
	dr := types.Duration(time.Minute)
	assert.Equal(t, dr.String(), time.Minute.String())
}

func TestDuration_MarshalText(t *testing.T) {
	dr := types.Duration(time.Minute)
	want := `1m0s`
	text, err := dr.MarshalText()
	if err != nil {
		return
	}
	assert.Equal(t, string(text), want)
}
