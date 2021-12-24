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

package event

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeSubscriptionID(t *testing.T) {
	subscriptionID := MakeSubscriptionID(func() {})
	if subscriptionID == 0 {
		t.Fatalf("unexpected 0")
	}
}

func TestMakeSubscriptionIDFails(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Fatalf("expected a panic")
		}
	}()
	MakeSubscriptionID(42)
}

func TestParseAndFormatSubscriptionID(t *testing.T) {
	var testID int64 = 123456
	formatted := FormatSubscriptionID(testID)
	sid, err := ParseSubscriptionID(formatted)
	assert.Nil(t, err)
	assert.Equal(t, testID, sid)
	// error on parse
	sid, err = ParseSubscriptionID("unparsable id")
	assert.IsType(t, &strconv.NumError{}, err)
	assert.Equal(t, int64(0), sid)
}
