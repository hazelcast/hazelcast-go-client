/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestDuration(t *testing.T) {
	testCases := []struct {
		f    func(t *testing.T)
		name string
	}{
		{name: "TestDuration_String", f: durationStringTest},
		{name: "TestDuration_MarshalText", f: durationMarshalTextTest},
		{name: "TestLocalTimeToTime", f: durationToDurationTest},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.f)
	}
}

func durationStringTest(t *testing.T) {
	dr := types.Duration(time.Minute)
	assert.Equal(t, time.Minute.String(), dr.String())
}

func durationMarshalTextTest(t *testing.T) {
	dr := types.Duration(time.Minute)
	want := `1m0s`
	text, err := dr.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, want, string(text))
}

func durationToDurationTest(t *testing.T) {
	testCases := []struct {
		tyd types.Duration
		tid time.Duration
	}{
		{tyd: types.Duration(time.Second), tid: time.Second},
		{tyd: types.Duration(time.Minute), tid: time.Minute},
		{tyd: types.Duration(time.Hour), tid: time.Hour},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.tid, tc.tyd.ToDuration())
	}
}
