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

package nearcache_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

var inMemoryFormatTestCases = []struct {
	text     string
	fmt      nearcache.InMemoryFormat
	hasError bool
}{
	{
		text: "garbage",
		// not used
		fmt:      -1,
		hasError: true,
	},
	{
		text: "binary",
		fmt:  nearcache.InMemoryFormatBinary,
	},
	{
		text: "object",
		fmt:  nearcache.InMemoryFormatObject,
	},
}

func TestInMemoryFormat_UnmarshalText(t *testing.T) {
	for _, tc := range inMemoryFormatTestCases {
		f := func(t *testing.T) {
			var fmt nearcache.InMemoryFormat
			err := fmt.UnmarshalText([]byte(tc.text))
			if tc.hasError {
				if err == nil {
					t.Fatalf("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tc.fmt, fmt)
		}
		t.Run(tc.text, f)
		t.Run(strings.ToUpper(tc.text), f)
	}
}

func TestInMemoryFormat_MarshalText(t *testing.T) {
	for _, tc := range inMemoryFormatTestCases {
		t.Run(tc.text, func(t *testing.T) {
			b, err := tc.fmt.MarshalText()
			if tc.hasError {
				if err == nil {
					t.Fatalf("expected an error")
				}
				return
			}
			assert.Equal(t, tc.text, string(b))
		})
	}
}

var evictionPolicyTestCases = []struct {
	text     string
	policy   nearcache.EvictionPolicy
	hasError bool
}{
	{
		text: "garbage",
		// not used
		policy:   -1,
		hasError: true,
	},
	{
		text:   "lru",
		policy: nearcache.EvictionPolicyLRU,
	},
	{
		text:   "lfu",
		policy: nearcache.EvictionPolicyLFU,
	},
	{
		text:   "none",
		policy: nearcache.EvictionPolicyNone,
	},
	{
		text:   "random",
		policy: nearcache.EvictionPolicyRandom,
	},
}

func TestEvictionPolicy_UnmarshalText(t *testing.T) {
	for _, tc := range evictionPolicyTestCases {
		f := func(t *testing.T) {
			var p nearcache.EvictionPolicy
			err := p.UnmarshalText([]byte(tc.text))
			if tc.hasError {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tc.policy, p)
		}
		t.Run(tc.text, f)
		t.Run(strings.ToUpper(tc.text), f)
	}
}

func TestEvictionPolicy_MarshalText(t *testing.T) {
	for _, tc := range evictionPolicyTestCases {
		t.Run(tc.text, func(t *testing.T) {
			b, err := tc.policy.MarshalText()
			if tc.hasError {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tc.text, string(b))
		})
	}
}
