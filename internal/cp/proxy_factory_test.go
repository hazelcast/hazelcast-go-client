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
package cp

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProxyFactory(t *testing.T) {
	tests := []struct {
		name       string
		f          func(t *testing.T)
		noParallel bool
	}{
		{name: "ObjectNameForProxy", f: objectNameForProxyTest, noParallel: false},
		{name: "ObjectNameForProxy_WithEmptyObjectName", f: objectNameForProxyWithEmptyObjectNameTest, noParallel: false},
		{name: "ObjectNameForProxy_WithEmptyProxyName", f: objectNameForProxyWithEmptyProxyNameTest, noParallel: false},
		{name: "WithoutDefaultGroupName", f: withoutDefaultGroupNameTest, noParallel: false},
		{name: "WithoutDefaultGroupName_WithMultipleGroupNames", f: withoutDefaultGroupNameWithMultipleGroupNamesTest, noParallel: false},
		{name: "WithoutDefaultGroupName_WithMetadataGroupName", f: withoutDefaultGroupNameWithMetadataGroupNameTest, noParallel: false},
	}
	// run no-parallel test first
	sort.Slice(tests, func(i, j int) bool {
		return tests[i].noParallel && !tests[j].noParallel
	})
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if !tc.noParallel {
				t.Parallel()
			}
			tc.f(t)
		})
	}
}

func objectNameForProxyTest(t *testing.T) {
	n, err := objectNameForProxy("test@default")
	require.NoError(t, err)
	require.Equal(t, "test", n)
	n, err = objectNameForProxy("test@custom")
	require.NoError(t, err)
	require.Equal(t, "test", n)
}

func objectNameForProxyWithEmptyObjectNameTest(t *testing.T) {
	_, err := objectNameForProxy("@default")
	assert.Error(t, err, "Object name cannot be empty string")
	_, err = objectNameForProxy("     @default")
	assert.Error(t, err, "Object name cannot be empty string")
}

func objectNameForProxyWithEmptyProxyNameTest(t *testing.T) {
	_, err := objectNameForProxy("test@")
	assert.Error(t, err, "Custom CP group name cannot be empty string")
	_, err = objectNameForProxy("test@    ")
	assert.Error(t, err, "Custom CP group name cannot be empty string")
}

func withoutDefaultGroupNameTest(t *testing.T) {
	n, err := withoutDefaultGroupName("test@default")
	require.NoError(t, err)
	require.Equal(t, n, "test")
	n, err = withoutDefaultGroupName("test@DEFAULT")
	require.NoError(t, err)
	require.Equal(t, n, "test")
	n, err = withoutDefaultGroupName("test@custom")
	require.NoError(t, err)
	require.Equal(t, n, "test@custom")
}

func withoutDefaultGroupNameWithMultipleGroupNamesTest(t *testing.T) {
	_, err := withoutDefaultGroupName("test@default@@default")
	require.Error(t, err, "Custom group name must be specified at most once")
}

func withoutDefaultGroupNameWithMetadataGroupNameTest(t *testing.T) {
	_, err := withoutDefaultGroupName("test@METADATA")
	require.Error(t, err, "CP data structures cannot run on the METADATA CP group!")
	_, err = withoutDefaultGroupName("test@metadata")
	require.Error(t, err, "CP data structures cannot run on the METADATA CP group!")
}
