// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package map1

import (
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/stretchr/testify/require"
)

func TestProxy_Destroy(t *testing.T) {
	name := "testMap"
	serviceName := bufutil.ServiceNameMap
	testMap, err := client.GetDistributedObject(serviceName, name)
	require.NoError(t, err)
	res, err := testMap.Destroy()

	if !res || err != nil {
		t.Error("Destroy() works wrong")
	}

	res, err = testMap.Destroy()

	if res || err != nil {
		t.Error("Destroy() works wrong")
	}
}

func TestProxy_GetDistributedObject(t *testing.T) {
	name := "testMap"
	serviceName := bufutil.ServiceNameMap
	mp, _ := client.GetDistributedObject(serviceName, name)
	mp2, _ := client.GetDistributedObject(serviceName, name)

	if !reflect.DeepEqual(mp, mp2) {
		t.Error("GetDistributedObject() works wrong")
	}
}
