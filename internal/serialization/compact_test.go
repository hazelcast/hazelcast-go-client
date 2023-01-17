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

package serialization_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/internal/it/skip"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestCompact(t *testing.T) {
	skip.If(t, "hz < 5.2")
	testCases := []struct {
		name string
		f    func(t *testing.T)
	}{
		{name: "Basic", f: compactBasicTest},
		{name: "BasicQuery", f: basicQueryTest},
		{name: "JoinedMemberQuery", f: joinedMemberQueryTest},
		{name: "ClusterRestart", f: clusterRestartTest},
		// TODO: add testEntryProcessor when generic record is supported
		// TODO: add testSchemaReplication when generic record is supported
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tc.f(t)
		})
	}
}

func compactBasicTest(t *testing.T) {
	// ported from: com.hazelcast.internal.serialization.impl.compact.integration.CompactFormatIntegrationTest#testBasic
	tcx := it.CompactTestContext{
		T:           t,
		Serializers: []serialization.CompactSerializer{EmployeeDTOCompactSerializer{}},
	}
	tcx.Tester(func(tcx it.CompactTestContext) {
		ctx := context.Background()
		v := EmployeeDTO{age: 30, id: 102310312}
		map1 := it.MustValue(tcx.Client1.GetMap(ctx, tcx.MapName)).(*hz.Map)
		it.Must(map1.Set(ctx, 1, v))
		map2 := it.MustValue(tcx.Client2.GetMap(ctx, tcx.MapName)).(*hz.Map)
		it.AssertEquals(t, it.MustValue(map2.Get(ctx, 1)), v)
	})
}

func basicQueryTest(t *testing.T) {
	// ported from: com.hazelcast.internal.serialization.impl.compact.integration.CompactFormatIntegrationTest#testBasicQuery
	tcx := it.CompactTestContext{
		T:           t,
		Serializers: []serialization.CompactSerializer{EmployeeDTOCompactSerializer{}},
	}
	tcx.Tester(func(tcx it.CompactTestContext) {
		ctx := context.Background()
		map1 := it.MustValue(tcx.Client1.GetMap(ctx, tcx.MapName)).(*hz.Map)
		for i := int32(0); i < 100; i++ {
			it.MustValue(map1.Put(ctx, i, EmployeeDTO{age: i, id: 102310312}))
		}
		map2 := it.MustValue(tcx.Client2.GetMap(ctx, tcx.MapName)).(*hz.Map)
		ks := it.MustValue(map2.GetKeySetWithPredicate(ctx, predicate.SQL("age > 19"))).([]interface{})
		assert.Equal(t, 80, len(ks))
	})
}

func joinedMemberQueryTest(t *testing.T) {
	// ported from: com.hazelcast.internal.serialization.impl.compact.integration.CompactFormatIntegrationTest#testJoinedMemberQuery
	tcx := it.CompactTestContext{
		T:           t,
		Serializers: []serialization.CompactSerializer{EmployeeDTOCompactSerializer{}},
	}
	tcx.Tester(func(tcx it.CompactTestContext) {
		ctx := context.Background()
		for i := int32(0); i < 100; i++ {
			it.MustValue(tcx.M.Put(ctx, i, EmployeeDTO{age: i, id: 102310312}))
		}
		it.MustValue(tcx.Cluster.StartMember(ctx))
		err := tcx.ExecuteScript(ctx, fmt.Sprintf(`
			var m = instance_2.getMap("%s");
			var size = m.keySet(com.hazelcast.query.Predicates.sql("age > 19")).size();
			if (size != 80) {
				throw new Error("size is " + size);
			}			
		`, tcx.MapName))
		assert.NoError(t, err)
	})
}

func clusterRestartTest(t *testing.T) {
	// ported from: com.hazelcast.internal.serialization.impl.compact.integration.CompactFormatIntegrationTest#testJoinedMemberQuery
	tcx := it.CompactTestContext{
		T:           t,
		Serializers: []serialization.CompactSerializer{EmployeeDTOCompactSerializer{}},
	}
	tcx.Tester(func(tcx it.CompactTestContext) {
		ctx := context.Background()
		v := EmployeeDTO{age: 30, id: 102310312}
		k := int64(1)
		it.MustValue(tcx.M.Put(ctx, k, v))
		// changeCluster
		tcx.Cluster.Shutdown()
		it.MustValue(tcx.Cluster.StartMember(ctx))
		it.MustValue(tcx.M.Put(ctx, k, v))
		v2 := it.MustValue(tcx.M.Get(ctx, k))
		assert.Equal(t, v, v2)
	})
}
