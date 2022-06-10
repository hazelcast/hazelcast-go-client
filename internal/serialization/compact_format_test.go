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
	"context"
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestBasic(t *testing.T) {
	mapName := "test"
	port := 59271
	compactSerializers := []serialization.CompactSerializer{EmployeeDTOCompactSerializer{}}
	it.CompactFormatTester(t, mapName, compactSerializers, port, func(t *testing.T, client1, client2 *hz.Client) {
		ctx := context.Background()
		employeeDTO := EmployeeDTO{age: 30, id: 102310312}
		map1 := it.MustValue(client1.GetMap(ctx, mapName)).(*hz.Map)
		it.Must(map1.Set(ctx, 1, employeeDTO))
		map2 := it.MustValue(client2.GetMap(ctx, mapName)).(*hz.Map)
		// todo: remove the following line when schema distribution is implemented
		it.Must(map2.Set(ctx, 2, EmployeeDTO{}))
		it.AssertEquals(t, it.MustValue(map2.Get(ctx, 1)), employeeDTO)
	})
}

/*
todo: Write the following tests after schema distribution is implemented

TestBasicQuery
TestJoinedMemberQuery
TestClusterRestart
*/
