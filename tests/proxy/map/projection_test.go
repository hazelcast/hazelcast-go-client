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

package _map

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/predicates"
	"github.com/hazelcast/hazelcast-go-client/core/projection"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/tests/assert"
)

var mp3 core.Map

func projectionTestInit() {
	fillMapForProjections()
}

func fillMapForProjections() {
	mp3, _ = client.GetMap("myMap3")
	for i := 0; i < 10; i++ {
		var height = float32(170) + 0.1*float32(i)
		mp3.Put("key"+strconv.Itoa(i), &student2{int64(i), height})
	}

}

func testSerializationOfProjection(t *testing.T, projection interface{}) {
	projectionData, err := serializationService.ToData(projection)
	if err != nil {
		t.Fatal(err)
	}
	retProjection, err := serializationService.ToObject(projectionData)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(projection, retProjection) {
		t.Errorf("%s failed", reflect.TypeOf(projection))
	}
}

func testProjection(t *testing.T, typ reflect.Type, result []interface{}, err error, expected []int64) {
	length := len(result)
	resultSlice := make([]int64, length)
	for i := 0; i < length; i++ {
		resultSlice[i] = result[i].(int64)
	}
	sort.Slice(resultSlice, func(i, j int) bool { return resultSlice[i] < resultSlice[j] })
	assert.Equalf(t, err, resultSlice, expected, fmt.Sprintf("IMap.Project with %v failed", typ))
}

// The equivalent Student class of this struct is available on server side.
type student2 struct {
	age    int64
	height float32
}

func (*student2) FactoryID() (factoryID int32) {
	return 666
}

func (*student2) ClassID() (classID int32) {
	return 6
}

func (h *student2) WritePortable(writer serialization.PortableWriter) (err error) {
	writer.WriteInt64("age", h.age)
	writer.WriteFloat32("height", h.height)
	return
}

func (h *student2) ReadPortable(reader serialization.PortableReader) (err error) {
	h.age, _ = reader.ReadInt64("age")
	h.height, err = reader.ReadFloat32("height")
	return
}

type portableFactory struct {
}

func (*portableFactory) Create(classID int32) (instance serialization.Portable) {
	if classID == 6 {
		return &student2{}
	}
	return
}

func TestProject_SingleAttribute(t *testing.T) {
	projection, _ := projection.SingleAttribute("age")
	expected := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	result, err := mp3.Project(projection)
	testProjection(t, reflect.TypeOf(projection), result, err, expected)
	testSerializationOfProjection(t, projection)
}

func TestProject_NilProjection(t *testing.T) {
	_, err := mp3.Project(nil)
	if _, ok := err.(*core.HazelcastNilPointerError); !ok {
		t.Errorf("IMap.Project should return HazelcastNilPointerError")
	}
}

func TestProjectWithPredicate_SingleAttribute(t *testing.T) {
	projection, _ := projection.SingleAttribute("age")
	predicate := predicates.GreaterEqual("age", int64(7))
	result, err := mp3.ProjectWithPredicate(projection, predicate)
	expected := []int64{7, 8, 9}
	testProjection(t, reflect.TypeOf(projection), result, err, expected)
	testSerializationOfProjection(t, projection)
}

func TestProjectWithPredicate_NilProjection(t *testing.T) {
	predicate := predicates.GreaterEqual("age", int64(7))
	_, err := mp3.ProjectWithPredicate(nil, predicate)
	if _, ok := err.(*core.HazelcastNilPointerError); !ok {
		t.Errorf("IMap.ProjectWithPredicate should return HazelcastNilPointerError")
	}
}

func TestProjectWithPredicate_NilPredicate(t *testing.T) {
	projection, _ := projection.SingleAttribute("age")
	_, err := mp3.ProjectWithPredicate(projection, nil)
	if _, ok := err.(*core.HazelcastNilPointerError); !ok {
		t.Errorf("IMap.ProjectWithPredicate should return HazelcastNilPointerError")
	}
}

func TestSingleAttribute_EmptyAttributePath(t *testing.T) {
	_, err := projection.SingleAttribute("")
	if _, ok := err.(*core.HazelcastIllegalArgumentError); !ok {
		t.Errorf("projection.SingleAttribute should return HazelcastIllegalArgumentError")
	}
}
