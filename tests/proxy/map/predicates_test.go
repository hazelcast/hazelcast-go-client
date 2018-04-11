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
	. "github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core/predicates"
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"log"
	"reflect"
	"strconv"
	"testing"
)

var serializationService *SerializationService

func predicateTestInit() {
	defineSerializationService()
	fillMapForPredicates()
}

func defineSerializationService() {
	config := NewSerializationConfig()
	serializationService = NewSerializationService(config)
}

func fillMapForPredicates() {
	mp2, _ = client.GetMap("myMap2")
	for i := 0; i < 50; i++ {
		mp2.Put("key"+strconv.Itoa(i), int32(i))
	}
}

func testSerialization(t *testing.T, predicate interface{}) {
	predicateData, err := serializationService.ToData(predicate)
	if err != nil {
		t.Fatal(err)
	}
	retPredicate, err := serializationService.ToObject(predicateData)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(predicate, retPredicate.(interface{})) {
		t.Errorf("%s failed", reflect.TypeOf(predicate))
	}
}

func testPredicate(t *testing.T, predicate interface{}, expecteds map[interface{}]interface{}) {
	set, err := mp2.EntrySetWithPredicate(predicate)
	if err != nil {
		t.Fatal(err)
	}
	if len(set) != len(expecteds) {
		t.Errorf("%s failed", reflect.TypeOf(predicate))
	}
	for i := 0; i < len(set); i++ {
		if set[i].Value() != expecteds[set[i].Key()] {
			t.Errorf("%s failed", reflect.TypeOf(predicate))
		}
	}
}

func TestSql(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key10"] = int32(10)
	sql := predicates.Sql("this == 10")
	testSerialization(t, sql)
	testPredicate(t, sql, expecteds)
}

func TestAnd(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	and := predicates.And(predicates.Equal("this", int32(10)), predicates.Equal("this", int32(11)))
	testSerialization(t, and)
	testPredicate(t, and, expecteds)
}

func TestBetween(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	for i := 5; i < 29; i++ {
		expecteds["key"+strconv.Itoa(i)] = int32(i)
	}
	between := predicates.Between("this", int32(5), int32(28))
	testSerialization(t, between)
	testPredicate(t, between, expecteds)
}

func TestGreaterThan(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key48"] = int32(48)
	expecteds["key49"] = int32(49)
	greaterThan := predicates.GreaterThan("this", int32(47))
	testSerialization(t, greaterThan)
	testPredicate(t, greaterThan, expecteds)
}

func TestGreaterEqual(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key47"] = int32(47)
	expecteds["key48"] = int32(48)
	expecteds["key49"] = int32(49)
	greaterEqual := predicates.GreaterEqual("this", int32(47))
	testSerialization(t, greaterEqual)
	testPredicate(t, greaterEqual, expecteds)
}

func TestLessThan(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key0"] = int32(0)
	expecteds["key1"] = int32(1)
	expecteds["key2"] = int32(2)
	expecteds["key3"] = int32(3)
	lessThan := predicates.LessThan("this", int32(4))
	testSerialization(t, lessThan)
	testPredicate(t, lessThan, expecteds)
}

func TestLessEqual(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key0"] = int32(0)
	expecteds["key1"] = int32(1)
	expecteds["key2"] = int32(2)
	expecteds["key3"] = int32(3)
	expecteds["key4"] = int32(4)
	lessEqual := predicates.LessEqual("this", int32(4))
	testSerialization(t, lessEqual)
	testPredicate(t, lessEqual, expecteds)
}

func TestLike(t *testing.T) {
	localMap, _ := client.GetMap("likePredMap")
	localMap.Put("temp", "tempval")
	localMap.Put("temp1", "tempval1")
	localMap.Put("temp2", "val2")
	localMap.Put("temp3", "tempval3")
	like := predicates.Like("this", "tempv%")
	testSerialization(t, like)
	set, err := localMap.EntrySetWithPredicate(like)
	if err != nil {
		log.Fatal(err)
	}
	testMap := make(map[interface{}]interface{}, 0)
	testMap["temp"] = "tempval"
	testMap["temp1"] = "tempval1"
	testMap["temp3"] = "tempval3"

	if len(set) != len(testMap) {
		t.Errorf("like predicate failed")
	}

	for _, pair := range set {
		key := pair.Key()
		value := pair.Value()
		expectedValue, found := testMap[key]
		if !found || expectedValue != value {
			t.Errorf("like predicate failed")
		}
	}
}

func TestILike(t *testing.T) {
	localMap, _ := client.GetMap("ilikePredMap")
	localMap.Put("temp", "tempval")
	localMap.Put("TEMP", "TeMPVAL")
	localMap.Put("temp1", "teMpvAl1")
	localMap.Put("TEMP1", "TEMpVAL1")
	ilike := predicates.ILike("this", "tempv%")
	testSerialization(t, ilike)
	set, err := localMap.EntrySetWithPredicate(ilike)
	if err != nil {
		log.Fatal(err)
	}
	testMap := make(map[interface{}]interface{}, 0)
	testMap["temp"] = "tempval"
	testMap["TEMP"] = "TeMPVAL"
	testMap["temp1"] = "teMpvAl1"
	testMap["TEMP1"] = "TEMpVAL1"

	if len(set) != len(testMap) {
		t.Errorf("ilike predicate failed")
	}

	for _, pair := range set {
		key := pair.Key()
		value := pair.Value()
		expectedValue, found := testMap[key]
		if !found || expectedValue != value {
			t.Errorf("ilike predicate failed")
		}
	}
}

func TestIn(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key48"] = int32(48)
	expecteds["key49"] = int32(49)
	in := predicates.In("this", int32(48), int32(49), int32(50), int32(51), int32(52))
	testSerialization(t, in)
	testPredicate(t, in, expecteds)
}

func TestInstanceOf(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	for i := 0; i < 50; i++ {
		expecteds["key"+strconv.Itoa(i)] = int32(i)
	}
	instanceOf := predicates.InstanceOf("java.lang.Integer")
	testSerialization(t, instanceOf)
	testPredicate(t, instanceOf, expecteds)
}

func TestEqual(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key1"] = int32(1)
	equal := predicates.Equal("this", int32(1))
	testSerialization(t, equal)
	testPredicate(t, equal, expecteds)
}

func TestNotEqual(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	for i := 0; i < 49; i++ {
		expecteds["key"+strconv.Itoa(i)] = int32(i)
	}
	notEqual := predicates.NotEqual("this", int32(49))
	testSerialization(t, notEqual)
	testPredicate(t, notEqual, expecteds)
}

func TestNot(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key0"] = int32(0)
	expecteds["key1"] = int32(1)
	not := predicates.Not(predicates.GreaterEqual("this", int32(2)))
	testSerialization(t, not)
	testPredicate(t, not, expecteds)
}

func TestOr(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key0"] = int32(0)
	expecteds["key35"] = int32(35)
	expecteds["key49"] = int32(49)
	or := predicates.Or(predicates.GreaterEqual("this", int32(49)),
		predicates.Equal("this", int32(35)), predicates.LessEqual("this", int32(0)))
	testSerialization(t, or)
	testPredicate(t, or, expecteds)
}

func TestRegex(t *testing.T) {
	localMap, _ := client.GetMap("regexMap")
	localMap.PutAll(map[interface{}]interface{}{"06": "ankara", "07": "antalya"})
	rp := predicates.Regex("this", "^.*ya$")
	testSerialization(t, rp)
	set, _ := localMap.EntrySetWithPredicate(rp)
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["07"] = "antalya"
	if len(set) != len(expecteds) {
		t.Errorf("%s failed", reflect.TypeOf(rp))
	}
	for i := 0; i < len(set); i++ {
		if set[i].Value() != expecteds[set[i].Key()] {
			t.Errorf("%s failed", reflect.TypeOf(rp))
		}
	}
}

func TestFalse(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	false := predicates.False()
	testSerialization(t, false)
	testPredicate(t, false, expecteds)
}

func TestTrue(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	for i := 0; i < 50; i++ {
		expecteds["key"+strconv.Itoa(i)] = int32(i)
	}
	true := predicates.True()
	testSerialization(t, true)
	testPredicate(t, true, expecteds)
}
