// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package proxy

import (
	. "github.com/hazelcast/go-client/core"
	"github.com/hazelcast/go-client/serialization"
	"log"
	"reflect"
	"strconv"
	"testing"
)

func fillMapForPredicates() {
	mapName2 := "myMap2"
	mp2, _ = client.GetMap(&mapName2)
	for i := 0; i < 50; i++ {
		mp2.Put("key"+strconv.Itoa(i), int32(i))
	}
}

func testPredicate(t *testing.T, predicate serialization.IPredicate, expecteds map[interface{}]interface{}) {
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
	testPredicate(t, Sql("this == 10"), expecteds)
}

func TestAnd(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	testPredicate(t, And([]serialization.IPredicate{Equal("this", int32(10)), Equal("this", int32(11))}), expecteds)
}

func TestBetween(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	for i := 5; i < 29; i++ {
		expecteds["key"+strconv.Itoa(i)] = int32(i)
	}
	testPredicate(t, Between("this", int32(5), int32(28)), expecteds)
}

func TestGreaterThan(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key48"] = int32(48)
	expecteds["key49"] = int32(49)
	testPredicate(t, GreaterThan("this", int32(47)), expecteds)
}

func TestGreaterEqual(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key47"] = int32(47)
	expecteds["key48"] = int32(48)
	expecteds["key49"] = int32(49)
	testPredicate(t, GreaterEqual("this", int32(47)), expecteds)
}

func TestLessThan(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key0"] = int32(0)
	expecteds["key1"] = int32(1)
	expecteds["key2"] = int32(2)
	expecteds["key3"] = int32(3)
	testPredicate(t, LessThan("this", int32(4)), expecteds)
}

func TestLessEqual(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key0"] = int32(0)
	expecteds["key1"] = int32(1)
	expecteds["key2"] = int32(2)
	expecteds["key3"] = int32(3)
	expecteds["key4"] = int32(4)
	testPredicate(t, LessEqual("this", int32(4)), expecteds)
}

func TestLike(t *testing.T) {
	name := "likePredMap"
	localMap, _ := client.GetMap(&name)
	localMap.Put("temp", "tempval")
	localMap.Put("temp1", "tempval1")
	localMap.Put("temp2", "val2")
	localMap.Put("temp3", "tempval3")
	set, err := localMap.EntrySetWithPredicate(Like("this", "tempv%"))
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
	name := "ilikePredMap"
	localMap, _ := client.GetMap(&name)
	localMap.Put("temp", "tempval")
	localMap.Put("TEMP", "TeMPVAL")
	localMap.Put("temp1", "teMpvAl1")
	localMap.Put("TEMP1", "TEMpVAL1")
	set, err := localMap.EntrySetWithPredicate(ILike("this", "tempv%"))
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
	testPredicate(t, In("this", []interface{}{int32(48), int32(49), int32(50), int32(51), int32(52)}), expecteds)
}

func TestInstanceOf(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	for i := 0; i < 50; i++ {
		expecteds["key"+strconv.Itoa(i)] = int32(i)
	}
	testPredicate(t, InstanceOf("java.lang.Integer"), expecteds)
}

func TestEqual(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key1"] = int32(1)
	testPredicate(t, Equal("this", int32(1)), expecteds)
}

func TestNotEqual(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	for i := 0; i < 49; i++ {
		expecteds["key"+strconv.Itoa(i)] = int32(i)
	}
	testPredicate(t, NotEqual("this", int32(49)), expecteds)
}

func TestNot(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key0"] = int32(0)
	expecteds["key1"] = int32(1)
	testPredicate(t, Not(GreaterEqual("this", int32(2))), expecteds)
}

func TestOr(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	expecteds["key0"] = int32(0)
	expecteds["key35"] = int32(35)
	expecteds["key49"] = int32(49)
	testPredicate(t, Or([]serialization.IPredicate{GreaterEqual("this", int32(49)), Equal("this", int32(35)), LessEqual("this", int32(0))}), expecteds)
}

func TestRegex(t *testing.T) {
	name := "regexMap"
	localMap, _ := client.GetMap(&name)
	localMap.PutAll(&map[interface{}]interface{}{"06": "ankara", "07": "antalya"})
	rp := Regex("this", "^.*ya$")
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
	testPredicate(t, False(), expecteds)
}

func TestTrue(t *testing.T) {
	expecteds := make(map[interface{}]interface{}, 0)
	for i := 0; i < 50; i++ {
		expecteds["key"+strconv.Itoa(i)] = int32(i)
	}
	testPredicate(t, True(), expecteds)
}
