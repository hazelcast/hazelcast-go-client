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

package assert

import (
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/core"
)

func Equalf(t *testing.T, err error, l interface{}, r interface{}, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(l, r) {
		t.Fatalf("%v != %v : %v", l, r, message)
	}
}

func LessThanf(t *testing.T, err error, l int64, r int64, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if l >= r {
		t.Fatalf("%v >= %v : %v", l, r, message)
	}
}

func Nilf(t *testing.T, err error, l interface{}, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if l != nil {
		t.Fatalf("%v != nil : %v", l, message)
	}
}

func ErrorNotNil(t *testing.T, err error, message string) {
	if err == nil {
		t.Fatal(message)
	}
}

func ErrorNil(t *testing.T, err error) {
	if err != nil {
		t.Error(err.Error())
	}
}

func MapEqualPairSlice(t *testing.T, err error, mp map[interface{}]interface{}, pairSlice []core.IPair, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if len(mp) != len(pairSlice) {
		t.Fatal(message)
	}
	for _, pair := range pairSlice {
		key := pair.Key()
		value := pair.Value()
		expectedValue, found := mp[key]
		if !found || expectedValue != value {
			t.Fatal(message)
		}
	}
}

func SlicesHaveSameElements(t *testing.T, err error, arg1 []interface{}, arg2 []interface{}, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if len(arg1) != len(arg2) {
		t.Fatal(message)
	}
	for _, elem := range arg1 {
		found := false
		for _, elem2 := range arg2 {
			if elem == elem2 {
				found = true
			}
		}
		if !found {
			t.Fatal(message)
		}
	}

}

func Equal(t *testing.T, err error, l interface{}, r interface{}) {
	if err != nil {
		t.Fatal(err)
	}
	if l != r {
		t.Fatalf("%v != %v", l, r)
	}

}
