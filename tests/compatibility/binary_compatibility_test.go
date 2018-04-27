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

package compatibility

import (
	"io/ioutil"
	"reflect"
	"strings"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

func TestBinaryCompatibility(t *testing.T) {
	var supporteds = []string{
		"1-NULL-BIG_ENDIAN",
		"1-NULL-LITTLE_ENDIAN",
		"1-Boolean-BIG_ENDIAN",
		"1-Boolean-LITTLE_ENDIAN",
		"1-Byte-BIG_ENDIAN",
		"1-Byte-LITTLE_ENDIAN",
		"1-Character-BIG_ENDIAN",
		"1-Character-LITTLE_ENDIAN",
		"1-Double-BIG_ENDIAN",
		"1-Double-LITTLE_ENDIAN",
		"1-Short-BIG_ENDIAN",
		"1-Short-LITTLE_ENDIAN",
		"1-Float-BIG_ENDIAN",
		"1-Float-LITTLE_ENDIAN",
		"1-Integer-BIG_ENDIAN",
		"1-Integer-LITTLE_ENDIAN",
		"1-Long-BIG_ENDIAN",
		"1-Long-LITTLE_ENDIAN",
		"1-String-BIG_ENDIAN",
		"1-String-LITTLE_ENDIAN",
		"1-AnInnerPortable-BIG_ENDIAN",
		"1-AnInnerPortable-LITTLE_ENDIAN",
		"1-boolean[]-BIG_ENDIAN",
		"1-boolean[]-LITTLE_ENDIAN",
		"1-byte[]-BIG_ENDIAN",
		"1-byte[]-LITTLE_ENDIAN",
		"1-char[]-BIG_ENDIAN",
		"1-char[]-LITTLE_ENDIAN",
		"1-double[]-BIG_ENDIAN",
		"1-double[]-LITTLE_ENDIAN",
		"1-short[]-BIG_ENDIAN",
		"1-short[]-LITTLE_ENDIAN",
		"1-float[]-BIG_ENDIAN",
		"1-float[]-LITTLE_ENDIAN",
		"1-int[]-BIG_ENDIAN",
		"1-int[]-LITTLE_ENDIAN",
		"1-long[]-BIG_ENDIAN",
		"1-long[]-LITTLE_ENDIAN",
		"1-String[]-BIG_ENDIAN",
		"1-String[]-LITTLE_ENDIAN",
		"1-AnIdentifiedDataSerializable-BIG_ENDIAN",
		"1-AnIdentifiedDataSerializable-LITTLE_ENDIAN",
		"1-APortable-BIG_ENDIAN",
		"1-APortable-LITTLE_ENDIAN",
	}

	var dataMap = make(map[string]*serialization.Data)

	dat, _ := ioutil.ReadFile("1.serialization.compatibility.binary")

	i := serialization.NewObjectDataInput(dat, 0, nil, true)
	var index int
	for i.Available() != 0 {
		objectKey, _ := i.ReadUTF()
		length, _ := i.ReadInt32()
		if length != common.NilArrayLength {
			payload := dat[i.Position() : i.Position()+length]
			i.SetPosition(i.Position() + length)
			if supporteds[index] == objectKey {
				dataMap[objectKey] = serialization.NewData(payload)
			}
		}
		index++
		if index == len(supporteds) {
			break
		}
	}

	serviceLE, _ := createSerializationService(false)
	serviceBE, _ := createSerializationService(true)

	objects := getAllTestObjects()

	var retObjects = make([]interface{}, len(supporteds)/2)

	var temp interface{}
	var temp2 interface{}
	for i := 0; i < len(supporteds); i++ {

		if strings.HasSuffix(supporteds[i], "BIG_ENDIAN") {
			temp, _ = serviceBE.ToObject(dataMap[supporteds[i]])
		} else {
			temp2, _ = serviceLE.ToObject(dataMap[supporteds[i]])
			if !reflect.DeepEqual(temp, temp2) {
				t.Error("compatibility test is incorrectly coded")
			}
		}
		retObjects[i/2] = temp
	}

	if !reflect.DeepEqual(objects, retObjects) {
		t.Error("go Serialization is not compatible with java")
	}

}

func createSerializationService(byteOrder bool) (*serialization.Service, error) {
	serConfing := config.NewSerializationConfig()
	pf := &aPortableFactory{}
	idf := &aDataSerializableFactory{}
	serConfing.AddPortableFactory(portableFactoryID, pf)
	serConfing.AddDataSerializableFactory(identifiedDataSerializableFactoryID, idf)

	if byteOrder {
		return serialization.NewSerializationService(serConfing)
	}
	serConfing.SetByteOrder(false)
	return serialization.NewSerializationService(serConfing)
}
