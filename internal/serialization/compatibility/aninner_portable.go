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

package compatibility

import . "github.com/hazelcast/go-client/serialization"

type AnInnerPortable struct {
	anInt  int32
	aFloat float32
}

func (*AnInnerPortable) FactoryId() int32 {
	return PORTABLE_FACTORY_ID
}

func (*AnInnerPortable) ClassId() int32 {
	return INNER_PORTABLE_CLASS_ID
}

func (ip *AnInnerPortable) WritePortable(writer PortableWriter) {
	writer.WriteInt32("i", ip.anInt)
	writer.WriteFloat32("f", ip.aFloat)
}

func (ip *AnInnerPortable) ReadPortable(reader PortableReader) {
	ip.anInt, _ = reader.ReadInt32("i")
	ip.aFloat, _ = reader.ReadFloat32("f")
}
