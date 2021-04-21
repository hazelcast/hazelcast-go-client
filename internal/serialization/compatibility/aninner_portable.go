// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import "github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"

type AnInnerPortable struct {
	anInt  int32
	aFloat float32
}

func (*AnInnerPortable) FactoryID() int32 {
	return portableFactoryID
}

func (*AnInnerPortable) ClassID() int32 {
	return innerPortableClassID
}

func (ip *AnInnerPortable) WritePortable(writer serialization.PortableWriter) error {
	writer.WriteInt32("i", ip.anInt)
	writer.WriteFloat32("f", ip.aFloat)
	return nil
}

func (ip *AnInnerPortable) ReadPortable(reader serialization.PortableReader) error {
	ip.anInt = reader.ReadInt32("i")
	ip.aFloat = reader.ReadFloat32("f")
	return reader.Error()
}
