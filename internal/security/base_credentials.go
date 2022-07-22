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

package security

import (
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	factoryID = 1
	classID   = 1
)

// BaseCredentials is base implementation for Credentials interface.
type BaseCredentials struct {
	endpoint  atomic.Value
	principal string
}

func (bc *BaseCredentials) FactoryID() int32 {
	return factoryID
}

func (bc *BaseCredentials) ClassID() int32 {
	return classID
}

func (bc *BaseCredentials) WritePortable(writer serialization.PortableWriter) {
	writer.WriteString("principal", bc.principal)
	writer.WriteString("endpoint", bc.Endpoint())
}

func (bc *BaseCredentials) ReadPortable(reader serialization.PortableReader) {
	bc.SetEndpoint(reader.ReadString("endpoint"))
	bc.principal = reader.ReadString("principal")
}

func (bc *BaseCredentials) Endpoint() string {
	return bc.endpoint.Load().(string)
}

func (bc *BaseCredentials) SetEndpoint(endpoint string) {
	bc.endpoint.Store(endpoint)
}

func (bc *BaseCredentials) Principal() string {
	return bc.principal
}
