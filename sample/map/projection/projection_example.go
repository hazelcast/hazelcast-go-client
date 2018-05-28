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

package main

import (
	"fmt"
	"strconv"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/projection"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type student struct {
	id int32
}

func (*student) FactoryID() (factoryID int32) {
	return 1
}

func (*student) ClassID() (classID int32) {
	return 1
}

func (s *student) WritePortable(writer serialization.PortableWriter) (err error) {
	writer.WriteInt32("id", s.id)
	return
}

func (s *student) ReadPortable(reader serialization.PortableReader) (err error) {
	s.id, err = reader.ReadInt32("id")
	return
}

func main() {
	config := hazelcast.NewConfig()
	config.NetworkConfig().AddAddress("127.0.0.1:5701")

	client, err := hazelcast.NewClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
	}
	mp, _ := client.GetMap("projectionExample")

	for i := 1; i < 50; i++ {
		mp.Put("key"+strconv.Itoa(i), &student{int32(i)})
	}

	p, _ := projection.NewSingleAttribute("id")

	set, err := mp.Project(p)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("IDs:")
	for _, value := range set {
		fmt.Println(value)
	}

	mp.Clear()
	client.Shutdown()
}
