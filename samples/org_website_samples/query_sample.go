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

package org_website_samples

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/predicates"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	userClassID   = 1
	userFactoryID = 1
)

type User struct {
	username string
	age      int32
	active   bool
}

func newUser(username string, age int32, active bool) *User {
	return &User{
		username: username,
		age:      age,
		active:   active,
	}
}

func (u *User) FactoryID() int32 {
	return userFactoryID
}

func (u *User) ClassID() int32 {
	return userClassID
}

func (u *User) WritePortable(writer serialization.PortableWriter) error {
	writer.WriteUTF("username", u.username)
	writer.WriteInt32("age", u.age)
	writer.WriteBool("active", u.active)
	return nil
}

func (u *User) ReadPortable(reader serialization.PortableReader) error {
	var err error
	u.username, err = reader.ReadUTF("username")
	if err != nil {
		return err
	}
	u.age, err = reader.ReadInt32("age")
	if err != nil {
		return err
	}
	u.active, err = reader.ReadBool("active")
	return err
}

type ThePortableFactory struct {
}

func (pf *ThePortableFactory) Create(classID int32) serialization.Portable {
	if classID == userClassID {
		return &User{}
	}
	return nil
}

func generateUsers(users core.IMap) {
	users.Put("Rod", newUser("Rod", 19, true))
	users.Put("Jane", newUser("Jane", 20, true))
	users.Put("Freddy", newUser("Freddy", 23, true))
}

func querySampleRun() {
	clientConfig := hazelcast.NewHazelcastConfig()
	clientConfig.SerializationConfig().
		AddPortableFactory(userFactoryID, &ThePortableFactory{})
	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewHazelcastClientWithConfig(clientConfig)
	// Get a Distributed Map called "users"
	users, _ := hz.Map("users")
	// Add some users to the Distributed Map
	generateUsers(users)
	// Create a Predicate from a String (a SQL like Where clause)
	var sqlQuery = predicates.SQL("active AND age BETWEEN 18 AND 21)")
	// Creating the same Predicate as above but with a builder
	var criteriaQuery = predicates.And(
		predicates.Equal("active", true),
		predicates.Between("age", 18, 21))

	// Get result collections using the two different Predicates
	result1, _ := users.ValuesWithPredicate(sqlQuery)
	result2, _ := users.ValuesWithPredicate(criteriaQuery)
	// Print out the results
	fmt.Println(result1)
	fmt.Println(result2)
	// Shutdown this hazelcast client
	hz.Shutdown()
}
