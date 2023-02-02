/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

/*
Package serialization contains serialization functions and types for Hazelcast Go client.

Serialization is the process of converting an object into a stream of bytes to store the object in the memory, a file or database, or transmit it through the network.
Its main purpose is to save the state of an object in order to be able to recreate it when needed.
The reverse process is called deserialization.

Hazelcast serializes all your objects before sending them to the server.
The following table is the conversion of types for Java server side, which cannot be overriden by the user.

	Go              Java
	============    =========
	uint8 (byte)	Byte
	bool            Boolean
	uint16          Character
	int16           Short
	int32           Integer
	int64           Long
	int             Long
	float32         Float
	float64         Double
	string          String
	types.UUID      UUID
	time.Time       (See the table below)
	*big.Int		java.util.BigInteger
	types.Decimal	java.util.BigDecimal

Types of time.Time are converted to Java using the table below:

		Go                                                   Java
		============                                         =========
		time.Time with year == 0                             java.time.LocalTime
		time.Time with hours, minutes, secs, nanosecs == 0   java.time.localDate
		time.Time with location == time.Local                java.time.LocalDateTime
	    time.Time, otherwise                                 java.time.OffsetDateTime

Slices of the types above are serialized as arrays in the Hazelcast server side and the Hazelcast Java client.
Reference types are not supported for builtin types, e.g., *int64.

Hazelcast Go client supports several serializers apart from the builtin serializer for default types.
They are Identified Data Serializer, Portable Serializer, JSON Serializer.

We will use the following type for all examples in this section:

	const factoryID = 1
	const employeeClassID = 1

	type Employee struct {
		Surname string
	}

	func (e Employee) String() string {
		return fmt.Sprintf("Employee: %s", e.Surname)
	}

	// Common serialization methods

	func (e Employee) FactoryID() int32 {
		return factoryID
	}

	func (e Employee) ClassID() int32 {
		return employeeClassID
	}

# Identified Data Serialization

Hazelcast recommends implementing the Identified Data serialization for faster serialization of values.
See https://docs.hazelcast.com/imdg/latest/serialization/implementing-dataserializable.html#identifieddataserializable for details.

In order to be able to serialize/deserialize a custom type using Identified Data serialization, the type has to implement the serialization.IdentifiedDataSerializable interface and a factory which creates values of that type.
The same factory can be used to create values of all Identified Data Serializable types with the same factory ID.

Note that Identified Data Serializable factory returns a reference to the created value.
Also, it is important to use a reference to the value with the Hazelcast Go client API, since otherwise the value wouldn't be implementing serialization.IdentifiedDataSerializable.
That causes the value to be inadvertently serialized by the global serializer.

Here are functions that implement serialization.IdentifiedDataSerializable interface for Employee type.
FactoryID and ClassID were defined before and they are also required:

	func (e Employee) WriteData(output serialization.DataOutput) {
		output.WriteString(e.Surname)
	}

	func (e *Employee) ReadData(input serialization.DataInput) {
		e.Surname = input.ReadString()
	}

Here's an Identified Data Serializable factory that creates Employees:

	type IdentifiedFactory struct{}

	func (f IdentifiedFactory) FactoryID() int32 {
		return factoryID
	}

	func (f IdentifiedFactory) Create(classID int32) serialization.IdentifiedDataSerializable {
		if classID == employeeClassID {
			return &Employee{}
		}
		// given classID was not found in the factory
		return nil
	}

In order to use the factory, you have to register it:

	config := hazelcast.Config{}
	config.Serialization.SetIdentifiedDataSerializableFactories(&IdentifiedFactory{})

# Portable Serialization

Hazelcast offers portable serialization as an alternative to the existing serialization methods.
Portable serialization has the following advantages: Supports multiple versions of the same object type,
can fetch individual fields without having to rely on the reflection and supports querying and indexing without deserialization and/or reflection.
In order to support these features, a serialized Portable object contains meta information like the version and concrete location of the each field in the binary data.
This way Hazelcast is able to navigate in the binary data and deserialize only the required field without actually deserializing the whole object which improves the query performance.

You can have two members with each one having different versions of the same object by using multiversions.
Hazelcast stores meta information and uses the correct one to serialize and deserialize portable objects depending on the member.
That enables rolling upgrades without shutting down the cluster.

Also note that portable serialization is totally language independent and is used as the binary protocol between Hazelcast server and clients.
See https://docs.hazelcast.com/imdg/latest/serialization/implementing-portable-serialization.html for details.

In order to be able to serialize/deserialize a custom type using Portable serialization, the type has to implement the serialization.Portable interface and a factory which creates values of that type.
The same factory can be used to create values of all Portable types with the same factory ID.

Note that Portable factory returns a reference to the created value.
Also, it is important to use a reference to the value with the Hazelcast Go client API, since otherwise the value wouldn't be implementing serialization.Portable.
That causes the value to be inadvertently serialized by the global serializer.

Here are functions that implement serialization.Portable interface for Employee type:

	func (e Employee) WritePortable(writer serialization.PortableWriter) {
		writer.WriteString("surname", e.Surname)
	}

	func (e *Employee) ReadPortable(reader serialization.PortableReader) {
		e.Surname = reader.ReadString("surname")
	}

Here's a Portable factory that creates Employees:

	type PortableFactory struct{}

	func (p PortableFactory) FactoryID() int32 {
		return factoryID
	}

	func (f PortableFactory) Create(classID int32) serialization.Portable {
		if classID == employeeClassID {
			return &Employee{}
		}
		// given classID was not found in the factory
		return nil
	}

In order to use the factory, you have to register it:

	config := hazelcast.Config{}
	config.Serialization.SetPortableFactories(&PortableFactory{})

# JSON Serialization

Hazelcast has first class support for JSON.
You can put/get JSON values and use them in queries.
See https://docs.hazelcast.com/imdg/latest/query/how-distributed-query-works.html#querying-json-strings for details.

The data type which is used during serializing/deserializing JSON data is serialization.JSON.
It is in fact defined as []byte, but having a separate type helps the client to use the correct type ID when serializing/deserializing the value.
Note that the client doesn't check the validity of serialized/deserialized JSON data.

In order to use JSON serialized data with Hazelcast Go client, you have to cast a byte array which contains JSON to `serialization.JSON`.
You can use any JSON serializer to convert values to byte arrays, including json.Marshal in Go standard library.
Here is an example:

	employee := &Employee{Surname: "Schrute"}
	b, err := json.Marshal(employee)
	// mark the byte array as JSON, corresponds to HazelcastJsonValue
	jsonValue := serialization.JSON(b)
	// use jsonValue like any other value you can pass to Hazelcast
	err = myHazelcastMap.Set(ctx, "Dwight", jsonValue)

Deserializing JSON values retrieved from Hazelcast is also very easy.
Just make sure the retrieved value is of type serialization.JSON.

	v, err := myHazelcastMap.Get(ctx, "Angela")
	jsonValue, ok := v.(serialization.JSON)
	if !ok {
		panic("expected a JSON value")
	}
	otherEmployee := &Employee{}
	err = json.Unmarshal(jsonValue, &otherEmployee)

# Custom Serialization

Hazelcast lets you plug a custom serializer to be used for serialization of values.
See https://docs.hazelcast.com/imdg/latest/serialization/custom-serialization.html for details.

In order to use a custom serializer for a type, the type should implement serialization.Serializer interface.
Here is an example:

	type EmployeeCustomSerializer struct{}

	func (e EmployeeCustomSerializer) ID() (id int32) {
		return 45392
	}

	func (e EmployeeCustomSerializer) Read(input serialization.DataInput) interface{} {
		surname := input.ReadString()
		return Employee{Surname: surname}
	}

	func (e EmployeeCustomSerializer) Write(output serialization.DataOutput, object interface{}) {
		employee := object.(Employee)
		output.WriteString(employee.Surname)
	}

You should register the serializer in the configuration with the corresponding type:

	config := hazelcast.Config{}
	config.Serialization.SetCustomSerializer(reflect.TypeOf(Employee{}), &EmployeeCustomSerializer{})

# Global Serializer

If a serializer cannot be found for a value, the global serializer is used.
Values serialized by the global serializer are treated as blobs by Hazelcast, so using them for querying is not possible.

The default global serializer for Hazelcast Go client uses the Gob encoder: https://golang.org/pkg/encoding/gob/
Values serialized by the gob serializer cannot be used by Hazelcast clients in other languages.

You can change the global serializer by implementing the serialization.Serializer interface on a type:

	type MyGlobalSerializer struct{}

	func (s MyGlobalSerializer) ID() int32 {
		return 123456
	}

	func (s MyGlobalSerializer) Read(input serialization.DataInput) interface{} {
		surname := input.ReadString()
		return &Employee{Surname: surname}
	}

	func (s MyGlobalSerializer) Write(output serialization.DataOutput, object interface{}) {
		employee, ok := object.(*Employee)
		if !ok {
			panic("can serialize only Employee")
		}
		output.WriteString(employee.Surname)
	}

And setting it in the serialization configuration:

	config := hazelcast.Config{}
	config.Serialization.SetGlobalSerializer(&MyGlobalSerializer{})
*/
package serialization
