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
They are Compact Serializer, Identified Data Serializer, Portable Serializer, JSON Serializer.

We will use the following type for all examples in this section:

	type Employee struct {
		Surname string
	}

# Compact Serialization

Compact serialization has the following main features:

  - Separates the schema from the data and stores it per type, not per object.
    This approach results in less memory and bandwidth usage compared to other formats.
  - Does not require a class to implement an interface or change the source code of the class in any way.
  - Supports schema evolution which permits adding or removing fields, or changing the types of fields.
  - Platform and language independent.
  - Supports partial deserialization of fields during queries or indexing.

Hazelcast achieves these features by having well-known schemas of objects and replicating them across the cluster.
That approach enables members and clients to fetch schemas they don't have in their local registries.
Each serialized object carries just a schema identifier and relies on the schema distribution service or configuration to match identifiers with the actual schema.
Once the schemas are fetched, they are cached locally on the members and clients so that the next operations that use the schema do not incur extra costs.

Schemas help Hazelcast to identify the locations of the fields on the serialized binary data.
With this information, Hazelcast can deserialize individual fields of the data, without deserializing the whole binary.
This results in a better query and indexing performance.

Schemas can evolve freely by adding or removing fields.
Even the types of the fields can be changed.
Multiple versions of the schema may live in the same cluster and both the old and new readers may read the compatible parts of the data.
This feature is especially useful in rolling upgrade scenarios.

The Compact serialization does not require any changes in the user classes as it doesn't need a class to implement a particular interface.
Serializers might be implemented and registered separately from the classes.

The underlying format of the Compact serialized objects is platform and language independent.

NOTE: Compact serialization is promoted to the stable status in the 5.2 Hazelcast release.
Hazelcast Go Client can use compact serialization only with Hazelcast version 5.2 and higher.

In order to implement a compact serializer, serialization.CompactSerializer interface must be implemented.
The interface consists of four methods:

  - Type: Returns the type that the serializer works on.
  - TypeName: Choosing a type name will associate that name with the schema and will make the polyglot use cases, where there are multiple clients from different languages, possible.
    Serializers in different languages can work on the same data, provided that their read and write methods are compatible, and they have the same type name.
    If you evolve your class in the later versions of your application, by adding or removing fields, you should continue using the same type name for that class.
  - Read: Creates a value of the target type using by reading from a serialization.CompactReader.
  - Write: Writes the value by writing to a serialization.CompactWriter.

Here is how a compact serializer for the Employee type may be written:

	type EmployeeSerializer struct{}

	func (s EmployeeSerializer) Type() reflect.Type {
		return reflect.TypeOf(Employee{})
	}

	func (s EmployeeSerializer) TypeName() string {
		return "Employee"
	}

	func (s EmployeeSerializer) Read(r serialization.CompactReader) interface{} {
		return Employee{
			Surname: r.ReadString("surname")
		}
	}

	func (s SampleCompactSerializer) Write(w serialization.CompactWriter, value interface{}) {
		v := value.(Employee)
		w.WriteString("surname", v.Surname)
	}

Once the necessary methods are implemented, the serializer should be registered:

	var cfg hazelcast.Config
	cfg.Serialization.Compact.SetSerializers(&EmployeeSerializer{})

The table of supported types in compact serialization and their Java counterparts is below:

	Go                        Java
	============              =========
	bool                      boolean
	[]bool                    boolean[]
	*bool                     Boolean
	[]*bool                   Boolean[]
	int8                      byte
	[]int8                    byte[]
	*int8                     Byte
	[]*int8                   Byte[]
	int16                     short
	[]int16                   short[]
	*int16                    Short
	[]*int16                  Short[]
	int32                     int
	[]int32                   int[]
	*int32                    Integer
	[]*int32                  Integer[]
	int64                     long
	[]int64                   long[]
	*int64                    Long
	[]*int64                  Long[]
	float32                   float
	[]float32                 float32[]
	*float32                  Float
	[]*float32                Float[]
	float64                   double
	[]float64                 double[]
	*float64                  Double
	[]*float64                Double[]
	*string                   String
	[]*string                 String[]
	*types.Decimal            BigDecimal
	[]*types.Decimal          BigDecimal[]
	*types.LocalTime          LocalTime
	[]*types.LocalTime        LocalTime[]
	*types.LocalDate          LocalDate
	[]*types.LocalDate        LocalDate[]
	*types.LocalDateTime      LocalDateTime
	[]*types.LocalDateTime    LocalDateTime[]
	*types.OffsetDateTime     OffsetDateTime
	[]*types.OffsetDateTime   OffsetDateTime[]

See https://docs.hazelcast.com/hazelcast/latest/serialization/compact-serialization#supported-types for more information.

Compact serialized objects can be used in SQL statements, provided that mappings are created, similar to other serialization formats.

# Schema Evolution

Compact serialization permits schemas and classes to evolve by adding or removing fields, or by changing the types of fields.
More than one version of a class may live in the same cluster and different clients or members might use different versions of the class.

Hazelcast handles the versioning internally.
So, you don’t have to change anything in the classes or serializers apart from the added, removed, or changed fields.

Hazelcast achieves this by identifying each version of the class by a unique fingerprint.
Any change in a class results in a different fingerprint.
Hazelcast uses a 64-bit Rabin Fingerprint to assign identifiers to schemas, which has an extremely low collision rate.

Different versions of the schema with different identifiers are replicated in the cluster and can be fetched by clients or members internally.
That allows old readers to read fields of the classes they know when they try to read data serialized by a new writer.
Similarly, new readers might read fields of the classes available in the data, when they try to read data serialized by an old writer.

Assume that the two versions of the following Employee class lives in the cluster.

	// version 0
	type Employee struct {
		Surname string
	}

	// version1
	type Employee struct {
		Surname string
		Age int // newly added field
	}

Then, when faced with binary data serialized by the new writer, old readers will be able to read the following fields.

		func (s EmployeeSerializer) Read(r serialization.CompactReader) interface{} {
			v := Employee{}
			v.Surname = r.ReadString("surname")
	        // The new "Age" field is there, but the old reader does not
	        // know anything about it. Hence, it will simply ignore that field.
			return v
		}

Then, when faced with binary data serialized by the old writer, new readers will be able to read the following fields.
Also, Hazelcast provides convenient APIs to check the existence of fields in the data when there is no such field.

	func (s EmployeeSerializer) Read(r serialization.CompactReader) interface{} {
		v := Employee{}
		v.Surname = r.ReadString("surname")
		// Read the "age" if it exists, or use the default value 0.
		// r.ReadInt32("age") would panic if the "age" field does not exist in data.
		if r.getFieldKind("age") == serialization.FieldKindInt32 {
			v.Age = r.ReadInt32("age")
		}
		return v
	}

Note that, when an old reader reads data written by an old writer, or a new reader reads a data written by a new writer, they will be able to read all fields written.

One thing to be careful while evolving the class is to not have any conditional code in the Write method.
That method must write all the fields available in the current version of the class to the writer, with appropriate field names and types.
Write method of the serializer is used to extract a schema out of the object, hence any conditional code that may or may not run depending on the object in that method might result in an undefined behavior.

For more information about compact serialization, check out https://docs.hazelcast.com/hazelcast/latest/serialization/compact-serialization.

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

	const factoryID = 1
	const employeeClassID = 1

	func (e Employee) FactoryID() int32 {
		return factoryID
	}

	func (e Employee) ClassID() int32 {
		return employeeClassID
	}

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

	const factoryID = 1
	const employeeClassID = 1

	func (e Employee) FactoryID() int32 {
		return factoryID
	}

	func (e Employee) ClassID() int32 {
		return employeeClassID
	}

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
