/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
Package driver provides a standard database/sql compatible SQL driver for Hazelcast.

This driver supports Hazelcast 5.0 and up. Check out the Hazelcast SQL documentation here: https://docs.hazelcast.com/hazelcast/latest/sql/sql-overview

The documentation for the database/sql package is here: https://pkg.go.dev/database/sql

Enabling Hazelcast SQL

The SQL support should be enabled in Hazelcast server configuration:

	<hazelcast>
		<jet enabled="true" />
	</hazelcast>

Creating a Driver Instance Using sql.Open

This driver provides two ways to create an instance.

The first one is via the standard sql.Open function.
That function takes two parameters, the driver name and the DSN (Data Source Name).
Here's a sample:

	db, err := sql.Open("hazelcast", "hz://@localhost:5701?cluster.name=dev")

Use hazelcast as the driver name.
The DSN may be blank. In that case, the default configuration is used.
Otherwise, the DSN must start with the scheme (hz://) and have the following optional parts:

	- Username and password for the cluster, separated by a column: dave:s3cr3t
	- Hazelcast member addresses, separated by commas: server1:port1,server2:port2
	- Options as key=value pairs, separated by ampersand (&). Both the key and value must be URL encoded: cluster.name=dev&ssl=true

Username/password part is separated from the address by the at sign (@).
There should be a question mark (?) between the address(es) and options.
Here is a full DSN:

	hz://dave:s3cr3t@my-server1.company.com:5000,my-server2.company.com:6000?cluster.name=prod&ssl=true&log=warn

The following are the available options:

	- unisocket: A boolean. Enables/disables the unisocket mode. Default: false. Example: unisocket=true
	- log: One of the following: off, fatal, error, warn, info, debug, trace. Default: info. Example: log=debug
	- cluster.name: A string. Specifies the cluster name. Default: dev. Example: cluster.name=hzc1
	- cloud.token: A string. Sets the Hazelcast Cloud token. Example: cloud.token=1234567abcde
	- stats.period: Duration between sending statistics, which can be parsed by time.Parse.
	  Use one of the following suffixes: s (seconds), m (minutes), h (hours). Example: stats.period=10s
	- ssl: A boolean. Enables/disables SSL connections. Defaults: false. Example: ssl=true
	- ssl.ca.path: The path to the PEM file for the certificate authority. Implies ssl=true. Example: ssl.ca.path=/etc/ssl/ca.pem
	- ssl.cert.path: The path to the TLS certificate. Implies ssl=true. Example: ssl.cert.path=/etc/ssl/cert.pem
	- ssl.key.path: The path to the certificate key. Implies ssl=true. Example: ssl.key.path=/etc/ssl/key.pem
	- ssl.key.password: The optional certificate password. Example: ssl.key.password=m0us3

Some items in the client configuration cannot be set in the DSN, such as serialization factories and SSL configuration.
You can use the following functions to set those configuration items globally:

	- SetSerializationConfig(...)
	- SetLoggerConfig(...)
	- SetSSLConfig(...)

Note that, these functions affect only the subsequent sql.Open calls, not the previous ones.

Here's an example:

	sc1 := &serialization.Config{}
	sc2 := &serialization.Config{}
	// no serialization configuration is used for the call below
	db1, err := sql.Open("hazelcast", "")
	// the following two sql.Open calls use sc1
	err = driver.SetSerializationConfig(sc1)
	db2, err := sql.Open("hazelcast", "")
	db3, err := sql.Open("hazelcast", "")
	// the following sql.Open call uses sc2
	err = driver.SetSerializationConfig(sc2)
	db4, err := sql.Open("hazelcast", "")

Creating a Driver Instance Using driver.Open

It is possible to create a driver instance using an existing Hazelcast client configuration using the driver.Open function.
All client configuration items, except listeners are supported.

	cfg := hazelcast.Config{}
	cfg.Cluster.Name = "prod"
	cfg.Serialization.SetPortableFactories(&MyPortableFactory{})
	db := driver.Open(cfg)

Executing Queries

database/sql package supports two kinds of queries: The ones returning rows (select statements and a few others) and the rest (insert, update, etc.).
The former kinds of queries are executed with QueryXXX methods and the latter ones are executed with ExecXXX methods of the sql.DB instance returned from sql.Open or driver.Open.

Use the question mark (?) for placeholders.

Here is an Exec example:

	q := `INSERT INTO person(__key, age, name) VALUES (?, ?, ?)`
	result, err := db.Exec(q, 1001, 35, "Jane Doe")
	// handle the error
	cnt, err := result.RowsAffected()
	// handle the error
	fmt.Printf("Affected rows: %d\n", cnt)

Note that LastInsertId is not supported and at the moment AffectedRows always returns 0.

An example Query call:

	q :=`SELECT name, age FROM person WHERE age >= ?`
	rows, err := db.Query(q, 30)
	// handle the error
	defer rows.Close()
	var name string
	var age int
	for rows.Next() {
		err := rows.Scan(&name, &age)
		// handle the error
		fmt.Println(name, age)
	}

Context variants of Query and Exec, such as QueryContext and ExecContext are fully supported.
They can be used to pass Hazelcast specific parameters, such as the cursor buffer size.
See the Passing Hazelcast Specific Parameters section below.

Passing Hazelcast-Specific Parameters

This driver supports the following extra query parameters that Hazelcast supports:

	- Cursor buffer size: Size of the server-side buffer for rows.
	- Timeout: Maximum time a query is allowed to execute.

Checkout the documentation below for details.

The extra query parameters are passed in a context augmented using WithCursorBufferSize and WithQueryTimeout functions. Here is an example:

	// set the cursor buffer size to 10_000
	ctx := driver.WithCursorBufferSize(context.Background(), 10_000)
	// set the query timeout to 2 minutes
	ctx = driver.WithQueryTimeout(ctx, 2*time.Minute)
	// use the parameters above with any methods that uses that context
	rows, err := db.QueryContext(ctx, "select * from people")

Creating a Mapping

To connect to a data source and query it as if it is a table, a mapping should be created.
Currently, mappings for Map, Kafka and file data sources are supported.

You can read the details about mappings here: https://docs.hazelcast.com/hazelcast/latest/sql/sql-overview#mappings

Supported Data Types

The following data types are supported when inserting/updating.
The names in parentheses correspond to SQL types:

	- string (varchar)
	- int8 (tinyint)
	- int16 (smallint)
	- int32 (integer)
	- int64 (bigint)
	- bool (boolean)
	- float32 (real)
	- float64 (double)
	- types.Decimal (decimal)
	- types.LocalDate (date)
	- types.LocalTime (time)
	- types.LocalDateTime (timestamp)
	- types.OffsetDateTime (timestamp with time zone)
	- time.Time (date) Detected by checking: hour == minute == second == nanoseconds = 0
	- time.Time (time) Detected by checking: year == 0, month == day == 1
	- time.Time (timestamp) Detected by checking: not time, timezone == time.Local
	- time.Time (timestamp with time zone) Detected by checking: not time, timezone != time.Local
	- serialization.JSON (json)

Using Date/Time

time.Time values are automatically serialized to the correct type.

In order to force using a specific date/time type, create a time.Time value and cast it to the target type:

	t := time.Now()
	dateValue := types.LocalDate(t)
	timeValue := types.LocalTime(t)
	dateTimeValue := types.LocalDateTime(t)
	dateTimeWithTimezoneValue := types.OffsetDateTime(t)

Using Raw Values

You can directly use one of the supported data types.

Creating a mapping:

	CREATE MAPPING person
	TYPE IMAP
	OPTIONS (
		'keyFormat' = 'int',
		'valueFormat' = 'varchar'
	)

Inserting rows:

	INSERT INTO person VALUES(100, 'Jane Doe')

Querying rows:

	SELECT __key, this from person

Using JSON

Two different JSON types are supported, namely "json-flat" and "json"

Checkout https://docs.hazelcast.com/hazelcast/5.1-beta-1/sql/working-with-json for more details.

1) "json-flat" value format treats top level fields of the json object as separate columns. It does not support nested JSON values.

Assuming the following JSON value:

	{
		"age": 35,
		"name": "Jane Doe"
	}

Some or all fields of the JSON value may be mapped and used.

Creating a mapping:

	CREATE MAPPING person (
		__key BIGINT,
		age BIGINT,
		name VARCHAR
	)
	TYPE IMAP
	OPTIONS (
		'keyFormat' = 'bigint',
		'valueFormat' = 'json-flat'
	)

Inserting rows:

	INSERT INTO person VALUES(100, 35, 'Jane Doe')

Querying rows:

	SELECT __key, name FROM person WHERE age > 30

2) "json" is a first-class SQL type with IMap and Kafka Connector support.

Creating a mapping:

	CREATE MAPPING person (
		__key BIGINT,
		this  JSON
	)
	TYPE IMAP
	OPTIONS (
		'keyFormat' = 'bigint',
		'valueFormat' = 'json'
	)

Inserting rows:

	// Use serialization.JSON type to specify a JSON value.
	INSERT INTO person VALUES(100, serialization.JSON(fmt.Sprintf(`{"age":%d, "name":%s}`, 35, 'Jane Doe')))

Querying rows:
Error handling is omitted to keep the example short.
	// Use serialization.JSON type to scan JSON string.
	q := fmt.Sprintf(`SELECT this FROM "%s" WHERE CAST(JSON_VALUE(this, '$.age') AS DOUBLE) > ?`, mapName)
	rows, err := db.Query(q, minAge)
	defer rows.Close()
	for rows.Next() {
		var js serialization.JSON
		rows.Scan(&js)
	}

Supported JSON related operations:

1. JSON_QUERY returns a json-object/array by the given JSON path.
    JSON_QUERY(jsonArg VARCHAR|JSON, jsonPath VARCHAR ... <extended syntax>)

2. JSON_VALUE returns a primitive value as varchar by the given JSON path.
	JSON_VALUE(jsonArg VARCHAR|JSON, jsonPath VARCHAR ... )

3. CAST can cast VARCHAR, columns/literals and dynamic params to JSON.
	CAST(x AS JSON)

Using Portable

Portable example:

Assuming the following portable type:

	type Person struct {
		Name string
		Age int16
	}

	func (r Person) FactoryID() int32 {
		return 100
	}

	func (r Person) ClassID() int32 {
		return 1
	}

	func (r Person) WritePortable(wr serialization.PortableWriter) {
		wr.WriteString("name", r.Name)
		wr.WriteInt16("age", r.Age)
	}

	func (r *Person) ReadPortable(rd serialization.PortableReader) {
		r.Name = rd.ReadString("name")
		r.Age = rd.ReadInt16("age")
	}

Creating a mapping:

	CREATE MAPPING person (
		__key BIGINT,
		age TINYINT,
		name VARCHAR
	)
	TYPE IMAP
	OPTIONS (
		'keyFormat' = 'bigint',
		'valueFormat' = 'portable',
		'valuePortableFactoryId' = '100',
		'valuePortableClassId' = '1'
	)

Querying rows:

	SELECT __key, name FROM person WHERE age > 30

*/
package driver
