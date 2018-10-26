# Table of Contents

* [Introduction](#introduction)
* [1. Getting Started](#1-getting-started)
  * [1.1. Requirements](#11-requirements)
  * [1.2. Working with Hazelcast Clusters](#12-working-with-hazelcast-clusters)
  * [1.3. Downloading and Installing](#13-downloading-and-installing)
  * [1.4. Basic Configuration](#14-basic-configuration)
    * [1.4.1. IMDG Configuration](#141-imdg-configuration)
    * [1.4.2. Hazelcast Client Configuration](#142-hazelcast-client-configuration)
  * [1.5. Basic Usage](#15-basic-usage)
  * [1.6. Code Samples](#16-code-samples)
* [2. Features](#2-features)
* [3. Configuration Overview](#3-configuration-overview)
* [4. Serialization](#4-serialization)
  * [4.1. IdentifiedDataSerializable Serialization](#41-identifieddataserializable-serialization)
  * [4.2. Portable Serialization](#42-portable-serialization)
  * [4.3. Custom Serialization](#43-custom-serialization)
  * [4.4. Global Serialization](#44-global-serialization)
* [5. Setting Up Client Network](#5-setting-up-client-network)
  * [5.1. Providing the Member Addresses](#51-providing-the-member-addresses)
  * [5.2. Setting Smart Routing](#52-setting-smart-routing)
  * [5.3. Setting Redo Operation](#53-enabling-redo-operation)
  * [5.4. Setting Connection Timeout](#54-setting-connection-timeout)
  * [5.5. Setting Connection Attempt Limit](#55-setting-connection-attempt-limit)
  * [5.6. Setting Connection Attempt Period](#56-setting-connection-attempt-period)
  * [5.7. Enabling Client TLS/SSL](#57-enabling-client-tlsssl)
  * [5.8. Enabling Hazelcast Cloud Discovery](#58-enabling-hazelcast-cloud-discovery)
* [6. Securing Client Connection](#6-securing-client-connection)
  * [6.1. TLS/SSL](#61-tlsssl)
    * [6.1.1. TLS/SSL for Hazelcast Members](#611-tlsssl-for-hazelcast-members)
    * [6.1.2. TLS/SSL for Hazelcast Go Clients](#612-tlsssl-for-hazelcast-go-clients)
    * [6.1.3. Mutual Authentication](#613-mutual-authentication)
* [7. Using Go Client with Hazelcast IMDG](#7-using-go-client-with-hazelcast-imdg)
  * [7.1. Go Client API Overview](#71-go-client-api-overview)
  * [7.2. Go Client Operation Modes](#72-go-client-operation-modes)
      * [7.2.1. Smart Client](#721-smart-client)
      * [7.2.2. Unisocket Client](#722-unisocket-client)
  * [7.3. Handling Failures](#73-handling-failures)
    * [7.3.1. Handling Client Connection Failure](#731-handling-client-connection-failure)
    * [7.3.2. Handling Retry-able Operation Failure](#732-handling-retry-able-operation-failure)
  * [7.4. Using Distributed Data Structures](#74-using-distributed-data-structures)
    * [7.4.1. Using Map](#741-using-map)
    * [7.4.2. Using MultiMap](#742-using-multimap)
    * [7.4.3. Using ReplicatedMap](#743-using-replicatedmap)
    * [7.4.4. Using Queue](#744-using-queue)
  * [7.5. Distributed Events](#75-distributed-events)
    * [7.5.1. Cluster Events](#751-cluster-events)
      * [7.5.1.1. Listening for Member Events](#7511-listening-for-member-events)
      * [7.5.1.2. Listening for Lifecycle Events](#7512-listening-for-lifecycle-events)
    * [7.5.2. Distributed Data Structure Events](#752-distributed-data-structure-events)
      * [7.5.2.1. Listening for Map Events](#7521-listening-for-map-events)
* [8. Development and Testing](#8-development-and-testing)
  * [8.1. Building and Using Client From Sources](#81-building-and-using-client-from-sources)
  * [8.2. Testing](#82-testing)
* [9. Support, License and Copyright](#9-support-license-and-copyright)


# Introduction

> **NOTE: This project is currently in active development.**

[![GoDoc](https://godoc.org/github.com/hazelcast/hazelcast-go-client?status.svg)](https://godoc.org/github.com/hazelcast/hazelcast-go-client)
[![Go Report Card](https://goreportcard.com/badge/github.com/hazelcast/hazelcast-go-client)](https://goreportcard.com/report/github.com/hazelcast/hazelcast-go-client)
<br />

This document explains Go client for Hazelcast which uses Hazelcast's Open Client Protocol 1.6. This client works with Hazelcast 3.6 and higher.

**Hazelcast** is a clustering and highly scalable data distribution platform. With its various distributed data structures, distributed caching capabilities, elastic nature and more importantly with so many happy users, Hazelcast is a feature-rich, enterprise-ready and developer-friendly in-memory data grid solution.

# 1. Getting Started

This chapter explains all the necessary things to start using Hazelcast GO Client including basic Hazelcast IMDG, IMDG and client
configuration and how to use distributed data structures with Hazelcast.

## 1.1. Requirements

- Windows, Linux or MacOS
- Go 1.9 or newer
- Java 6 or newer
- Hazelcast IMDG 3.6 or newer
- Latest Hazelcast Go Client

## 1.2. Working with Hazelcast Clusters

Hazelcast Go Client requires a working Hazelcast IMDG cluster to run. IMDG cluster handles storage and manipulation of the user data.
Clients are a way to connect to IMDG cluster and access such data.

IMDG cluster consists of one or more Hazelcast IMDG members. These members generally run on multiple virtual or physical machines
and are connected to each other via network. Any data put on the cluster is partitioned to multiple members transparent to the user.
It is therefore very easy to scale the system by adding new members as the data grows. IMDG cluster also offers resilience. Should
any hardware or software problem causes a crash to any member, the data on that member is recovered from backups and the cluster
continues to operate without any downtime. Hazelcast clients are an easy way to connect to an IMDG cluster and perform tasks on
distributed data structures that live on the cluster.

In order to use Hazelcast Go Client, we first need to setup an IMDG cluster.

### Setting Up an IMDG Cluster
There are multiple ways of starting an IMDG cluster easily. You can run standalone IMDG members by downloading and running jar files
from the website. You can embed IMDG members to your Java projects. The easiest way is to use [hazelcast-member tool](https://github.com/hazelcast/hazelcast-member-tool)
if you have brew installed in your computer. We are going to download jars from the website and run a standalone member for this guide.

#### Running Standalone Jars
Go to https://hazelcast.org/download/ and download `.zip` or `.tar` distribution of Hazelcast IMDG. Decompress the contents into any directory that you
want to run IMDG members from. Change into the directory that you decompressed the Hazelcast content. Go into `bin` directory. Use either
`start.sh` or `start.bat` depending on your operating system. Once you run the start script, you should see IMDG logs on the terminal.
Once you see some log similar to the following, your 1-member cluster is ready to use:
```
INFO: [192.168.0.3]:5701 [dev] [3.10.4]

Members {size:1, ver:1} [
	Member [192.168.0.3]:5701 - 65dac4d1-2559-44bb-ba2e-ca41c56eedd6 this
]

Sep 06, 2018 10:50:23 AM com.hazelcast.core.LifecycleService
INFO: [192.168.0.3]:5701 [dev] [3.10.4] [192.168.0.3]:5701 is STARTED
```


#### Using hazelcast-member Tool
`hazelcast-member` is a tool to make downloading and running IMDG members as easy as it could be. If you have brew installed, run the following commands:
```
brew tap hazelcast/homebrew-hazelcast
brew install hazelcast-member
hazelcast-member start
```
In order to stop the member, run the following command:
```
hazelcast-member stop
```
Find more information about `hazelcast-member` tool at https://github.com/hazelcast/hazelcast-member-tool

Refer to the official [Hazelcast IMDG Reference Manual](http://docs.hazelcast.org/docs/3.10.4/manual/html-single/index.html#getting-started) for more information regarding starting clusters.

## 1.3. Downloading and Installing

Following command installs Hazelcast Go client:

```
go get github.com/hazelcast/hazelcast-go-client
```
[For more details](https://github.com/hazelcast/hazelcast-go-client/tree/master/sample/helloworld)

## 1.4. Basic Configuration
If you are using Hazelcast IMDG and Go Client on the same computer, generally default configuration just works. This is great for
trying out the client. However, if you run the client on a different computer than any of the cluster members, you may
need to do some simple configuration such as specifying the member addresses.

The IMDG members and clients have their own configuration options. You may need to reflect some of the member side configurations on the client side to properly connect to the cluster.
This section describes the most common configuration elements to get you started in no time.
It discusses some member side configuration options to ease understanding Hazelcast's ecosystem. Then, client side configuration options
regarding cluster connection are discussed. Configuration material regarding data structures are discussed in the following sections.
You can refer to [IMDG Documentation](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html) and [Configuration Overview](#configuration-overview) for more information.

### 1.4.1. IMDG Configuration
Hazelcast IMDG aims to run out of the box for most common scenarios. However if you have limitations on your network such as multicast being disabled,
you may have to configure your Hazelcast IMDG instances so that they can find each other on the network. Also most data structures are configurable.
Therefore, you may want to configure your Hazelcast IMDG. We will show you the basics about network configuration here.

There are two ways to configure Hazelcast IMDG. One is to use a `hazelcast.xml` file and the other is to programmatically configure the
instance before starting it from Java code. Since we use standalone servers, we will use `hazelcast.xml` to configure our cluster members.

When you download and unzip `hazelcast-<version>.zip`, you see the `hazelcast.xml` in `bin` folder. When a Hazelcast member starts, it looks for
`hazelcast.xml` file to load configuration from. A sample `hazelcast.xml` is below. We will go over some important elements in the rest of this section.
```xml
<hazelcast>
    <group>
        <name>dev</name>
        <password>dev-pass</password>
    </group>
    <network>
        <port auto-increment="true" port-count="100">5701</port>
        <join>
            <multicast enabled="true">
                <multicast-group>224.2.2.3</multicast-group>
                <multicast-port>54327</multicast-port>
            </multicast>
            <tcp-ip enabled="false">
                <interface>127.0.0.1</interface>
                <member-list>
                    <member>127.0.0.1</member>
                </member-list>
            </tcp-ip>
        </join>
        <ssl enabled="false"/>
    </network>
    <partition-group enabled="false"/>
    <map name="default">
        <backup-count>1</backup-count>
    </map>
</hazelcast>
```

- `<group>`: Specifies which cluster this member belongs to. A member connects only to other members that are in the same group as
itself. You will see `<name>` and `<password>` tags with some pre-configured values. You may give your clusters different names so that they can
live in the same network without disturbing each other. Note that the cluster name should be the same across all members and clients that belong
 to the same cluster. `<password>` tag is not in use since Hazelcast 3.9. It is there for backward compatibility
purposes. You can remove or leave it as it is if you use Hazelcast 3.9 or later.
- `<network>`
    - `<port>`: Specifies the port number to be used by the member when it starts. Its default value is 5701 You can specify another port number, and if
     you set `auto-increment` to `true`, than Hazelcast will try subsequent ports until it finds an available port or `port-count` is reached.
    - `<join>`: Specifies the strategies to be used by the member to find other cluster members. Choose which strategy you want to
    use by setting its `enabled` attribute to `true` and the others to `false`.
        - `<multicast>`: Members find each other by sending multicast requests to the specified address and port. It is very useful if IP addresses
        of the members are not static.
        - `<tcp>`: This strategy uses a pre-configured list of known members to find an already existing cluster. It is enough for a member to
        find only one cluster member to connect to the cluster. The rest of the member list is automatically retrieved from that member. We recommend
        putting multiple known member addresses there to avoid disconnectivity should one of the members in the list is unavailable at the time
        of connection.

These configuration elements are enough for most connection scenarios. Now we will move onto configuration of the Go client.

### 1.4.2. Hazelcast Client Configuration

This section describes some network configuration settings to cover common use cases in connecting the client to a cluster. Refer to [Configuration Overview](#configuration-overview)
and the following sections for information about detailed network configuration and/or additional features of Hazelcast Go Client configuration.

An easy way to configure your Hazelcast Go Client is to create a `Config` object and set the appropriate options. Then you can
supply this object to your client at the startup.

**Configuration**

You need to create a `ClientConfig` object and adjust its properties. Then you can pass this object to the client when starting it.

```go
package main

import "github.com/hazelcast/hazelcast-go-client"

func main() {

	config := hazelcast.NewConfig()
	hazelcast.NewClientWithConfig(config)
}
```
---

If you run Hazelcast IMDG members in a different server than the client, you most probably have configured the members' ports and cluster
names as explained in the previous section. If you did, then you need to make certain changes to the network settings of your client.

### Group Settings

```
config := hazelcast.NewConfig()
config.GroupConfig().SetName("GROUP_NAME_OF_YOUR_CLUSTER")
```
> **NOTE: If you have a Hazelcast release older than `3.11`, you need to provide also a group password along with the group name.**

### Network Settings

You need to provide the ip address and port of at least one member in your cluster so the client finds it.

```
config := hazelcast.NewConfig()
config.NetworkConfig().AddAddress("some-ip-address:port")
hazelcast.NewClientWithConfig(config)
```
## 1.5. Basic Usage

Now that we have a working cluster and we know how to configure both our cluster and client, we can run a simple program to use a distributed map in Go client.

The following example first creates a programmatic configuration object. Then, it starts a client.

```go

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {

	config := hazelcast.NewConfig()  // We create a config for illustrative purposes.
                                    // We do not adjust this config. Therefore it has default settings.

	client, err := hazelcast.NewClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(client.Name()) // Connects and prints the name of the client

}

```
This should print logs about the cluster members and information about the client itself such as client type, uuid and address.

```
2018/10/24 16:16:16 New State :  STARTING
2018/10/24 16:16:16 

Members {size:2} [
	Member localhost:5701 - 923f0f91-9bc8-432f-9650-fd4a5436e80b
	Member localhost:5702 - c01a31c1-e90d-4a63-a9b1-f323606431ec
]

2018/10/24 16:16:16 Registered membership listener with ID  400022bd-dcbe-4cf5-b2c1-9e41cf6e16d9
2018/10/24 16:16:16 New State :  CONNECTED
2018/10/24 16:16:16 New State :  STARTED


```
Congratulations, you just started a Hazelcast Go Client.

**Using a Map**

Let us manipulate a distributed map on a cluster using the client.

**IT.go**

```go
import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	config := hazelcast.NewConfig()
	client, err := hazelcast.NewClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	personnelMap, _ := client.GetMap("personnelMap")
	personnelMap.Put("Alice", "IT")
	personnelMap.Put("Bob", "IT")
	personnelMap.Put("Clark", "IT")
	fmt.Println("Added IT personnel. Logging all known personnel")
	resultPairs, _ := personnelMap.EntrySet()
	for _, pair := range resultPairs {
		fmt.Println(pair.Key(), " is in ", pair.Value(), " department")
	}
}

```
**Output**

```
2018/10/24 16:23:26 New State :  STARTING
2018/10/24 16:23:26 

Members {size:2} [
	Member localhost:5701 - 923f0f91-9bc8-432f-9650-fd4a5436e80b
	Member localhost:5702 - c01a31c1-e90d-4a63-a9b1-f323606431ec
]

2018/10/24 16:23:26 Registered membership listener with ID  199b9d1a-9085-4f1e-b6da-3a15a7757637
2018/10/24 16:23:26 New State :  CONNECTED
2018/10/24 16:23:26 New State :  STARTED
Added IT personnel. Logging all known personnel
Alice  is in  IT  department
Clark  is in  IT  department
Bob  is in  IT  department

```

You see this example puts all IT personnel into a cluster-wide `personnelMap` and then prints all known personnel.

**Sales.go**

```go
import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	config := hazelcast.NewConfig()
	client, err := hazelcast.NewClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	personnelMap, _ := client.GetMap("personnelMap")
	personnelMap.Put("Denise", "Sales")
	personnelMap.Put("Erwin", "Sales")
	personnelMap.Put("Faith", "Sales")
	fmt.Println("Added Sales personnel. Logging all known personnel")
	resultPairs, _ := personnelMap.EntrySet()
	for _, pair := range resultPairs {
		fmt.Println(pair.Key(), " is in ", pair.Value(), " department")
	}
}
```

**Output**

```
2018/10/24 16:25:58 New State :  STARTING
2018/10/24 16:25:58 

Members {size:2} [
	Member localhost:5701 - 923f0f91-9bc8-432f-9650-fd4a5436e80b
	Member localhost:5702 - c01a31c1-e90d-4a63-a9b1-f323606431ec
]

2018/10/24 16:25:58 Registered membership listener with ID  7014c382-182e-4962-94ff-d6094917d864
2018/10/24 16:25:58 New State :  CONNECTED
2018/10/24 16:25:58 New State :  STARTED
Added Sales personnel. Logging all known personnel
Erwin  is in  Sales  department
Alice  is in  IT  department
Clark  is in  IT  department
Bob  is in  IT  department
Denise  is in  Sales  department
Faith  is in  Sales  department

```

You will see this time we add only the sales employees but we get the list all known employees including the ones in IT.
That is because our map lives in the cluster and no matter which client we use, we can access the whole map.

## 1.6. Code Samples
Please see Hazelcast Go [code samples](https://github.com/hazelcast/hazelcast-go-client/blob/master/sample) for more examples.

You can also refer to Hazelcast Go [API Documentation](https://godoc.org/github.com/hazelcast/hazelcast-go-client).

# 2. Features

Hazelcast Go client supports the following data structures and features:

* Map (including entry processors and `PartitionAware` keys)
* MultiMap
* List
* Set
* Queue
* Topic
* ReliableTopic
* ReplicatedMap
* Ringbuffer
* Query (Predicates)
* API configuration
* Event Listeners
* Flake Id Generator
* CRDT Counter
* Aggregations & Projections
* Lifecycle Service
* Smart and Unisocket Client operation
* Hazelcast Serialization (IdentifiedDataSerializable, Portable, Custom Serializers, Global Serializers)
* SSL/TLS Support With Mutual Authentication(Enterprise)
* Custom Credentials
* Hazelcast Cloud Discovery
* Statistics

# 3. Configuration Overview

You can configure Hazelcast Go Client programmatically (API).

For programmatic configuration of the Hazelcast Go Client, just instantiate a `ClientConfig` object and configure the
desired aspects. An example is shown below.

```
config := hazelcast.NewConfig()
config.NetworkConfig().AddAddress("some-ip-address:port")
hazelcast.NewClientWithConfig(config)
```

Refer to `ClientConfig` class documentation at [Hazelcast Go Client API Docs](https://godoc.org/github.com/hazelcast/hazelcast-go-client/config#Config) for details.



# 4. Serialization


Serialization is the process of converting an object into a stream of bytes to store the object in memory, a file or database, or transmit it through network. Its main purpose is to save the state of an object in order to be able to recreate it when needed. The reverse process is called deserialization. Hazelcast offers you its own native serialization methods. You will see these methods throughout the chapter. For primitive types, it uses Hazelcast native serialization. For other complex types (e.g. Go objects), it uses Gob serialization.

> **NOTE: `int` and `[]int` types in Go Language are serialized as `int64` and `[]int64` respectively by Hazelcast Serialization.**
 
Note that if the object is not one of the above-mentioned types, the Go client uses `Gob serialization` by default.

However, `Gob Serialization` is not the best way of serialization in terms of performance and interoperability between the clients in different languages. If you want the serialization to work faster or you use the clients in different languages, Hazelcast offers its own native serialization types, such as [IdentifiedDataSerializable Serialization](#1-identifieddataserializable-serialization) and [Portable Serialization](#2-portable-serialization).

On top of all, if you want to use your own serialization type, you can use a [Custom Serialization](#3-custom-serialization).

## 4.1. IdentifiedDataSerializable Serialization

For a faster serialization of objects, Hazelcast recommends to implement IdentifiedDataSerializable interface.

Here is an example of an object implementing IdentifiedDataSerializable interface:

```go
type address struct {
	street  string
	zipcode int32
	city    string
	state   string
}

func (a *address) ReadData(input serialization.DataInput) error {
	a.street, _ = input.ReadUTF()
	a.zipcode, _ = input.ReadInt32()
	a.city, _ = input.ReadUTF()
	a.state, _ = input.ReadUTF()
	return nil
}

func (a *address) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(a.street)
	output.WriteInt32(a.zipcode)
	output.WriteUTF(a.city)
	output.WriteUTF(a.state)
	return nil
}

func (*address) FactoryID() int32 {
	return 1
}

func (*address) ClassID() int32 {
	return 1
}
```

IdentifiedDataSerializable uses `ClassID()` and `FactoryID()` to reconstitute the object. To complete the implementation `IdentifiedDataSerializableFactory` should also be implemented and registered into `SerializationConfig` which can be accessed from `config.SerializationConfig()`. The factory's responsibility is to return an instance of the right `IdentifiedDataSerializable` object, given the class id.

A sample `IdentifiedDataSerializableFactory` could be implemented as following:

```go

type myIdentifiedFactory struct{}

func (myIdentifiedFactory) Create(classID int32) serialization.IdentifiedDataSerializable {
	if classID == 1 {
		return &address{}
	}
	return nil
}
```

The last step is to register the `IdentifiedDataSerializableFactory` to the `SerializationConfig`.

```go
config := hazelcast.NewConfig()
config.SerializationConfig().AddDataSerializableFactory(1, myIdentifiedFactory{})
```

Note that the id that is passed to the `SerializationConfig` is same as the `FactoryID ` that `address` object returns.

## 4.2. Portable Serialization

As an alternative to the existing serialization methods, Hazelcast offers Portable serialization. To use it, you need to implement `Portable` interface. Portable serialization has the following advantages:

- Supporting multiversion of the same object type
- Fetching individual fields without having to rely on reflection
- Querying and indexing support without de-serialization and/or reflection

In order to support these features, a serialized Portable object contains meta information like the version and the concrete location of the each field in the binary data. This way Hazelcast is able to navigate in the binary data and de-serialize only the required field without actually de-serializing the whole object which improves the Query performance.

With multiversion support, you can have two nodes where each of them having different versions of the same object and Hazelcast will store both meta information and use the correct one to serialize and de-serialize Portable objects depending on the node. This is very helpful when you are doing a rolling upgrade without shutting down the cluster.

Also note that Portable serialization is totally language independent and is used as the binary protocol between Hazelcast server and clients.

A sample Portable implementation of a `Foo` class will look like the following:

```go
type foo struct {
	foo string
}

func (f *foo) WritePortable(writer serialization.PortableWriter) (err error) {
	writer.WriteUTF("foo", f.foo)
	return
}

func (f *foo) ReadPortable(reader serialization.PortableReader) (err error) {
	f.foo, _ = reader.ReadUTF("foo")
	return
}

func (*foo) FactoryID() int32 {
	return 1
}

func (*foo) ClassID() int32 {
	return 1
}
```

Similar to `IdentifiedDataSerializable`, a Portable object must provide `ClassID ` and `FactoryID `. The factory object will be used to create the Portable object given the classId.

A sample `PortableFactory` could be implemented as following:

```go
type myPortableFactory struct{}


func (myPortableFactory) Create(classID int32) serialization.Portable {
	if classID == 1 {
		return &foo{}
	}
	return nil
}
```

The last step is to register the `PortableFactory` to the `SerializationConfig`.

```go
config := hazelcast.NewConfig()
config.SerializationConfig().AddPortableFactory(1, myPortableFactory{})
```

Note that the id that is passed to the `SerializationConfig` is same as the `FactoryID` that `Foo` object returns.

## 4.3. Custom Serialization

Hazelcast lets you plug a custom serializer to be used for serialization of objects.

Let's say you have an object `Musician` and you would like to customize the serialization. The reason may be you want to use an external serializer for only one object.

```go
type musician struct{
	name string
}
```

Let's say your custom `MusicianSerializer` will serialize `Musician`.

```go
type MusicianSerializer struct {
}

func (*MusicianSerializer) ID() int32 {
	return 10
}

func (s *MusicianSerializer) Read(input serialization.DataInput) (interface{}, error) {
	l, _ := input.ReadInt64()
	buffer := make([]byte, l)
	for i := int64(0); i < l; i++ {
		buffer[i], _ = input.ReadByte()
	}
	name := string(buffer[:l])
	m := musician{name}
	return m, nil
}

func (s *MusicianSerializer) Write(output serialization.DataOutput, obj interface{}) error {
	m := obj.(musician)
	l := len(m.name)
	output.WriteInt64(int64(l))
	for i := 0; i < l; i++ {
		output.WriteByte(m.name[i]) 
	}
	return nil
}
```

Note that the serializer `id` must be unique as Hazelcast will use it to lookup the `MusicianSerializer` while it deserializes the object. Now the last required step is to register the `MusicianSerializer` to the configuration.

```go
musicianSerializer := &MusicianSerializer{}
config.SerializationConfig().AddCustomSerializer(reflect.TypeOf((*musician)(nil)).Elem(), musicianSerializer)
```

From now on, Hazelcast will use `MusicianSerializer` to serialize `Musician` objects.

## 4.4. Global Serialization

The global serializer is identical to custom serializers from the implementation perspective. The global serializer is registered as a fallback serializer to handle all other objects if a serializer cannot be located for them.

By default, Gob serialization is used if the object is not `IdentifiedDataSerializable` and `Portable` or there is no custom serializer for it. When you configure a global serializer, it is used instead of Gob serialization.

**Use cases**

- Third party serialization frameworks can be integrated using the global serializer.

- For your custom objects, you can implement a single serializer to handle all of them.

A sample global serializer that integrates with a third party serializer is shown below.

```go
type GlobalSerializer struct {
}

func (s *GlobalSerializer) ID() int32 {
	return 20
}

func (s *GlobalSerializer) Read(input serialization.DataInput) (interface{}, error) {
	return SomeThirdPartySerializer.deserialize(input.ReadByteArray())
}

func (s *GlobalSerializer) Write(output serialization.DataOutput, obj interface{}) error {
	return output.WriteByteArray(SomeThirdPartySerializer.serialize(object))
	
}
```

You should register the global serializer in the configuration.

```go
config.SerializationConfig().SetGlobalSerializer(&GlobalSerializer{})
```

# 5. Setting Up Client Network

All network related configuration of Hazelcast Go Client is performed via the `NetworkConfig` class when using programmatic configuration. 
Here is an example of configuring network for Go Client programmatically.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.AddAddress("10.1.1.21", "10.1.1.22:5703")
networkConfig.SetSmartRouting(true)
networkConfig.SetRedoOperation(true)
networkConfig.SetConnectionTimeout(6 * time.Second)
networkConfig.SetConnectionAttemptPeriod(5 * time.Second)
networkConfig.SetConnectionAttemptLimit(5)
```

## 5.1. Providing the Member Addresses

Address list is the initial list of cluster addresses to which the client will connect. The client uses this
list to find an alive member. Although it may be enough to give only one address of a member in the cluster
(since all members communicate with each other), it is recommended that you give the addresses for all the members.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.AddAddress("10.1.1.21", "10.1.1.22:5703")
```

If the port part is omitted, then 5701, 5702 and 5703 will be tried in random order.

You can specify multiple addresses with or without port information as seen above. The provided list is shuffled and tried in random order. Its default value is `localhost`.

## 5.2. Setting Smart Routing

Smart routing defines whether the client mode is smart or unisocket. See [Go Client Operation Modes section](#go-client-operation-modes)
for the description of smart and unisocket modes.

The following are example configurations.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.SetSmartRouting(true)
```

Its default value is `true` (smart client mode).

## 5.3. Enabling Redo Operation

It enables/disables redo-able operations. While sending the requests to related members, operations can fail due to various reasons. Read-only operations are retried by default. If you want to enable retry for the other operations, you can set the `redoOperation` to `true`.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.SetRedoOperation(true)
```

Its default value is `false` (disabled).

## 5.4. Setting Connection Timeout

Connection timeout is the timeout value in milliseconds for members to accept client connection requests.
If server does not respond within the timeout, the client will retry to connect as many as `ClientNetworkConfig.connectionAttemptLimit` times.

The following are the example configurations.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.SetConnectionTimeout(6 * time.Second)
```

Its default value is `5000` milliseconds.

## 5.5. Setting Connection Attempt Limit

While the client is trying to connect initially to one of the members in the `ClientNetworkConfig.addresses`, that member might not be available at that moment. Instead of giving up, throwing an error and stopping the client, the client will retry as many as `ClientNetworkConfig.connectionAttemptLimit` times. This is also the case when the previously established connection between the client and that member goes down.

The following are example configurations.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.SetConnectionAttemptLimit(5)
```

Its default value is `2`.

## 5.6. Setting Connection Attempt Period

Connection timeout period is the duration in milliseconds between the connection attempts 

The following are example configurations.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.SetConnectionAttemptPeriod(5 * time.Second)
```

Its default value is `3000` milliseconds.

## 5.7. Enabling Client TLS/SSL

You can use TLS/SSL to secure the connection between the clients and members. If you want TLS/SSL enabled
for the client-cluster connection, you should set an SSL configuration. Please see [TLS/SSL section](#1-tlsssl).

As explained in the [TLS/SSL section](#1-tlsssl), Hazelcast members have key stores used to identify themselves (to other members) and Hazelcast Go clients have certificate authorities used to define which members they can trust. Hazelcast has the mutual authentication feature which allows the Go clients also to have their private keys and public certificates and members to have their certificate authorities so that the members can know which clients they can trust. Please see the [Mutual Authentication section](#13-mutual-authentication).

## 5.8. Enabling Hazelcast Cloud Discovery

The purpose of Hazelcast Cloud Discovery is to provide clients to use IP addresses provided by `hazelcast orchestrator`. To enable Hazelcast Cloud Discovery, specify a token for the `discoveryToken` field and set the `enabled` field to `true`.

The following are example configurations.

```go
config.GroupConfig().SetName("hazel")
config.GroupConfig().SetPassword("cast")

cloudConfig := config.NetworkConfig().CloudConfig()
cloudConfig.SetDiscoveryToken("EXAMPLE_TOKEN")
cloudConfig.SetEnabled(true)
```

To be able to connect to the provided IP addresses, you should use secure TLS/SSL connection between the client and members. Therefore, you should set an SSL configuration as described in the previous section.

# 6. Securing Client Connection

This chapter describes the security features of Hazelcast Go Client. These include using TLS/SSL for connections between members and between clients and members and mutual authentication. These security features require **Hazelcast IMDG Enterprise** edition.

### 6.1. TLS/SSL

One of the offers of Hazelcast is the TLS/SSL protocol which you can use to establish an encrypted communication across your cluster with key stores and trust stores.

- A Java `keyStore` is a file that includes a private key and a public certificate. The equivalent of a key store is the combination of `key` and `cert` files at the Go client side.

- A Java `trustStore` is a file that includes a list of certificates trusted by your application which is named certificate authority. The equivalent of a trust store is a `ca` file at the Go client side.

You should set `keyStore` and `trustStore` before starting the members. See the next section how to set `keyStore` and `trustStore` on the server side.

#### 6.1.1. TLS/SSL for Hazelcast Members

Hazelcast allows you to encrypt socket level communication between Hazelcast members and between Hazelcast clients and members, for end to end encryption. To use it, see [TLS/SSL for Hazelcast Members section](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#tls-ssl-for-hazelcast-members).

#### 6.1.2. TLS/SSL for Hazelcast Go Clients

Hazelcast Go clients which support TLS/SSL should have the following user supplied SSLConfig

```go
config := hazelcast.NewConfig()
sslConfig := config.NetworkConfig().SSLConfig()
sslConfig.SetEnabled(true)
sslConfig.SetCaPath("yourCaPath")
```

#### 6.1.3. Mutual Authentication

As explained above, Hazelcast members have key stores used to identify themselves (to other members) and Hazelcast clients have trust stores used to define which members they can trust.

Using mutual authentication, the clients also have their key stores and members have their trust stores so that the members can know which clients they can trust.

To enable mutual authentication, firstly, you need to set the following property at server side by configuring `hazelcast.xml`:

```xml
<network>
    <ssl enabled="true">
        <properties>
            <property name="javax.net.ssl.mutualAuthentication">REQUIRED</property>
        </properties>
    </ssl>
</network>
```

You can see the details of setting mutual authentication on the server side in the [Mutual Authentication section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#mutual-authentication) of the Reference Manual.

Client side config needs to be set as follows:

```go
config := hazelcast.NewConfig()
sslConfig := config.NetworkConfig().SSLConfig()
sslConfig.SetEnabled(true)
sslConfig.SetCaPath("yourCaPath")
sslConfig.AddClientCertAndKeyPath("yourClientCertPath", "yourClientKeyPath")
sslConfig.ServerName = "yourServerName"
```

# 7. Using Go Client with Hazelcast IMDG

## 7.1. Go Client API Overview

If you are ready to go, let's start to use Hazelcast Go client!

The first step is configuration. You can configure the Go client programmatically.

```go
config := hazelcast.NewConfig()
config.GroupConfig().SetName("dev")
config.GroupConfig().SetPassword("pass")
config.NetworkConfig().AddAddress("10.1.1.21", "10.1.1.22:5703")
```

The second step is to initialize the `HazelcastClient` to be connected to the cluster.

```go
client, err := hazelcast.NewClientWithConfig(config)
```

**This client object is your gateway to access all Hazelcast distributed objects.**

Let’s create a map and populate it with some data.

```go
client, err := hazelcast.NewClientWithConfig(config)
if err != nil {
	fmt.Println(err)
	return
}
personnelMap, _ := client.GetMap("personnelMap")
personnelMap.Put("Denise", "Sales")
personnelMap.Put("Erwin", "Sales")
personnelMap.Put("Faith", "Sales")
```

As a final step, if you are done with your client, you can shut it down as shown below. This will release all the used resources and will close connections to the cluster.

```go
client.Shutdown()
```

## 7.2. Go Client Operation Modes

The client has two operation modes because of the distributed nature of the data and cluster.

### 7.2.1. Smart Client

In the smart mode, clients connect to each cluster member. Since each data partition uses the well known and consistent hashing algorithm, each client can send an operation to the relevant cluster member, which increases the overall throughput and efficiency. Smart mode is the default mode.


### 7.2.2. Unisocket Client

For some cases, the clients can be required to connect to a single member instead of each member in the cluster. Firewalls, security, or some custom networking issues can be the reason for these cases.

In the unisocket client mode, the client will only connect to one of the configured addresses. This single member will behave as a gateway to the other members. For any operation requested from the client, it will redirect the request to the relevant member and return the response back to the client returned from this member.

## 7.3. Handling Failures

There are two main failure cases you should be aware of, and configurations you can perform to achieve proper behavior.

### 7.3.1. Handling Client Connection Failure

While the client is trying to connect initially to one of the members in the `ClientNetworkConfig.SetAddresses`, all the members might be not available. Instead of giving up, throwing an error and stopping the client, the client will retry as many as `connectionAttemptLimit` times.

You can configure `connectionAttemptLimit` for the number of times you want the client to retry connecting. Please see [Setting Connection Attempt Limit](#5-setting-connection-attempt-limit).

The client executes each operation through the already established connection to the cluster. If this connection(s) disconnects or drops, the client will try to reconnect as configured.

### 7.3.2. Handling Retry-able Operation Failure

While sending the requests to related members, operations can fail due to various reasons. Read-only operations are retried by default. If you want to enable retry for the other operations, you can set the `redoOperation` to `true`. Please see [Enabling Redo Operation](#3-enabling-redo-operation).

You can set a timeout for retrying the operations sent to a member. This can be provided by using the property `hazelcast.client.invocation.timeout.seconds` in `config.SetProperty`. The client will retry an operation within this given period, of course, if it is a read-only operation or you enabled the `redoOperation` as stated in the above paragraph. This timeout value is important when there is a failure resulted by either of the following causes:

- Member throws an exception.

- Connection between the client and member is closed.

- Client’s heartbeat requests are timed out.

When a connection problem occurs, an operation is retried if it is certain that it has not run on the member yet or if it is idempotent such as a read-only operation, i.e., retrying does not have a side effect. If it is not certain whether the operation has run on the member, then the non-idempotent operations are not retried. However, as explained in the first paragraph of this section, you can force all client operations to be retried (`redoOperation`) when there is a connection failure between the client and member. But in this case, you should know that some operations may run multiple times causing conflicts. For example, assume that your client sent a `queue.offer` operation to the member, and then the connection is lost. Since there will be no response for this operation, you will not now whether it has run on the member or not. If you enabled `redoOperation`, it means this operation may run again, which may cause two instances of the same object in the queue.


## 7.4. Using Distributed Data Structures

Most of the Distributed Data Structures are supported by the Go client. In this chapter, you will learn how to use these distributed data structures.

### 7.4.1. Using Map

A Map usage example is shown below.

```go
m, _ := client.GetMap("myMap")
m.Put(1, "Furkan")
m.Get(1)
m.Remove(1)
```

### 7.4.2. Using MultiMap

A MultiMap usage example is shown below.

```go
multiMap, _ := client.GetMultiMap("myMultiMap")
multiMap.Put(1, "Furkan")
multiMap.Put(1, "Mustafa")
values, _ := multiMap.Get(1)
fmt.Println(values[0], " ", values[1]) //Furkan  Mustafa
```

### 7.4.3. Using ReplicatedMap

A ReplicatedMap usage example is shown below.

```go
replicatedMap, _ := client.GetReplicatedMap("myReplicatedMap")
replicatedMap.Put(1, "Furkan")
replicatedMap.Put(2, "Ahmet")
fmt.Println(replicatedMap.Get(2)) //Ahmet
```

### 7.4.4. Using Queue

A Queue usage example is shown below.

```go
queue, _ := client.GetQueue("myQueue")
queue.Offer("Furkan")
fmt.Println(queue.Peek()) //Furkan
```

## 7.5. Distributed Events

This chapter explains when various events are fired and describes how you can add event listeners on a Hazelcast Go client. These events can be categorized as cluster and distributed data structure events.

### 7.5.1. Cluster Events

You can add event listeners to a Hazelcast Go client. You can configure the following listeners to listen to the events on the client side.

`Membership Listener`: Notifies when a member joins to/leaves the cluster, or when an attribute is changed in a member.

`Distributed Object Listener`: Notifies when a distributed object is created or destroyed throughout the cluster.

`Lifecycle Listener`: Notifies when the client is starting, started, shutting down, and shutdown.

#### 7.5.1.1. Listening for Member Events

You can add the following types of member events to the `ClusterService`.

- `memberAdded`: A new member is added to the cluster.
- `memberRemoved`: An existing member leaves the cluster.

The following is a membership listener registration by using `client.Cluster().AddMembershipListener(&membershipListener{w})` function.

```go
type membershipListener struct {
}

func (l *membershipListener) MemberAdded(member core.Member) {
}

func (l *membershipListener) MemberRemoved(member core.Member) {
}
```

#### 7.5.1.2. Listening for Lifecycle Events

The Lifecycle Listener notifies for the following events:

- `STARTING`: A client is starting.
- `STARTED`: A client has started.
- `SHUTTING_DOWN`: A client is shutting down.
- `SHUTDOWN`: A client’s shutdown has completed.
- `CONNECTED`: A client is connected to cluster
- `DISCONNECTED`: A client is disconnected from cluster note that this does not imply shutdown

The following is an example of Lifecycle Listener that is added to config and its output.

```go
type lifecycleListener struct {
}

func (l *lifecycleListener) LifecycleStateChanged(newState string) {
	fmt.Println("Lifecycle Event >>> ", newState)
}

config.AddLifecycleListener(&lifecycleListener{})
```

Or it can be added later after client has started
```
registrationID := client.LifecycleService().AddLifecycleListener(&lifecycleListener{})
```

**Output:**

```
2018/10/26 16:16:51 New State :  STARTING
2018/10/26 16:16:51 

Lifecycle Event >>>  CONNECTED
Members {size:1} [
Lifecycle Event >>>  STARTED
	Member localhost:5701 - 936e0450-fc62-4927-9751-07c145f88a6f
]
Lifecycle Event >>>  SHUTTING_DOWN
Lifecycle Event >>>  SHUTDOWN

2018/10/26 16:16:51 Registered membership listener with ID  3e15ce02-4b14-4e4d-afca-bd69ea174498
2018/10/26 16:16:51 New State :  CONNECTED
2018/10/26 16:16:51 New State :  STARTED
2018/10/26 16:16:51 New State :  SHUTTING_DOWN
2018/10/26 16:16:51 New State :  SHUTDOWN
```

### 7.5.2. Distributed Data Structure Events

You can add event listeners to the Distributed Data Structures.

#### 7.5.2.1. Listening for Map Events

You can listen to map-wide or entry-based events. To listen to these events, you need to implement the relevant interfaces.

An entry-based  event is fired after the operations that affect a specific entry. For example, `IMap.put()`, `IMap.remove()` or `IMap.evict()`. An `EntryEvent` object is passed to the listener function.

- EntryExpiredListener
- EntryMergedListener
- EntryEvictedListener
- EntryUpdatedListener
- EntryRemovedListener
- EntryAddedListener

Let’s take a look at the following example.

```go
type entryListener struct {
}

func (l *entryListener) EntryAdded(event core.EntryEvent) {
	fmt.Println("Entry Added: ", event.Key(), " ", event.Value()) // Entry Added: 1 Furkan
}
```
To add listener and fire an event:

```
m, _ := client.GetMap("m")
m.AddEntryListener(&entryListener{}, true)
m.Put("1", "Furkan")
```


A map-wide event is fired as a result of a map-wide operation. For example, `IMap.clear()` or `IMap.evictAll()`. A `MapEvent` object is passed to the listener function.

- MapEvictedListener
- MapClearedListener

Let’s take a look at the following example.

```go
type mapListener struct {
}

func (l *mapListener) MapCleared(event core.MapEvent) {
	fmt.Println("Map Cleared:", event.NumberOfAffectedEntries()) // Map Cleared: 3
}
```
To add listener and fire a related event:
```
m, _ := client.GetMap("m")
m.AddEntryListener(&mapListener{}, true)
m.Put("1", "Mali")
m.Put("2", "Ahmet")
m.Put("3", "Furkan")

m.Clear()
```

# 8. Development and Testing

If you want to help with bug fixes, develop new features or tweak the implementation to your application's needs, 
you can follow the steps in this section.

## 8.1. Building and Using Client From Sources

Follow the below steps to build and install Hazelcast Go client from its source:

- Clone the GitHub repository [https://github.com/hazelcast/hazelcast-go-client.git](https://github.com/hazelcast/hazelcast-go-client.git).
- Run `sh build.sh`.

If you are planning to contribute, please run the style checker, as shown below, and fix the reported issues before sending a pull request.
- `sh linter.sh`

## 8.2. Testing
In order to test Hazelcast Go Client locally, you will need the following:
* Java 6 or newer
* Maven

Following command starts the tests:

```
sh local-test.sh
```

Test script automatically downloads `hazelcast-remote-controller` and Hazelcast IMDG. The script uses Maven to download those.

# 9. Support, License and Copyright

## Mail Group

Please join the mail group if you are interested in using or developing Hazelcast.

http://groups.google.com/group/hazelcast

## License

Hazelcast is available under the Apache 2 License. Please see the [Licensing appendix](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#license-questions) for more information.

## Copyright

Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com) for more information.
