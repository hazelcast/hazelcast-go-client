[![GoDoc](https://godoc.org/github.com/hazelcast/hazelcast-go-client?status.svg)](https://godoc.org/github.com/hazelcast/hazelcast-go-client)
# Table of Contents

* [Hazelcast Go Client](#hazelcast-go-client)
* [Features](#features)
* [Installing the Client](#installing-the-client)
* [Using the Client](#using-the-client)
* [Serialization Considerations](#serialization-considerations)
* [Development](#development)
  * [Building And Installing from Sources](#building-and-installing-from-sources)
* [Testing](#testing)
* [Mail Group](#mail-group)
* [License](#license)
* [Copyright](#copyright)

# Hazelcast Go Client

Go client implementation for [Hazelcast](https://github.com/hazelcast/hazelcast), the open source in-memory data grid.

Go client is implemented using the [Hazelcast Open Binary Client Protocol](http://docs.hazelcast.org/docs/HazelcastOpenBinaryClientProtocol-Version1.0-Final.pdf). 

This client works with Hazelcast 3.6 and higher.

**Hazelcast** is a clustering and highly scalable data distribution platform. With its various distributed data structures, distributed caching capabilities, elastic nature and more importantly with so many happy users, Hazelcast is a feature-rich, enterprise-ready and developer-friendly in-memory data grid solution.

> **NOTE: This project is currently in active development.**

# Features

Hazelcast Go client supports the following data structures and features:

* Map (including entry processors and `PartitionAware` keys) and MultiMap
* Smart Client
* Hazelcast Native Serialization
* Distributed Object Listener
* Lifecycle Service

# Installing the Client

Following command installs Hazelcast Go client:

```
go get github.com/hazelcast/hazelcast-go-client
```
[For more details](https://github.com/hazelcast/hazelcast-go-client/tree/master/samples/hello-world/README.md)

# Using the Client

Following code snippet illustrates a simple usage of Map in Hazelcast Go Client.

```golang
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().AddAddress("127.0.0.1:5701")

	client, err := hazelcast.NewHazelcastClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	mp, _ := client.GetMap("simpleExample")

	mp.Put("key1", "value1")
	mp.Put("key2", "value2")

	result1, _ := mp.Get("key1")
	fmt.Println("key1 has the value of ", result1)

	result2, _ := mp.Get("key2")
	fmt.Println("key2 has the value of ", result2)

	mp.Clear()
	client.Shutdown()
```

Please see Hazelcast Go [code samples](https://github.com/hazelcast/hazelcast-go-client/blob/master/samples) for more examples.

You can also refer to Hazelcast Go [API Documentation](https://godoc.org/github.com/hazelcast/hazelcast-go-client).

# Serialization Considerations

Hazelcast needs to serialize objects in order to be able to keep them in the server memory. For primitive types, it uses Hazelcast native serialization. For other complex types (e.g. Go objects), it uses Gob serialization.

For example, when you try to query your data using predicates, this querying is handled on the server side so Hazelcast does not have to bring all data to the client but only the relevant entries. Otherwise, there would be a lot of unneccessary data traffic between the client and the server and the performance would severely drop.
Because predicates run on the server side, the server should be able to reason about your objects. That is why you need to implement serialization on the server side.

The same applies to MapStore. The server should be able to deserialize your objects in order to store them in MapStore.

Regarding arrays in a serializable object, you can use methods like `WriteInt32Array` if the array is of a primitive type.

If you have nested objects, these nested objects also need to be serializable. Register the serializers for nested objects and the method `WriteObject` will not have any problem with finding a suitable serializer for and writing/reading the nested object.

# Development

## Building And Installing from Sources

Follow the below steps to build and install Hazelcast Go client from its source:

- Clone the GitHub repository [https://github.com/hazelcast/hazelcast-go-client.git](https://github.com/hazelcast/hazelcast-go-client.git).
- Run `sh build.sh`.

# Testing

Following command starts the tests:

Run `sh localTest.sh`

### Mail Group

Please join the mail group if you are interested in using or developing Hazelcast.

http://groups.google.com/group/hazelcast

### License

Hazelcast is available under the Apache 2 License. Please see the [Licensing appendix](http://docs.hazelcast.org/docs/latest/manual/html-single/hazelcast-documentation.html#license-questions) for more information.

### Copyright

Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com) for more information.
