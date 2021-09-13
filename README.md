
---

# Hazelcast Go Client

Hazelcast is an open-source distributed in-memory data store and computation platform that provides a wide variety of distributed data structures and concurrency primitives.

Hazelcast Go client is a way to communicate to Hazelcast 4 and 5 clusters and access the cluster data.

## Sample Code

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	ctx := context.TODO()
	// create the client and connect to the cluster
	client, err := hazelcast.StartNewClient(ctx) 
    if err != nil {
    	log.Fatal(err)
    }
    // get a map
    people, err := client.GetMap(ctx, "people")
    if err != nil {
        log.Fatal(err)
    }
    personName := "Jane Doe"
    // set a value in the map
    if err = people.Set(ctx, personName, 30); err != nil {
    	log.Fatal(err)
    }
    // get a value from the map
    age, err := people.Get(ctx, personName)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("%s is %d years old.\n", personName, age)
    // stop the client to release resources
    client.Shutdown(ctx)
}
```

## Features

* Distributed, partitioned and queryable in-memory key-value store implementation, called Map.
* Additional data structures and simple messaging constructs such as Replicated Map, Queue, List, PNCounter, Set and Topic and others.
* Support for serverless and traditional web service architectures with Unisocket and Smart operation modes.
* Go context support for all distributed data structures.
* Hazelcast Cloud integration.
* External smart client discovery.
* Hazelcast Management Center integration.
* Ability to listen to client lifecycle, cluster state, and distributed data structure events.
* And [more](https://hazelcast.com/clients/go/#client-features)...

## Install

Requirements:

* Hazelcast Go client is compatible only with Hazelcast IMDG 4.x and above.
* We support Go 1.15 and up.

In your Go module enabled project, add a dependency to `github.com/hazelcast/hazelcast-go-client`:
```shell
# Depend on a specific release
$ go get github.com/hazelcast/hazelcast-go-client@v1.1.0
```

## Quick Start

Hazelcast Go client requires a working Hazelcast IMDG cluster.
This cluster handles the storage and manipulation of the user data.

A Hazelcast IMDG cluster consists of one or more cluster members.
These members generally run on multiple virtual or physical machines and are connected to each other via the network.
Any data put on the cluster is partitioned to multiple members transparent to the user.
It is therefore very easy to scale the system by adding new members as the data grows.
Hazelcast IMDG cluster also offers resilience.
Should any hardware or software problem causes a crash to any member, the data on that member is recovered from backups and the cluster continues to operate without any downtime.

The quickest way to start a single member cluster for development purposes is to use our Docker images.

```
docker run --rm --name hazelcast -p 5701:5701 hazelcast/hazelcast:4.2
```

You can also use our ZIP or TAR [distributions](https://hazelcast.org/imdg/download/archives/#hazelcast-imdg).
After the download, you can start the Hazelcast member using the bin/start.sh script.

### Starting the Default Client

Start the client with the default Hazelcast IMDG host and port using `hazelcast.StartNewClient`: 

```go
ctx := context.TODO()
client, err := hazelcast.StartNewClient(ctx)
// handle client start error
```

### Starting the Client with Given Options

Note that `Config` structs are not thread-safe. Complete creation of the configuration in a single goroutine. 

```go
// create the default configuration
config := hazelcast.Config{}

// optionally set member addresses manually
config.Cluster.Network.SetAddresses("member1.example.com", "member2.example.com")

// create and start the client with the configuration provider
client, err := hazelcast.StartNewClientWithConfig(ctx, config)
// handle client start error
```

## Documentation

Hazelcast Go Client documentation is hosted at [pkg.go.dev](https://pkg.go.dev/github.com/hazelcast/hazelcast-go-client@v1.1.0).

You can view the documentation locally by using godoc:
```  
$ godoc -http=localhost:5500
```

godoc is not installed by default with the base Go distribution. Install it using:
```
$ go get -u golang.org/x/tools/...`
```

## Support

Join us at [Go Client channel](https://hazelcastcommunity.slack.com/channels/go-client) or [Hazelcast at Google Groups](https://groups.google.com/g/hazelcast).

## Running the tests

Currently, we support only Linux, MacOS and WSL (Windows Subsystem for Linux) for testing the client.

You need to have the following installed in order to run integration tests:
* Java 8
* Maven 3 or better
* Bash
* Make

Before running the tests, starts Hazelcast Remote Controller, which enables the test suite to create clusters:
```shell
# Start RC with Hazelcast Community features
$ ./rc.sh start

# Or, start RC with Hazelcast Enterprise features
$ HAZELCAST_ENTERPRISE_KEY=ENTERPRISE-KEY-HERE ./rc.sh start 
```

You can run the tests using one of the following approaches:
* Run `make test-all` to run integration tests.
* Run `make test-all-race` to run integration tests with race detection.
* Run `make test-cover` to generate the coverage report and `make view-cover` to view the test coverage summary and generate an HTML report.

Testing the client with SSL support requires running the remote controller with Hazelcast Enterprise features.
To enable SSL connections, add `ENABLE_SSL=1` to environment variables, or prepend it to the make commands above.

In order to turn on verbose logging, add `ENABLE_TRACE=1` to environment variables, or prepend it to the make commands above.

## License

[Apache 2 License](https://github.com/hazelcast/hazelcast-go-client/blob/master/LICENSE).

Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com) for more information.

