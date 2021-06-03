
---

## Note to Hazelcast Go Client for Hazelcast 3 Users

Support for Hazelcast 3.x ended on April 9th 2021, so Go client for Hazelcast 3 will not be updated.
You can still use Go client for Hazelcast 3 by migrating your project to use Go modules and adding the following in your `go.mod` file:
```
require github.com/hazelcast/hazelcast-go-client v0.6.0
```

---

## Note

The master branch is the development branch for the upcoming version 1 of the client. Version 1 is currently in alpha stage:

* CODE IN THIS BRANCH IS IN NO WAY SUPPORTED FOR ANY PRODUCTION USE. 
* API IN THIS BRANCH CAN CHANGE IN BREAKING WAYS AT ANY TIME.

Your feedback and contribution are appreciated! Join us at [Hazelcast Community Slack](https://hazelcastcommunity.slack.com/channels/go-client)

---

# Hazelcast Go Client

Hazelcast is an open-source distributed in-memory data store and computation platform that provides a wide variety of distributed data structures and concurrency primitives.

Hazelcast Go client is a way to communicate to Hazelcast IMDG clusters and access the cluster data.

## Release Notes

### 1.0.0 Preview 2 (2021-05-21)

New features:

* [IList](https://docs.hazelcast.com/imdg/4.2/data-structures/list.html) distributed data structure,

Changes:
* `hazelcast.ConfigBuilder` is removed. Use `hazelcast.Config` instead.
* Signatures of serialization functions have changed to remove returned `error`s. If your serialization code needs to fail, simply `panic`. Panics are recovered and converted to `error`s.  
* `hzerrors` package is public.

Improvements:
* Serialization performance is increased, especially for large payloads.
* Memory utilization is improved and number of allocations are decreased. 
* Heartbeat service is enabled.

Fixes:
* Fixed a regression introduced in Preview 1 which limited the message size to 8KBs.
* Fixed Non-retryable errors.
* `Destroy` function of DSSs removes corresponding proxies from the cache.

### 1.0.0 Preview 1 (2021-05-07)

The first preview release of the Hazelcast Go client has the following features:

* Distributed data structures:
  1. [Map](https://docs.hazelcast.com/imdg/4.2/data-structures/map.html),
  2. [Replicated Map](https://docs.hazelcast.com/imdg/4.2/data-structures/replicated-map.html),
  3. [Queue](https://docs.hazelcast.com/imdg/4.2/data-structures/queue.html)
  4. [Topic](https://docs.hazelcast.com/imdg/4.2/data-structures/topic.html)
* Distributed queries via predicates. [Documentation](https://docs.hazelcast.com/imdg/4.2/query/how-distributed-query-works.html)
* [Lifecycle](https://docs.hazelcast.com/imdg/4.2/events/cluster-events.html#listening-for-lifecycle-events) and [cluster membership](https://docs.hazelcast.com/imdg/4.2/events/cluster-events.html#listening-for-member-events) event listeners.
* [Smart routing](https://docs.hazelcast.com/imdg/4.2/clients/java.html#java-client-operation-modes)
* Ownerless client.
* JSON, identified and portable serialization.
* SSL connections.

Expect breaking changes in the following areas:
* Configuration.
* Map lock functions.

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
	// create the client and connect to the cluster
	client, err := hazelcast.StartNewClient() 
    if err != nil {
    	log.Fatal(err)
    }
    // get a map
    people, err := client.GetMap("people")
    if err != nil {
        log.Fatal(err)
    }
    personName := "Jane Doe"
    // set a value in the map
    if err = people.Set(context.TODO(), personName, 30); err != nil {
    	log.Fatal(err)
    }
    // get a value from the map
    age, err := people.Get(context.TODO(), personName)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("%s is %d years old.\n", personName, age)
}
```

## Features

* Distributed, partitioned and queryable in-memory key-value store implementation, called Map.
* Additional data structures and simple messaging constructs such as Replicated Map, Queue, List, and Topic.
* Support for serverless and traditional web service architectures with Unisocket and Smart operation modes.
* Ability to listen to client lifecycle, cluster state, and distributed data structure events.

## Install

Requirements:

* Hazelcast Go client is compatible only with Hazelcast IMDG 4.x and above.

In your Go module enabled project, add a dependency to `github.com/hazelcast/hazelcast-go-client`:
```shell
# Depend on a specific release
$ go get github.com/hazelcast/hazelcast-go-client@v1.0.0-preview.2
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
client, err := hazelcast.StartNewClient()
// handle client start error
```

### Starting the Client with Given Options

Note that `Config` structs are not thread-safe. Complete creation of the configuration in a single go routine. 

```go
// create the default configuration
config := hazelcast.NewConfig()

// optionally set member addresses manually
config.ClusterConfig.SetAddress("member1.example.com", "member2.example.com")

// create and start the client with the configuration provider
client, err := hazelcast.StartNewClientWithConfig(config)
// handle client start error
```

## Documentation

Use godoc:
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

