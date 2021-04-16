# Hazelcast Go Client

Hazelcast is an open-source distributed in-memory data store and computation platform that provides a wide variety of distributed data structures and concurrency primitives.

Hazelcast Go client is a way to communicate to Hazelcast IMDG clusters and access the cluster data.

## Sample Code

```go
package main

import (
	"fmt"
	"log"
	"github.com/hazelcast/hazelcast-go-client/hazelcast"
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
    if err = people.Set(personName, 30); err != nil {
    	log.Fatal(err)
    }
    // get a value from the map
    age, err := people.Get(person)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("%s is %d years old.\n", personName, age)
}
```

## Features

* Distributed, partitioned and queryable in-memory key-value store implementation, called Map.
* Support for serverless and traditional web service architectures with Unisocket and Smart operation modes.
* Ability to listen to client lifecycle, cluster state, and distributed data structure events.

## Install

Requirements:

* Hazelcast Go client is compatible only with Hazelcast IMDG 4.x and above.

In your Go module enabled project, add a dependency to `github.com/hazelcast/hazelcast-go-client/v4`:
```
go get github.com/hazelcast/hazelcast-go-client/v4
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

Note that `ClientConfigBuilder` is not thread-safe.
Complete creating the configuration in a single go routine, do not pass configuration builder to other go routines without synchronization. 

```go
// create the config builder
cb := hz.NewClientConfigBuilder()

// optionally turn off smart routing
cb.Network().SetSmartRouting(false).
    // optionally set cluster addresses manually
    SetAddrs("member1.example.com", "member2.example.com")

// create and start the client with the configuration provider
client, err := hazelcast.StartNewClientWithConfig(cb)
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

## License

[Apache 2 License](https://github.com/hazelcast/hazelcast-go-client/blob/master/LICENSE).

Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com) for more information.

