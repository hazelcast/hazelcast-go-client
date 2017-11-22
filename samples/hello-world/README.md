# Hazelcast for Go Getters

Know "_Go_" but don't know about "_Hazelcast_" ?

Know "_Hazelcast_" but don't know about "_Go_" ?

This tutorial gets you going with both together, in a follow-along style, assuming you only know one of the two.

## Hello World

Naturally, this is a simple example, "Hello World", the bare minimum.

We'll start by installing the tools, then review the architecture, then finally do a bit of coding.

## Set-up

This section explains how to set things up on your machine, skip over any bits you're familiar with.

### "_Go_" set-up.

Four short steps here, nice and easy.

#### "_Go_" itself

Firstly, download and install "_Go_" from [https://golang.org/](https://golang.org/)

Once installed, open a *new* command window and try this command `go version`

Hopefully you will see something like this:

```
$ go version
go version go1.9.2 darwin/amd64
$ 
```

This indicates the version of "_Go_", here it is 1.9.2, and the hardware being used. '_Darwin_' means this machine is a Mac.

#### "_Go_" structure

The next thing is to create a directory or folder called `go`, all in lowercase, in your home directory. This is
the convention, best not to deviate from it unless you really know what you're doing.

Then, create an environment variable named `GOPATH`, all in uppercase, that points to this directory and store it
in your profile so it is available each time you login.


#### "_Hazelcast Go_" client

Finally, we want to download Hazelcast's client for Go. Hazelcast here will be a _client-server_ architecture, we'll
cover this later on. For now the server side is Java, so the client side we want to be Go.

The command here is "*go get*", hence the title of this tutorial. We ask Go to get the
code from Github.

From the `go` directory, run this command

```
go get github.com/hazelcast/hazelcast-go-client
```

or this with the `-v` flag for verbose output

```
go get -v github.com/hazelcast/hazelcast-go-client
```

This will pull down the Hazelcast client for Go from Github, from [here](https://github.com/hazelcast/hazelcast-go-client.git), and
set up the necessary Go directory structory.

#### Verification

Go will place the Hazelcast client for Go in a subdirectory following a specific structure.

You should see at least these folders

```
├── go/
├── go/pkg/
├── go/src/
├── go/src/github.com
├── go/src/github.com/hazelcast
├── go/src/github.com/hazelcast/hazelcast-go-client
```

The `pkg` folder holds output packages.

The `src` folder holds the source code, in a subdirectory for Github code, then a subdirectory for the 
Hazelcast Github user, and then a subdirectory for the Hazelcast client for Go repository.

If you see this, we are done with Go set-up.

### "_Hazelcast_" set-up.

Three simple steps.

#### "_Java_"

Hazelcast is a Java application, so you need a Java implementation. 

The best place to get this from is [OpenJDK](http://openjdk.java.net/), an open source implementation of Java.
Or commercially, from [Oracle](http://www.oracle.com/technetwork/java/javase/downloads/index.html).

Either way, take the latest version of Java 9 available for your platform, and this should come with
an installer.

The Java language has two variants alive currently, Java 8 and Java 9, but Java 8 will be retired first so
Java 9 is a better choice.

If offered a _JRE_ (Java Runtime Environment) or _JDK_ (Java Development Kit), either will do here. The
development kit adds compilers to the runtime, but we don't use them in this example.

#### "_Hazelcast_"

Next download Hazelcast.

You can get this from a number of places, in this case try [here](https://hazelcast.org/download/).

You want "_Hazelcast IMDG_". _IMDG_ here is short for In-Memory Data Grid. 

The most recent release is shown at the top. Select the _ZIP_ or _TAR_, download and unpack.
You can put it anywhere for now, so your downloads folder is as good a place as any.

You should see something like this, assuming a version of 3.9,

```
├── hazelcast-3.9/
├── hazelcast-3.9/bin
├── hazelcast-3.9/code-samples
├── hazelcast-3.9/demo
├── hazelcast-3.9/docs
├── hazelcast-3.9/lib
├── hazelcast-3.9/license
├── hazelcast-3.9/mancenter
```

#### Verification

Firstly, run the `java -version` command to see the Java version.

You will likely need to open a new window to ensure Java is on known to the command interpreter, but
should then get version output. The exact wording will likely differ.

```
$ java -version
java version "9.0.1"
Java(TM) SE Runtime Environment (build 9.0.1+11)
Java HotSpot(TM) 64-Bit Server VM (build 9.0.1+11, mixed mode)
$ 
```

If Java is found, then you can try Hazelcast, using the `start.sh` script in
Hazelcast's `bin` folder.

This will give a lot of output, as Hazelcast is quite verbose by default about what it is doing,
but towards the end you should see messages like

```
INFO: [10.37.217.195]:5701 [dev] [3.9] 

Members {size:1, ver:1} [
	Member [10.37.217.195]:5701 - 57a58f7f-4453-4a6f-8e89-bf1a464e46b1 this
]

INFO: [10.37.217.195]:5701 [dev] [3.9] [10.37.217.195]:5701 is STARTED

```

This message indicates you have a cluster (of one!) Hazelcast servers running on your machine. 
In this case, the Hazelcast server is on the machine with IP address 10.37.217.195 and port 5701.

## Architecture

## TODO 

## Coding

If both "_Go_" and "_Hazelcast_" are successfully installed, and you have a rough understanding of the architecture,
then it's time for Hello World.

### Client Server architecture

#### TODO 

### Outline

#### TODO 

### The Hazelcast half

#### TODO 

### The Go half

There are three programs, imaginatively named `one.go`, `two.go` and `three.go`.

All three are clients of the Hazelcast server, and for this example we run them in sequence,
though they could be run concurrently.


#### `one.go`

From the command line, in the directory with the `one.go` program (`$GOPATH/src/github.com/hazelcast/go-client/samples/hello-world/`)
run this command

```
go run one.go
```

This will request Go to run the `one.go` program.

This program does very little except try to connect to the Hazelcast server on the _host:port_
pairing of `127.0.0.1:5701`, to prove connectivity.

One of two things will happen, depending whether the Hazelcast server is running. Try this
both with and without the Hazelcast server running to see what happens.

##### Failure

If the Hazelcast server is not running, the Go client will fail to connect to it.

You should see an error message much like this

```
Cluster name:  dev
Cluster discovery:  [127.0.0.1:5701]
2017/11/21 14:43:45 New State :  STARTING
2017/11/21 14:43:46 the following error occured while trying to connect to cluster:  target is disconnected
2017/11/21 14:43:47 the following error occured while trying to connect to cluster:  target is disconnected
2017/11/21 14:43:47 New State :  SHUTTING_DOWN
2017/11/21 14:43:47 New State :  SHUTDOWN
```

The client will try to connect a couple of times, then give up and the routine ends.

##### Success

If the Hazelcast server is running, the Go client will connect to it.

So this time you should see this:

```
Cluster name:  dev
Cluster discovery:  [127.0.0.1:5701]
2017/11/21 14:53:39 New State :  STARTING
2017/11/21 14:53:39 Registered membership listener with Id  c06ec961-0159-481b-a6ee-fa00c145f3b9
2017/11/21 14:53:39 New State :  CONNECTED
2017/11/21 14:53:39 New State :  STARTED
2017/11/21 14:53:39 New State :  SHUTTING_DOWN
2017/11/21 14:53:39 New State :  SHUTDOWN
```

##### Discovery

The Go client is configured in `one.go` to try to connect to `127.0.0.1:5701`. 

Obviously this fails if there isn't a Hazelcast server on that _host:port_.

More usually, you run multiple Hazelcast servers to form a cluster. In this case
you can still configure the Go client with _host:port_ of one of the Hazelcast
servers. It's a better idea to list the _host:port_ pairings for two or three of
the Hazelcast servers than just one, in case that one is offline. But you don't
need to list them all, if you get an answer from any of the listed Hazelcast
servers, the answer includes the location of all the other Hazelcast
servers in the cluster.

#### `two.go`

##### TODO 

#### `three.go`

##### TODO 

## Bonus Step - Clustering

As per above, typical usage of Hazelcast is to run multiple servers that join
together to form a cluster, providing resilience and scalable storage.

The `start.sh` script in Hazelcast's `bin` folder creates one Hazelcast
instance, and logs it's process id in a `*.pid` file for later user.

### Clean start

Firstly, we want to ensure no Hazelcast servers are running.

The easiest way is just to kill off any Java processes from the O/s. On Windows
this might be with `taskkill`, on Mac with `kill -9`.

Also make sure the `hazelcast_instance.pid` is removed.

### Start a first _Hazelcast_ server

Use the `start.sh` script to start up a server.

You are looking for a message like this to confirm it is up:

```
Members {size:1, ver:1} [
	Member [192.168.1.156]:5701 - f4855568-dcd6-4cbc-b231-6f8af5c72487 this
]
```

This lists the members of the cluster, and if you're starting from clean there
should only be one member, the process just started.

Take a note of the process id of this server, for instance by looking in the 
`hazelcast_instance.pid` file.

*NOTE* The default port for _Hazelcast_ is 5701. 5701 was free, so 5701 was selected.

### Write some data using _Go_

Run

```
go run two.go
```

from the `hello-world` folder in the _Go_ source hierarchy.

Amongst the output you should see the same message as before,

```
Map 'greetings' Size before 0
Map 'greetings' Size after 5
```

confirming that the "_greetings_" map has changed from empty to having 5 data records.

### Start a second _Hazelcast_ server

In _Hazelcast_'s `bin` folder, remove the `hazelcast_instance.pid` file.

This file is what the `start.sh` script uses to determine if a process is running,
so by removing it we can trick the `start.sh` script (and lose the ability for the
`stop.sh` script to find it to shut it down cleanly)

So now if we run `start.sh`, it should start another Hazelcast server

You should now see messages like this

```
Members {size:2, ver:2} [
	Member [192.168.1.156]:5701 - f4855568-dcd6-4cbc-b231-6f8af5c72487
	Member [192.168.1.156]:5702 - 3f84bc0e-5ce2-457f-8db2-c52c82e34d82 this
]
```

Two Hazelcast servers are running, they have found each other and joined together
to form a cluster. Each will log out the presence of the others.

*NOTE* The default port for _Hazelcast_ is 5701. 5701 is in use by the first
server, so the second server takes the next available one, 5702.

### Kill the first _Hazelcast_ server

Using your O/s (`kill -9`, `taskkill`, etc) forceably terminate the first
Hazelcast server that you started.

The second _Hazelcast_ server will notice the first disappear, and moan
about it. 

It should then produce a message with the new member list of the cluster,
only containing itself.

```
Members {size:1, ver:3} [
	Member [192.168.1.156]:5702 - 3f84bc0e-5ce2-457f-8db2-c52c82e34d82 this
]
```

The proof you've killed the right one of the two, is that the one using
port 5702 is still running and reporting the other as AWOL.

### Read some data from _Go_

From the `hello-world` folder in your _Go_ source tree, run the third
program to inspect the data in Hazelcast

```
go run three.go
```

Problems!

The _Go_ client routine is looking for a _Hazelcast_ server on 127.0.0.1:5701
and there is no server there as we've just killed it.

The _Go_ routine `three.go` has been deliberately configured with only one place to look for
a connection and there's nothing there. That's not good practice.

So it'll produce a slew of error messages starting with

```
2017/11/22 20:58:13 the following error occured while trying to connect to cluster:  target is disconnected
2017/11/22 20:58:17 the following error occured while trying to connect to cluster:  target is disconnected
```

and shutdown.

### Start another _Hazelcast_ server

Back to _Hazelcast_'s `bin` folder, remove the `hazelcast_instance.pid` file again, and
run the `start.sh` again.

A _Hazelcast_ server will start up, find the other one that is still running, and both 
will produce a message showing who is now in the cluster.

```
Members {size:2, ver:4} [
	Member [192.168.1.156]:5702 - 3f84bc0e-5ce2-457f-8db2-c52c82e34d82
	Member [192.168.1.156]:5701 - b1536a80-78c6-401e-b870-8515acbbabe7 this
]
```

*NOTE* As mentioned a few times now, the default port for _Hazelcast_ is 5701.
This port is available, so this port is selected for this new server. It's
not the first server revived, it's an entirely new server that is utlising the
same port (the UUID _b1536a80-78c6-401e-b870-8515acbbabe7_ is different).

### Try again to read some data from _Go_

Now try

```
go run three.go
```

again.

Output should be

```
 -> 'Spanish'=='hola mundo'
 -> 'German'=='hallo welt'
 -> 'Italian'=='ciao mondo'
 -> 'English'=='hello world'
 -> 'French'=='bonjour monde'
[5 records]
```

the five data records is an unspecified order.

### Recap

So what we've seen in the bonus section is some of the power of Hazelcast
as a resilient and scalable store.

Although some of _Hazelcast_ servers may suffer mishaps and go offline,
so long as sufficient others remain you don't lose data.

(And you shouldn't lose access to the data either, if you've configured
correctly!)

## Summary


#### TODO 

