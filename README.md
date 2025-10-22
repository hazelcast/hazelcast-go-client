# Hazelcast Go Client

[![Go Reference](https://pkg.go.dev/badge/github.com/hazelcast/hazelcast-go-client.svg)](https://pkg.go.dev/github.com/hazelcast/hazelcast-go-client)

Hazelcast is an open-source distributed in-memory data store and computation platform that provides a wide variety of distributed data structures and concurrency primitives.

The Hazelcast Go client allows you to communicate with Hazelcast 4 and 5 clusters and access the cluster data.

## Documentation

For a list of the features available, and for information about how to install and get started with the client, see the [Go client documentation](https://docs.hazelcast.com/hazelcast/latest/clients/go).

## Test the client

This section describes how to test the Hazelcast Go client. 
Currently, we support only Linux, MacOS and WSL (Windows Subsystem for Linux) for testing the client.

To run integration tests, you must have the following installed:
* Java 8
* Maven 3 or later
* Bash
* Make

Before running the tests, start the Hazelcast remote controller to enable the test suite to create clusters:
```shell
# Start RC with Hazelcast Community features
$ ./rc.sh start

# Or, start RC with Hazelcast Enterprise features
$ HAZELCAST_ENTERPRISE_KEY=ENTERPRISE-KEY-HERE ./rc.sh start 
```

Run the tests using one of the following methods:
* Run `make test-all` to run integration tests.
* Run `make test-all-race` to run integration tests with race detection.
* Run `make test-cover` to generate the coverage report and `make view-cover` to view the test coverage summary and generate an HTML report.

Testing the client with SSL support requires running the remote controller with Hazelcast Enterprise features.
To enable SSL connections, add `ENABLE_SSL=1` to environment variables, or prepend it to the make commands above.

To turn on verbose logging, add `ENABLE_TRACE=1` to environment variables, or prepend it to the make commands above.

## License

[Apache 2 License](https://github.com/hazelcast/hazelcast-go-client/blob/master/LICENSE).

Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com) for more information.

