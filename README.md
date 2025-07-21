# Hazelcast Go Client

[![Go Reference](https://pkg.go.dev/badge/github.com/hazelcast/hazelcast-go-client.svg)](https://pkg.go.dev/github.com/hazelcast/hazelcast-go-client)

Hazelcast is an open-source distributed in-memory data store and computation platform that provides a wide variety of distributed data structures and concurrency primitives.

Hazelcast Go client is a way to communicate with Hazelcast 4 and 5 clusters and access the cluster data.

## Documentation

For a list of the features available, and for information about how to install and get started with the client, see the [Go client documentation](https://docs.hazelcast.com/hazelcast/6.0-snapshot/clients/go).

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

