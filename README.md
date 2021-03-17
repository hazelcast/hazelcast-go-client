# Hazelcast Go Client V3

    WARNING!
    Using Hazelcast Go Client without Go modules is deprecated.
    3.7.0 is the last version of the client that supports Hazelcast IMDG v3.x.
    Next version of the client will support Hazelcast IMDG v4.x and up.
    The master branch of this repository is going to be available for v3 users until April 2021.

See the [original README here](../docs).

## Usage

In your Go module enabled project, add a dependency to `github.com/hazelcast/hazelcast-go-client/v3@v3.7.0`:
```
go get github.com/hazelcast/hazelcast-go-client/v3@v3.7.0
```

And replace import paths starting with `https://github.com/hazelcast/hazelcast-go-client` with `https://github.com/hazelcast/hazelcast-go-client/v3`.

For example:
```
import "github.com/hazelcast/hazelcast-go-client/v3/hazelcast"
```

## Documentation

Moved [here](../docs).

## Support

Join us at [Go Client channel](https://hazelcastcommunity.slack.com/channels/go-client) or [Hazelcast at Google Groups](https://groups.google.com/g/hazelcast).

## License

[Apache 2 License](https://github.com/hazelcast/hazelcast-go-client/blob/master/LICENSE).

Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com) for more information.

