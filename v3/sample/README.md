# Hazelcast Go Client Samples

In order to run any example, you should have a running Hazelcast instance. If you don't already have one:
- [download the latest hazelcast release](https://hazelcast.org/download/), 
- [start an instance](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#using-the-scripts-in-the-package).

When you have a running Hazelcast instance, you will see its address and port in the console.  You should add that information to the **network config**, as shown below,

```
config := hazelcast.NewConfig()
config.NetworkConfig().AddAddress("192.168.1.9:5701") // change this with your address

```

**The address given above is an example. Don't forget to change it to your server's address.**
## Samples

- [flakeidgen](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/flakeidgen):
    Usage of distributed [Flake ID Generator](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/flake_id_generator.go).
- [helloworld](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/helloworld):
    "<i>Hello World</i>", the classic entry point to learning.  
- [hzclouddiscovery](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/hzclouddiscovery): 
    Usage of Hazelcast Cloud Discovery.
- [list](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/list):
    Usage of distributed [list](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/list.go).
- [map/aggregator](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/map/aggregator):
    Usage of [aggregators](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/aggregator/aggregators.go).
- [map/entryprocessor](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/map/entryprocessor):
    Usage of [entryprocessor](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/map.go#L405).
- [map/listener](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/map/listener) :
    Usage of [map listeners](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/map.go#L352). 
- [map/predicate](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/map/predicate):
    Usage of [predicates](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/predicate/predicates.go).
- [map/projection](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/map/projection):
    Usage of [projection](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/projection/projection.go).
- [map/simple](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/map/simple):
    Usage of distributed [map](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/map.go).           
- [multimap](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/multimap):
    Usage of distributed [multimap](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/multi_map.go).
- [orgwebsite](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/orgwebsite): 
    Samples of [https://hazelcast.org](https://hazelcast.org).
- [pncounter](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/pncounter): 
    Usage of distributed [PN counter](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/pn_counter.go).
- [queue](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/queue): 
    Usage of distributed [queue](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/queue.go).
- [reliable topic](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/reliabletopic):
    Usage of distributed reliable topic.
- [replicated map](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/replicatedmap):
    Usage of distributed [replicated map](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/replicated_map.go).
- [ringbuffer](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/ringbuffer):
    Usage of distributed [ringbuffer](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/ringbuffer.go).
- [serialization/custom](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/serialization/custom) : 
    Usage of [custom serializer](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/serialization/api.go#L81).
- [serialization/global](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/serialization/global):
    Usage of [global serializer](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/serialization/api.go#L81). 
- [serialization/identified](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/serialization/identified):
    Usage of [identified serializer](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/serialization/api.go#L27).
- [serialization/portable](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/serialization/portable):
    Usage of [portable serializer](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/serialization/api.go#L50).
- [set](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/set):
    Usage of distributed [set](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/set.go).
- [topic](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/topic): 
    Usage of distributed [topic](https://github.com/hazelcast/hazelcast-go-client/v3/blob/master/core/topic.go).