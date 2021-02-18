# Table of Contents

* [Soak Test Description](#soak-test-description)
* [Success Criteria](#success-criteria)

# SoakTest Test Description
The test is being performed based on "Test based on Integration Tests without Teardown" section at [Release Process](https://hazelcast.atlassian.net/wiki/spaces/EN/pages/4030856/Release+Process) page. See [SoakTest](https://github.com/hazelcast/hazelcast-go-client/v3/tree/master/sample/soak/map_soak.go) 

The test setup at Hazelcast lab environment is:

1. Use 4 member cluster with 1 JVM per VM 
2. The client test program exercises the following API calls from 32 subroutines in a random manner in a busy loop (See [map_soak.go](./map_soak.go)):
    + Put/Gets (number of keys: 1000, value size of 1 byte)
    + Predicates
    + MapListeners
    + EntryProcessors
    <p>The test code captures any error and logs to a file. 
    
3. Run 10 clients on one lab machine and this machine only runs clients (i.e. no server at this machine)

4. Run the tests for 48 hours. Verify that: 
    + Make sure that all the client processes are up and running before killing the clients after 48 hours.
    + Analyse the output file: Make sure that there are no errors printed.
    
# Success Criteria
1. No errors printed.
2. All client processes are up and running after 48 hours with no problem.