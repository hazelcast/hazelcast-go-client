/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Package hazelcast provides the Hazelcast Go client.

Hazelcast is an open-source distributed in-memory data store and computation platform. It provides a wide variety of distributed data structures and concurrency primitives.

Hazelcast Go client is a way to communicate to Hazelcast IMDG clusters and access the cluster data.

# Configuration

If you are using Hazelcast and Go Client on the same computer, generally the default configuration should be fine.
This is great for trying out the client.
However, if you run the client on a different computer than any of the cluster members, you may need to do some simple configurations such as specifying the member addresses.

The Hazelcast members and clients have their own configuration options.
You may need to reflect some of the member side configurations on the client side to properly connect to the cluster.

In order to configure the client, you only need to create a new `hazelcast.Config{}`, which you can pass to `hazelcast.StartNewClientWithConnfig` function:

	config := hazelcast.Config{}
	client, err := hazelcast.StartNewClientWithConfig(context.TODO(), config)

Calling hazelcast.StartNewClientWithConfig with the default configuration is equivalent to hazelcast.StartNewClient.
The default configuration assumes Hazelcast is running at localhost:5701 with the cluster name set to dev.
If you run Hazelcast members in a different server than the client, you need to make certain changes to client settings.

Assuming Hazelcast members are running at hz1.server.com:5701, hz2.server.com:5701 and hz3.server.com:5701 with cluster name production, you would use the configuration below.
Note that addresses must include port numbers:

	config := hazelcast.Config{}
	config.Cluster.Name = "production"
	config.Cluster.Network.SetAddresses("hz1.server.com:5701", "hz2.server.com:5701", "hz3.server.com:5701")

You can also load configuration from JSON:

	text := `
		{
			"Cluster": {
				"Name": "production",
				"Network": {
					"Addresses": [
						"hz1.server.com:5701",
						"hz2.server.com:5701",
						"hz3.server.com:5701"
					]
				}
			}
		}`
	var config hazelcast.Config
	if err := json.Unmarshal([]byte(text), &config); err != nil {
		panic(err)
	}

If you are changing several options in a configuration section, you may have to repeatedly specify the configuration section:

	config := hazelcast.Config{}
	config.Cluster.Name = "dev"
	config.Cluster.HeartbeatInterval = types.Duration(60 * time.Second)
	config.Cluster.Unisocket = true
	config.Cluster.SetLoadBalancer(cluster.NewRandomLoadBalancer())

You can simplify the code above by getting a reference to config.Cluster and update it:

	config := hazelcast.Config{}
	cc := &config.Cluster  // Note that we are getting a reference to config.Cluster!
	cc.Name = "dev"
	cc.HeartbeatInterval = types.Duration(60 * time.Second)
	cc.Unisocket = true
	cc.SetLoadBalancer(cluster.NewRandomLoadBalancer())

Note that you should get a reference to the configuration section you are updating, otherwise you would update a copy of it, which doesn't modify the configuration.

There are a few options that require a duration, such as config.Cluster.HeartbeatInterval, config.Cluster.Network.ConnectionTimeout and others.
You must use types.Duration instead of time.Duration with those options, since types.Duration values support human readable durations when deserialized from text:

	import "github.com/hazelcast/hazelcast-go-client/types"
	// ...
	config := hazelcast.Config{}
	config.Cluster.InvocationTimeout = types.Duration(3 * time.Minute)
	config.Cluster.Network.ConnectionTimeout = types.Duration(10 * time.Second)

That corresponds to the following JSON configuration. Refer to https://golang.org/pkg/time/#ParseDuration for the available duration strings:

	{
		"Cluster": {
			"InvocationTimeout": "3m",
			"Network": {
				"ConnectionTimeout": "10s"
			}
	}

Here are all configuration items with their default values:

	config := hazelcast.Config{}
	config.ClientName = ""
	config.SetLabels()

	cc := &config.Cluster
	cc.Name = "dev"
	cc.HeartbeatTimeout = types.Duration(5 * time.Second)
	cc.HeartbeatInterval = types.Duration(60 * time.Second)
	cc.InvocationTimeout = types.Duration(120 * time.Second)
	cc.RedoOperation = false
	cc.Unisocket = false
	cc.SetLoadBalancer(cluster.NewRoundRobinLoadBalancer())

	cc.Network.SetAddresses("127.0.0.1:5701")
	cc.Network.SSL.Enabled = true
	cc.Network.SSL.SetTLSConfig(&tls.Config{})
	cc.Network.ConnectionTimeout = types.Duration(5 * time.Second)

	cc.Security.Credentials.Username = ""
	cc.Security.Credentials.Password = ""

	cc.Discovery.UsePublicIP = false

	cc.Cloud.Enabled = false
	cc.Cloud.Token = ""

	cc.ConnectionStrategy.ReconnectMode = cluster.ReconnectModeOn
	cc.ConnectionStrategy.Timeout = types.Duration(1<<63 - 1)
	cc.ConnectionStrategy.Retry.InitialBackoff = types.Duration(1*time.Second)
	cc.ConnectionStrategy.Retry.MaxBackoff = types.Duration(30*time.Second)
	cc.ConnectionStrategy.Retry.Multiplier = 1.05
	cc.ConnectionStrategy.Retry.Jitter = 0.0

	config.Serialization.PortableVersion = 0
	config.Serialization.LittleEndian = false
	config.Serialization.SetPortableFactories()
	config.Serialization.SetIdentifiedDataSerializableFactories()
	config.Serialization.SetCustomSerializer()
	config.Serialization.SetClassDefinitions()
	config.Serialization.SetGlobalSerializer() // Gob serializer

	config.Stats.Enabled = false
	config.Stats.Period = types.Duration(5 * time.Second)

	config.Logger.Level = logger.InfoLevel

Checkout the nearcache package for the documentation about the Near Cache.

# Listening for Distributed Object Events

You can listen to creation and destroy events for distributed objects by attaching a listener to the client.
A distributed object is created when first referenced unless it already exists.
Here is an example:

	// Error handling is omitted for brevity.
	handler := func(e hazelcast.DistributedObjectNotified) {
		isMapEvent := e.ServiceName == hazelcast.ServiceNameMap
		isCreationEvent := e.EventType == hazelcast.DistributedObjectCreated
		log.Println(e.EventType, e.ServiceName, e.ObjectName, "creation?", isCreationEvent, "isMap?", isMapEvent)
	}
	subscriptionID, _ := client.AddDistributedObjectListener(ctx, handler)
	myMap, _ := client.GetMap(ctx, "my-map")
	// handler is called with: ServiceName=ServiceNameMap; ObjectName="my-map"; EventType=DistributedObjectCreated
	myMap.Destroy(ctx)
	// handler is called with: ServiceName=ServiceNameMap; ObjectName="my-map"; EventType=DistributedObjectDestroyed

If you don't want to receive any distributed object events, use client.RemoveDistributedObjectListener:

	client.RemoveDistributedObjectListener(subscriptionID)

# Running SQL Queries

Running SQL queries require Hazelcast 5.0 and up.
Check out the Hazelcast SQL documentation here: https://docs.hazelcast.com/hazelcast/latest/sql/sql-overview

The SQL support should be enabled in Hazelcast server configuration:

	<hazelcast>
		<jet enabled="true" />
	</hazelcast>

The client supports two kinds of queries: The ones returning rows (select statements and a few others) and the rest (insert, update, etc.).
The former kinds of queries are executed with QuerySQL method and the latter ones are executed with ExecSQL method.

Use the question mark (?) for placeholders.

To connect to a data source and query it as if it is a table, a mapping should be created.
Currently, mappings for Map, Kafka and file data sources are supported.

You can read the details about mappings here: https://docs.hazelcast.com/hazelcast/latest/sql/sql-overview#mappings

The following data types are supported when inserting/updating.
The names in parantheses correspond to SQL types:

  - string (varchar)
  - int8 (tinyint)
  - int16 (smallint)
  - int32 (integer)
  - int64 (bigint)
  - bool (boolean)
  - float32 (real)
  - float64 (double)
  - types.Decimal (decimal)
  - time.Time not supported, use one of types.LocalDate, types.LocalTime, types.LocalDateTime or types.OffsetDateTime
  - types.LocalDate (date)
  - types.LocalTime (time)
  - types.LocalDateTime (timestamp)
  - types.OffsetDateTime (timestamp with time zone)
  - serialization.JSON (json)

Using Date/Time

In order to force using a specific date/time type, create a time.Time value and cast it to the target type:

	t := time.Now()
	dateValue := types.LocalDate(t)
	timeValue := types.LocalTime(t)
	dateTimeValue := types.LocalDateTime(t)
	dateTimeWithTimezoneValue := types.OffsetDateTime(t)

# TLS/SSL

One of the offers of Hazelcast is the TLS/SSL protocol which you can use to establish an encrypted communication across your cluster with key stores and trust stores.

  - A Java keyStore is a file that includes a private key and a public certificate.
    The equivalent of a key store is the combination of keyfile and certfile at the Go client side.
  - A Java trustStore is a file that includes a list of certificates trusted by your application which is named certificate authority.
    The equivalent of a trust store is a cafile at the Go client side.

You should set keyStore and trustStore before starting the members.

Hazelcast allows you to encrypt socket level communication between Hazelcast members and between Hazelcast clients and members, for end to end encryption.
To use it, see the TLS/SSL for Hazelcast Members section: https://docs.hazelcast.com/hazelcast/latest/security/tls-ssl.html#tlsssl-for-hazelcast-members

TLS/SSL for the Hazelcast Go client can be configured using the cluster.SSLConfig{} struct.
This struct is accessible from the hazelcast.Config struct through Cluster.Network.SSL path.
You can either edit the hazelcast.Config object or create a cluster.SSLConfig{} instance and then assign it to config.Cluster.Network.SSL.

TLS/SSL for the Hazelcast Go client can be enabled/disabled using the SSLConfig.Enabled option.
When this option is set to True, TLS/SSL will be configured with respect to the other SSL options.
Setting this option to False will result in discarding the other SSL options.

	// error handling is omitted for brevity.
	config := hazelcast.Config{}
	config.Cluster.Network.SSL.Enabled = true
	client, _ := hazelcast.StartNewClientWithConfig(ctx, config)

Certificates of the Hazelcast members can be validated against CA file.
SSLConfig.SetCAPath()'s argument should point to the absolute path of the concatenated CA certificates in PEM format.
When SSL is enabled and CA file path is not set, a set of default CA certificates from default locations will be used.

	// error handling is omitted for brevity.
	config := hazelcast.NewConfig()
	_ := config.Cluster.Network.SSL.SetCAPath("/path/of/server.pem")
	client, _ := hazelcast.StartNewClientWithConfig(ctx, config)

When mutual authentication is enabled on the member side, clients or other members should also provide a certificate file that identifies themselves.
Then, Hazelcast members can use these certificates to validate the identity of their peers.
To enable mutual authentication, firstly, you need to set the following property on the server side in the hazelcast.xml file:

	<network>
	  <ssl enabled="true">
	    <properties>
	      <property name="javax.net.ssl.mutualAuthentication">REQUIRED</property>
	    </properties>
	  </ssl>
	</network>

Client certificate, private key and private key password can be set using the SSLConfig.AddClientCertAndEncryptedKeyPath().
The arguments should point to the absolute paths of the client certificate and private key in PEM format.
If the private key is encrypted using a password, third argument will be used to decrypt it.

	// error handling is omitted for brevity.
	config := hazelcast.NewConfig()
	_ := config.Cluster.Network.SSL.AddClientCertAndEncryptedKeyPath("/path/of/cert.pem", "path/of/key.pem", "password")
	client, _ := hazelcast.StartNewClientWithConfig(ctx, config)

SSLConfig has tls.Config embedded in it so that users can set any field of tls config as they wish.
You can set the tls.Config using the SSLConfig.SetTLSConfig() method.
Check out this page for further details about tls.Config options: https://pkg.go.dev/crypto/tls#Config

	// error handling is omitted for brevity.
	config := hazelcast.NewConfig()
	config.Cluster.Network.SSL.SetTLSConfig(&tls.Config{ServerName: "foo.bar", MinVersion: tls.VersionTLS13})
	client, _ := hazelcast.StartNewClientWithConfig(ctx, config)

Hazelcast Go client offers the following protocols:

  - TLSv1 : TLS 1.0 Protocol described in RFC 2246
  - TLSv1_1 : TLS 1.1 Protocol described in RFC 4346
  - TLSv1_2 : TLS 1.2 Protocol described in RFC 5246
  - TLSv1_3 : TLS 1.3 Protocol described in RFC 8446

# Management Center Integration

Hazelcast Management Center can monitor your clients if client-side statistics are enabled.

You can enable statistics by setting config.Stats.Enabled to true.
Optionally, the period of statistics collection can be set using config.Stats.Period setting.
The labels set in configuration appear in the Management Center console:

	config := hazelcast.Config{}
	config.SetLabels("fast-cache", "staging")
	config.Stats.Enabled = true
	config.Stats.Period = 1 * time.Second
	client, err := hazelcast.StartNewClientWithConfig(config)
*/
package hazelcast
