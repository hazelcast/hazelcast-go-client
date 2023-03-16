/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
Package cluster contains functions and types needed to connect to a Hazelcast cluster.

# Port Range

Port Range configuration allows you to specify the port range for cluster addresses where you did not specify any port.
If no port range was specified and also no port was set on an address, then the default port range will be applied (5701-5703).
If you use a port range in any way, then the client will try all the ports for a given address until it is able to connect to the right member.

# Load Balancer

Load balancer configuration allows you to specify which cluster address to send next operation.

If smart client mode is used, only the operations that are not key-based are routed to the member that is returned by the load balancer.
Load balancer is ignored for unisocket mode.

The default load balancer is the RoundRobinLoadBalancer, which picks the next address in order among the provided addresses.
The other built-in load balancer is RandomLoadBalancer.
You can also write a custom load balancer by implementing LoadBalancer.

Use config.Cluster.SetLoadBalancer to set the load balancer:

	config := hazelcast.Config{}
	config.Cluster.SetLoadBalancer(cluster.NewRandomLoadBalancer())

# Hazelcast Cloud Discovery

Hazelcast Go client can discover and connect to Hazelcast clusters running on Hazelcast Cloud https://cloud.hazelcast.com.
In order to activate it, set the cluster name, enable Hazelcast Cloud discovery and add Hazelcast Cloud Token to the configuration.
Here is an example:

	config := hazelcast.Config{}
	config.Cluster.Name = "MY-CLUSTER-NAME"
	cc := &config.Cluster.Cloud
	cc.Enabled = true
	cc.Token = "MY-CLUSTER-TOKEN"
	client, err := hazelcast.StartNewClientWithConfig(config)
	if err != nil {
		log.Fatal(err)
	}

Also check the code sample in https://github.com/hazelcast/hazelcast-go-client/tree/master/examples/discovery/cloud.

If you have enabled encryption for your cluster, you should also enable TLS/SSL configuration for the client.

# External Client Public Address Discovery

When you set up a Hazelcast cluster in the Cloud (AWS, Azure, GCP, Kubernetes) and would like to use it from outside the Cloud network,
the client needs to communicate with all cluster members via their public IP addresses.
Whenever Hazelcast cluster members are able to resolve their own public external IP addresses, they pass this information to the client.
As a result, the client can use public addresses for communication, if it cannot access members via private IPs.

Hazelcast Go client has a built-in mechanism to use public IP addresses instead of private ones.
You can enable this feature by setting config.Discovery.UsePublicIP to true and specifying the adddress of at least one member:

	config := hazelcast.Config{}
	config.Cluster.Network.SetAddresses("30.40.50.60:5701")
	config.Cluster.Discovery.UsePublicIP = true

For more details on member-side configuration, refer to the Discovery SPI section in the Hazelcast IMDG Reference Manual.

# Client Connection Strategy

You can configure how the client reconnects to the cluster after a disconnection by setting config.Cluster.ConnectionStrategy.ReconnectMode.
cluster.ReconnectModeOn is the default and causes the client to try to reconnect until cluster connection timeout.
cluster.ReconnectModeOff disables reconnection.
You can control the cluster connection timeout using config.Cluster.ConnectionStrategy.Timeout setting:

	config := hazelcast.Config{}
	config.Cluster.ConnectionStrategy.ReconnectMode = cluster.ReconnectModeOn
	config.Cluster.ConnectionStrategy.Timeout = types.Duration(5 * time.Minute)

The client tries to reconnect when the client is disconnected from the cluster.
The waiting duration before the next reconnection attempt is found using the following formula:

	backoff = minimum(MaxBackoff, InitialBackoff)
	duration = backoff + backoff*Jitter*2.0*(RandomFloat64()-1.0)
	next(backoff) = minimum(MaxBackoff, backoff*Multiplier)

You can configure the frequency of the reconnection attempts using config.Cluster.ConnectionStrategy.Retry setting:

	config := hazelcast.Config{}
	r := &config.Cluster.ConnectionStrategy.Retry
	r.MaxBackoff = types.Duration(30*time.Second)
	r.InitialBackoff = types.Duration(1*time.Second)
	r.Jitter = 0.0
	r.Multiplier = 1.05

# TLS/SSL

Hazelcast supports encrypted communication between the client and the server using TLS/SSL protocol with key stores and trust stores.
This feature requires Hazelcast Enterprise.

In order to use TLS encryption, the Hazelcast members should be configured.
See https://docs.hazelcast.com/hazelcast/latest/security/tls-ssl.html#tlsssl-for-hazelcast-members for more information.

In order to enable TLS on the client side, set config.Cluster.Network.SSl.Enabled to true.

	// error handling is omitted for brevity.
	var config hazelcast.Config
	config.Cluster.Network.SSL.Enabled = true

If you need to set a custom tls.Config, for example to turn off verification, you need to assign the tls.Config before any other SSL configuration:
Check out this page for further details about tls.Config options: https://pkg.go.dev/crypto/tls#Config

	// warning: never set InsecureSkipVerify: true on production
	// config.Cluster.Network.SSL.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})

You need to set the host name with:

	config.Cluster.Network.SSL.ServerName = "foo.com"

Certificates of the Hazelcast members can be validated against CA file.
SSLConfig.SetCAPath()'s argument should point to the absolute path of the concatenated CA certificates in PEM format.
When SSL is enabled and CA file path is not set, a set of default CA certificates from default locations will be used.

	// error handling is omitted for brevity.
	var config hazelcast.Config
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

Client certificate, private key and private key password can be set using the SSLConfig.AddClientCertAndEncryptedKeyPath() method.
The arguments should point to the absolute paths of the client certificate and private key in PEM format.
If the private key is encrypted using a password, third argument will be used to decrypt it.

	// error handling is omitted for brevity.
	var config hazelcast.Config
	_ := config.Cluster.Network.SSL.AddClientCertAndEncryptedKeyPath("/path/of/cert.pem", "path/of/key.pem", "password")
	client, _ := hazelcast.StartNewClientWithConfig(ctx, config)
*/
package cluster
