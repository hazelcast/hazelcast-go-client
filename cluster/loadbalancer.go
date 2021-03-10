package cluster

// LoadBalancer allows you to send operations to one of a number of endpoints(Members).
// It is up to the implementation to use different load balancing policies.
//
// If client is configured with smart routing,
// only the operations that are not key based will be routed to the endpoint returned by the LoadBalancer.
// If the client is not smart routing, LoadBalancer will not be used.
type LoadBalancer interface {
	// Init initializes LoadBalancer with the given cluster.
	// The given cluster is used to select members.
	InitLoadBalancer(cluster Cluster)

	// Next returns the next member to route to.
	// It returns nil if no member is available.
	//Next() Member
}
