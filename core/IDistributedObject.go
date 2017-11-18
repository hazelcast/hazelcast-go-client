package core

// IDistributedObject is the base interface for all distributed objects.
type IDistributedObject interface {
	// Destroy destroys this object cluster-wide.
	// Destroy clears and releases all resources for this object.
	Destroy() (bool, error)

	// Name returns the unique name for this IDistributedObject. Returned value will never be nil.
	Name() string

	// PartitionKey returns the key of partition this IDistributedObject is assigned to. The returned value only has meaning
	// for a non partitioned data structure like an IAtomicLong. For a partitioned data structure like an IMap
	// the returned value will not be nil, but otherwise undefined.
	PartitionKey() string

	// ServiceName returns the service name for this object.
	ServiceName() string
}
