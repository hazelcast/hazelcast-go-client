package internal

type ListProxy struct {
	*partitionSpecificProxy
}

func newListProxy(client *HazelcastClient, serviceName *string, name *string) (*ListProxy, error) {
	parSpecProxy, err := newPartitionSpecificProxy(client, serviceName, name)
	if err != nil {
		return nil, err
	}
	return &ListProxy{parSpecProxy}, nil
}
