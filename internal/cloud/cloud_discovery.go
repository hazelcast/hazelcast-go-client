package cloud

import (
	"context"
	"fmt"
	"strings"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/rest"
)

type DiscoveryClient struct {
	token      string
	httpClient *rest.HTTPClient
	logger     logger.Logger
}

func NewDiscoveryClient(config *cluster.HazelcastCloudConfig, logger logger.Logger) *DiscoveryClient {
	return &DiscoveryClient{
		token:      config.Token,
		httpClient: rest.NewHTTPClient(),
		logger:     logger,
	}
}

func (c *DiscoveryClient) DiscoverNodes(ctx context.Context) ([]Address, error) {
	url := makeCoordinatorURL(c.token)
	c.logger.Trace(func() string { return fmt.Sprintf("cloud discovery: %s", url) })
	if j, err := c.httpClient.GetJSONArray(ctx, url); err != nil {
		return nil, err
	} else {
		addrs := extractAddresses(j)
		c.logger.Trace(func() string { return fmt.Sprintf("cloud addresses: %v", addrs) })
		return extractAddresses(j), nil
	}
}

func extractAddresses(j interface{}) []Address {
	// sample JSON:
	// [{"private-address":"100.115.50.221","public-address":"35.177.212.248:31984"},{"private-address":"100.109.198.133","public-address":"3.8.123.82:31984"}]
	jv := rest.JsonArray(j)
	r := make([]Address, len(jv))
	for i, v := range jv {
		public := rest.JsonString(rest.JsonObjectGet(v, "public-address"))
		private := rest.JsonString(rest.JsonObjectGet(v, "private-address"))
		private = augmentPrivateAddr(private, public)
		r[i] = NewAddress(public, private)
	}
	return r
}

func makeCoordinatorURL(token string) string {
	return fmt.Sprintf("https://coordinator.hazelcast.cloud/cluster/discovery?token=%s", token)
}

func augmentPrivateAddr(private, public string) string {
	// public addresses don't seem to have the port
	// try use the one from public if missing
	if strings.Index(private, ":") > 0 {
		return private
	}
	// if private address doesn't have the port, use public port
	idx := strings.Index(public, ":")
	if idx < 0 {
		return private
	}
	return fmt.Sprintf("%s%s", private, public[idx:])
}
