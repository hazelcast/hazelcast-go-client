/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package nearcache_test

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

func TestNearCacheConfigWithoutWildcard(t *testing.T) {
	// ported from: com.hazelcast.client.config.MatchingPointConfigPatternMatcherTest#testNearCacheConfigWithoutWildcard
	config, ncs := configWithNearCacheNames("someNearCache")
	assert.Equal(t, ncs[0], assertTrueGetNearCacheConfig(t, config, "someNearCache"))
	assertFalseGetNearCacheConfig(t, config, "doesNotExist")
	assertFalseGetNearCacheConfig(t, config, "SomeNearCache")
}

func TestNearCacheConfigWildcard1(t *testing.T) {
	// ported from: com.hazelcast.client.config.MatchingPointConfigPatternMatcherTest#testNearCacheConfigWildcard1
	config, ncs := configWithNearCacheNames("*hazelcast.test.myNearCache")
	assert.Equal(t, ncs[0], assertTrueGetNearCacheConfig(t, config, "com.hazelcast.test.myNearCache"))
}

func TestNearCacheConfigWildcard2(t *testing.T) {
	// ported from: com.hazelcast.client.config.MatchingPointConfigPatternMatcherTest#testNearCacheConfigWildcard2
	config, ncs := configWithNearCacheNames("com.hazelcast.*.myNearCache")
	assert.Equal(t, ncs[0], assertTrueGetNearCacheConfig(t, config, "com.hazelcast.test.myNearCache"))
}

func TestNearCacheConfigWildcard3(t *testing.T) {
	// ported from: com.hazelcast.client.config.MatchingPointConfigPatternMatcherTest#testNearCacheConfigWildcard3
	config, ncs := configWithNearCacheNames("com.hazelcast.test.*")
	assert.Equal(t, ncs[0], assertTrueGetNearCacheConfig(t, config, "com.hazelcast.test.myNearCache"))
}

func TestNearCacheConfigWildcardMultipleConfigs(t *testing.T) {
	// ported from: com.hazelcast.client.config.MatchingPointConfigPatternMatcherTest#testNearCacheConfigWildcardMultipleConfigs
	config, ncs := configWithNearCacheNames(
		"com.hazelcast.*",
		"com.hazelcast.test.*",
		"com.hazelcast.test.sub.*",
	)
	assert.Equal(t, ncs[0], assertTrueGetNearCacheConfig(t, config, "com.hazelcast.myNearCache"))
	assert.Equal(t, ncs[1], assertTrueGetNearCacheConfig(t, config, "com.hazelcast.test.myNearCache"))
	assert.Equal(t, ncs[2], assertTrueGetNearCacheConfig(t, config, "com.hazelcast.test.sub.myNearCache"))
}

func TestMapConfigWildcardMultipleAmbiguousConfigs(t *testing.T) {
	// ported from: com.hazelcast.client.config.MatchingPointConfigPatternMatcherTest#testMapConfigWildcardMultipleAmbiguousConfigs
	config, _ := configWithNearCacheNames("com.hazelcast*", "*com.hazelcast")
	_, _, err := config.GetNearCacheConfig("com.hazelcast")
	if !errors.Is(err, hzerrors.ErrInvalidConfiguration) {
		t.Fatalf("expected invalid configuration error, but got: %v", err)
	}
}

func TestNearCacheConfigWildcardMatchingPointStartsWith(t *testing.T) {
	// ported from: com.hazelcast.client.config.MatchingPointConfigPatternMatcherTest#testNearCacheConfigWildcardMatchingPointStartsWith
	config, _ := configWithNearCacheNames(
		"hazelcast.*",
		"hazelcast.test.*",
		"hazelcast.test.sub.*",
	)
	assertFalseGetNearCacheConfig(t, config, "com.hazelcast.myNearCache")
	assertFalseGetNearCacheConfig(t, config, "com.hazelcast.test.myNearCache")
	assertFalseGetNearCacheConfig(t, config, "com.hazelcast.test.sub.myNearCache")
}

func TestNearCacheConfigWildcardMatchingPointEndsWith(t *testing.T) {
	// ported from: com.hazelcast.client.config.MatchingPointConfigPatternMatcherTest#testNearCacheConfigWildcardMatchingPointEndsWith
	config, _ := configWithNearCacheNames(
		"*.sub",
		"*.test.sub",
		"*.hazelcast.test.sub",
	)
	assertFalseGetNearCacheConfig(t, config, "com.hazelFast.Fast.sub.myNearCache")
	assertFalseGetNearCacheConfig(t, config, "hazelFast.test.sub.myNearCache")
	assertFalseGetNearCacheConfig(t, config, "test.sub.myNearCache")
}

func TestNearCacheConfigWildcardOnly(t *testing.T) {
	// ported from: com.hazelcast.client.config.MatchingPointConfigPatternMatcherTest#testNearCacheConfigWildcardOnly
	config, ncs := configWithNearCacheNames("*")
	assert.Equal(t, ncs[0], assertTrueGetNearCacheConfig(t, config, "com.hazelcast.myNearCache"))
}

func TestNearCacheConfigWildcardOnlyMultipleConfigs(t *testing.T) {
	// com.hazelcast.client.config.MatchingPointConfigPatternMatcherTest#testNearCacheConfigWildcardOnlyMultipleConfigs
	config, ncs := configWithNearCacheNames("*", "com.hazelcast.*")
	assert.Equal(t, ncs[1], assertTrueGetNearCacheConfig(t, config, "com.hazelcast.myNearCache"))
}

func TestDefaultConfig(t *testing.T) {
	ncc := nearcache.Config{}
	if err := ncc.Validate(); err != nil {
		t.Fatal(err)
	}
	target := nearcache.Config{
		Name:           "default",
		EvictionConfig: nearcache.EvictionConfig{},
		PreloaderConfig: nearcache.PreloaderConfig{
			Enabled:                  false,
			Directory:                "",
			StoreIntervalSeconds:     600,
			StoreInitialDelaySeconds: 600,
		},
		InMemoryFormat:    nearcache.InMemoryFormatBinary,
		SerializeKeys:     false,
		TimeToLiveSeconds: math.MaxInt32,
		MaxIdleSeconds:    math.MaxInt32,
	}
	assert.Equal(t, target, ncc)
}

type testCase struct {
	name string
	cfg  nearcache.Config
}

func (tc testCase) Run(t *testing.T) {
	t.Run(tc.name, func(t *testing.T) {
		err := tc.cfg.Validate()
		if !errors.Is(err, hzerrors.ErrInvalidConfiguration) {
			t.Fatalf("%s: expected ErrInvalidConfiguration", tc.name)
		}
	})
}

func TestConfigInvalid(t *testing.T) {
	testCases := []testCase{
		{
			name: "negative time to live",
			cfg:  nearcache.Config{TimeToLiveSeconds: -1},
		},
		{
			name: "negative max idle",
			cfg:  nearcache.Config{MaxIdleSeconds: -1},
		},
	}
	for _, tc := range testCases {
		tc.Run(t)
	}
}

type comparator struct{}

func (c comparator) Compare(a, b nearcache.EvictableEntryView) int {
	return 0
}

func TestEvictionConfigInvalid(t *testing.T) {
	// has both policy and comparator
	ec1 := nearcache.EvictionConfig{}
	ec1.SetEvictionPolicy(nearcache.EvictionPolicyNone)
	ec1.SetComparator(&comparator{})
	// invalid policy
	ec2 := nearcache.EvictionConfig{}
	ec2.SetEvictionPolicy(1000)
	// size out of range
	ec3 := nearcache.EvictionConfig{}
	ec3.SetSize(-1)
	testCases := []testCase{
		{
			name: "has both policy and comparator",
			cfg:  nearcache.Config{EvictionConfig: ec1},
		},
		{
			name: "invalid policy",
			cfg:  nearcache.Config{EvictionConfig: ec2},
		},
		{
			name: "size out of range",
			cfg:  nearcache.Config{EvictionConfig: ec3},
		},
	}
	for _, tc := range testCases {
		tc.Run(t)
	}
}

func TestPreloaderConfigInvalid(t *testing.T) {
	testCases := []testCase{
		{
			name: "negative store initial delay seconds",
			cfg: nearcache.Config{
				PreloaderConfig: nearcache.PreloaderConfig{
					StoreInitialDelaySeconds: -1,
				},
			},
		},
		{
			name: "negative store interval seconds",
			cfg: nearcache.Config{
				PreloaderConfig: nearcache.PreloaderConfig{
					StoreIntervalSeconds: -1,
				},
			},
		},
		{
			name: "blank directory when enabled",
			cfg: nearcache.Config{
				PreloaderConfig: nearcache.PreloaderConfig{
					Enabled: true,
				},
			},
		},
	}
	for _, tc := range testCases {
		tc.Run(t)
	}
}

func TestConfigInvalidNon32bit(t *testing.T) {
	it.SkipIf(t, "arch = 386")
	mi32 := math.MaxInt32 // makes compiler ignore MaxInt32+1 on 32 bit platforms
	// big eviction size
	ec := nearcache.EvictionConfig{}
	ec.SetSize(mi32 + 1)
	// big store initial delay seconds
	pc1 := nearcache.PreloaderConfig{StoreInitialDelaySeconds: mi32 + 1}
	// big store interval seconds
	pc2 := nearcache.PreloaderConfig{StoreIntervalSeconds: mi32 + 1}
	testCases := []testCase{
		{
			name: "big time to live",
			cfg:  nearcache.Config{TimeToLiveSeconds: mi32 + 1},
		},
		{
			name: "big max idle",
			cfg:  nearcache.Config{MaxIdleSeconds: mi32 + 1},
		},
		{
			name: "big eviction size",
			cfg:  nearcache.Config{EvictionConfig: ec},
		},
		{
			name: "big store initial delay seconds",
			cfg:  nearcache.Config{PreloaderConfig: pc1},
		},
		{
			name: "big store big store interval seconds seconds",
			cfg:  nearcache.Config{PreloaderConfig: pc2},
		},
	}
	for _, tc := range testCases {
		tc.Run(t)
	}
}

func assertTrueGetNearCacheConfig(t *testing.T, config hazelcast.Config, pattern string) nearcache.Config {
	nc, ok, err := config.GetNearCacheConfig(pattern)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("%s: GetNearCacheConfig expected to return true for: %s", t.Name(), pattern)
	}
	return nc
}

func assertFalseGetNearCacheConfig(t *testing.T, config hazelcast.Config, pattern string) {
	_, ok, err := config.GetNearCacheConfig(pattern)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("%s: GetNearCacheConfig expected to return false for: %s", t.Name(), pattern)
	}
}

func configWithNearCacheNames(names ...string) (hazelcast.Config, []nearcache.Config) {
	config := hazelcast.Config{}
	var ncs []nearcache.Config
	for _, name := range names {
		nc := nearcache.Config{Name: name}
		config.AddNearCacheConfig(nc)
		ncs = append(ncs, nc)
	}
	return config, ncs
}
