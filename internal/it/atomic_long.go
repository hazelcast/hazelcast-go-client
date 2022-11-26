package it

import (
	"context"
	hz "github.com/hazelcast/hazelcast-go-client"
	"testing"
)

func AtomicLongTester(t *testing.T, f func(t *testing.T, a *hz.AtomicLong)) {
	AtomicLongTesterWithConfig(t, nil, f)
}

func AtomicLongTesterWithConfig(t *testing.T, configCallback func(*hz.Config), f func(t *testing.T, a *hz.AtomicLong)) {
	makeName := func() string {
		return NewUniqueObjectName("atomic-long")
	}
	AtomicLongTesterWithConfigAndName(t, makeName, configCallback, f)
}

func AtomicLongTesterWithConfigAndName(t *testing.T, makeName func() string, configCallback func(*hz.Config), f func(t *testing.T, a *hz.AtomicLong)) {
	ensureRemoteController(true)
	runner := func(t *testing.T, smart bool) {
		cls := cpEnabledTestCluster.Launch(t)
		config := cls.DefaultConfig()
		if configCallback != nil {
			configCallback(&config)
		}
		config.Cluster.Unisocket = !smart
		client, atm := GetClientAtomicLongWithConfig(makeName(), &config)
		defer func() {
			ctx := context.Background()
			if err := atm.Destroy(ctx); err != nil {
				t.Logf("test warning, could not destroy atomic long conter: %s", err.Error())
			}
			if err := client.Shutdown(ctx); err != nil {
				t.Logf("Test warning, client not shutdown: %s", err.Error())
			}
		}()
		f(t, atm)
	}
	if SmartEnabled() {
		t.Run("Smart Client", func(t *testing.T) {
			runner(t, true)
		})
	}
	if NonSmartEnabled() {
		t.Run("Non-Smart Client", func(t *testing.T) {
			runner(t, false)
		})
	}
}

func GetClientAtomicLongWithConfig(name string, config *hz.Config) (*hz.Client, *hz.AtomicLong) {
	client := getDefaultClient(config)
	cp := client.CPSubsystem()
	if a, err := cp.GetAtomicLong(context.Background(), name); err != nil {
		panic(err)
	} else {
		return client, a
	}
}
