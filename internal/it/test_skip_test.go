package it

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/stretchr/testify/assert"
)

func TestSkipIf(t *testing.T) {
	testCases := []struct {
		name       string
		conditions string

		expectPanic bool
		expectSkip  bool
	}{
		// Check parsing
		{name: "incorrect version format", conditions: "version < 1.0.0", expectPanic: true},
		{name: "incorrect os format", conditions: "os windows", expectPanic: true},
		{name: "incorrect enterprise format", conditions: "enterprise = os", expectPanic: true},
		{name: "empty condition #1", conditions: "enterprise = os, , ver > 0", expectPanic: true},
		{name: "empty condition #2", conditions: "enterprise = os,, ver > 0", expectPanic: true},

		// Check OS based on non-existing OS
		{name: "skip if not non-existing OS", conditions: "os != non-existent", expectSkip: true},
		{name: "skip if non-existing OS", conditions: "os = non-existent"},

		// Check version compared to 0
		{name: "skip version > 0.0.0", conditions: "ver > 0.0.0", expectSkip: true},
		{name: "skip version > 0", conditions: "ver > 0", expectSkip: true},
		{name: "skip version >= 0.0.0", conditions: "ver >= 0.0.0", expectSkip: true},
		{name: "skip version < 0.0.0", conditions: "ver < 0.0.0"},
		{name: "skip version <= 0.0.0", conditions: "ver <= 0.0.0"},
		{name: "skip version = 0.0.0", conditions: "ver = 0.0.0"},
		{name: "skip version != 0.0.0", conditions: "ver != 0.0.0", expectSkip: true},

		// Check version compared to 100
		{name: "skip version > 100.0.0", conditions: "ver > 100.0.0"},
		{name: "skip version > 100.10", conditions: "ver > 100.10"},
		{name: "skip version >= 100.0.0", conditions: "ver >= 100.0.0"},
		{name: "skip version < 100.0.0", conditions: "ver < 100.0.0", expectSkip: true},
		{name: "skip version <= 100.0.0", conditions: "ver <= 100.0.0", expectSkip: true},
		{name: "skip version = 100.0.0", conditions: "ver = 100.0.0"},
		{name: "skip version != 100.0.0", conditions: "ver != 100.0.0", expectSkip: true},

		// Check version compared to actual versiojn
		{name: "skip version >= actual", conditions: "ver >= " + internal.ClientVersion, expectSkip: true},
		{name: "skip version < actual", conditions: "ver > " + internal.ClientVersion},
		{name: "skip version <= actual", conditions: "ver <= " + internal.ClientVersion, expectSkip: true},
		{name: "skip version < actual", conditions: "ver > " + internal.ClientVersion},
		{name: "skip version = actual", conditions: "ver = " + internal.ClientVersion, expectSkip: true},
		{name: "skip version != actual", conditions: "ver != " + internal.ClientVersion},

		// Check hz
		{name: "skip hz != 100.0.0", conditions: "hz != 100.0.0", expectSkip: true},
		{name: "skip hz = 100", conditions: "hz = 100"},

		// Multiple conditions
		{name: "skip hz = 100 or version = 100", conditions: "hz = 100, ver = 100"},
		{name: "skip hz = 100 or version < 100", conditions: "hz = 100.0.0, ver < 100", expectSkip: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectPanic {
				assert.Panics(t, func() { canSkip(tc.conditions) })
			} else {
				skip := canSkip(tc.conditions)
				assert.Equal(t, tc.expectSkip, skip)
			}
		})
	}
}
