package it_test

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/stretchr/testify/assert"
)

func TestSkipIf(t *testing.T) {
	testCases := []struct {
		name       string
		conditions string

		expectFatal bool
		expectSkip  bool
	}{
		// Check parsing
		{name: "incorrect version format", conditions: "version < 1.0.0", expectFatal: true, expectSkip: true},
		{name: "incorrect os format", conditions: "os windows", expectFatal: true, expectSkip: true},
		{name: "incorrect enterprise format", conditions: "enterprise = os", expectFatal: true, expectSkip: true},

		// Check OS based on non-existing OS
		{name: "skip if not non-existing OS", conditions: "os != non-existent", expectFatal: false, expectSkip: true},
		{name: "skip if non-existing OS", conditions: "os = non-existent", expectFatal: false, expectSkip: false},

		// Check version compared to 0
		{name: "skip version > 0.0.0", conditions: "ver > 0.0.0", expectFatal: false, expectSkip: true},
		{name: "skip version > 0", conditions: "ver > 0", expectFatal: false, expectSkip: true},
		{name: "skip version >= 0.0.0", conditions: "ver >= 0.0.0", expectFatal: false, expectSkip: true},
		{name: "skip version < 0.0.0", conditions: "ver < 0.0.0", expectFatal: false, expectSkip: false},
		{name: "skip version <= 0.0.0", conditions: "ver <= 0.0.0", expectFatal: false, expectSkip: false},
		{name: "skip version = 0.0.0", conditions: "ver = 0.0.0", expectFatal: false, expectSkip: false},
		{name: "skip version != 0.0.0", conditions: "ver != 0.0.0", expectFatal: false, expectSkip: true},

		// Check version compared to 100
		{name: "skip version > 100.0.0", conditions: "ver > 100.0.0", expectFatal: false, expectSkip: false},
		{name: "skip version > 100.10", conditions: "ver > 100.10", expectFatal: false, expectSkip: false},
		{name: "skip version >= 100.0.0", conditions: "ver >= 100.0.0", expectFatal: false, expectSkip: false},
		{name: "skip version < 100.0.0", conditions: "ver < 100.0.0", expectFatal: false, expectSkip: true},
		{name: "skip version <= 100.0.0", conditions: "ver <= 100.0.0", expectFatal: false, expectSkip: true},
		{name: "skip version = 100.0.0", conditions: "ver = 100.0.0", expectFatal: false, expectSkip: false},
		{name: "skip version != 100.0.0", conditions: "ver != 100.0.0", expectFatal: false, expectSkip: true},

		// Check version compared to actual version
		{name: "skip version >= actual", conditions: "ver >= " + internal.ClientVersion, expectFatal: false, expectSkip: true},
		{name: "skip version < actual", conditions: "ver > " + internal.ClientVersion, expectFatal: false, expectSkip: false},
		{name: "skip version <= actual", conditions: "ver <= " + internal.ClientVersion, expectFatal: false, expectSkip: true},
		{name: "skip version < actual", conditions: "ver > " + internal.ClientVersion, expectFatal: false, expectSkip: false},
		{name: "skip version = actual", conditions: "ver = " + internal.ClientVersion, expectFatal: false, expectSkip: true},
		{name: "skip version != actual", conditions: "ver != " + internal.ClientVersion, expectFatal: false, expectSkip: false},

		// Check hz
		{name: "skip hz != 100.0.0", conditions: "hz != 100.0.0", expectFatal: false, expectSkip: true},
		{name: "skip hz = 100", conditions: "hz = 100", expectFatal: false, expectSkip: false},

		// Multiple conditions
		{name: "skip hz = 100 or version = 100", conditions: "hz = 100, ver = 100", expectFatal: false, expectSkip: false},
		{name: "skip hz = 100 or version < 100", conditions: "hz = 100.0.0, ver < 100", expectFatal: false, expectSkip: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			skip, err := it.InternalSkipIf(tc.conditions)

			// Then
			assert.Equal(t, tc.expectSkip, skip)
			if tc.expectFatal {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
