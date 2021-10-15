package cluster

import "testing"

func TestCalculateVersion(t *testing.T) {
	// TODO: convert this test to table driven
	// invalid versions
	testVersionString(t, "", -1)
	testVersionString(t, "a.3.7.5", -1)
	testVersionString(t, "3.a.5", -1)
	testVersionString(t, "3,7.5", -1)
	testVersionString(t, "3.7,5", -1)
	testVersionString(t, "10.99.RC1", -1)
	// valid versions
	testVersionString(t, "3.7.2", 30702)
	testVersionString(t, "1.99.30", 19930)
	testVersionString(t, "3.7-SNAPSHOT", 30700)
	testVersionString(t, "3.7.2-SNAPSHOT", 30702)
	testVersionString(t, "10.99.2-SNAPSHOT", 109902)
	testVersionString(t, "10.99.30-SNAPSHOT", 109930)
	testVersionString(t, "10.99-RC1", 109900)
}

func testVersionString(t *testing.T, version string, expected int32) {
	if result := calculateVersion(version); result != expected {
		t.Errorf("expected %d got %d", result, expected)
	}
}
