package common

import (
	"testing"
	"time"
)

func TestGetTimeInMilliSeconds(t *testing.T) {
	var expected int64 = 100
	var baseTime int64 = 100
	if result := GetTimeInMilliSeconds(baseTime, time.Millisecond); result != expected {
		t.Fatal("An error in GetTimeInMilleSeconds()")
	}
	expected = expected * 1000
	if result := GetTimeInMilliSeconds(baseTime, time.Second); result != expected {
		t.Fatal("An error in GetTimeInMilleSeconds()")
	}
	expected = expected * 60
	if result := GetTimeInMilliSeconds(baseTime, time.Minute); result != expected {
		t.Fatal("An error in GetTimeInMilleSeconds()")
	}
	expected = expected * 60
	if result := GetTimeInMilliSeconds(baseTime, time.Hour); result != expected {
		t.Fatal("An error in GetTimeInMilleSeconds()")
	}
}
