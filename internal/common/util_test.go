package common

import (
	"testing"
	"time"
)

func TestIsValidIpAddress(t *testing.T) {
	ip := "142.1.2.4"
	if ok := IsValidIpAddress(ip); !ok {
		t.Fatal("IsValidIpAddress failed")
	}
	ip = "123.2.1"
	if ok := IsValidIpAddress(ip); ok {
		t.Fatal("IsValidIpAddress failed")
	}
}
func TestGetIpAndPort(t *testing.T) {
	testAddress := "121.1.23.3:1231"
	if ip, port := GetIpAndPort(testAddress); ip != "121.1.23.3" || port != 1231 {
		t.Fatal("GetIPAndPort failed.")
	}
}
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
