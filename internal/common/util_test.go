package common

import "testing"

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
