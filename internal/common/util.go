package common

import (
	"net"
	"strconv"
	"strings"
)

func IsValidIpAddress(addr string) bool {
	return net.ParseIP(addr) != nil
}
func GetIpAndPort(addr string) (string, int) {
	var port int
	parts := strings.Split(addr, ":")
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		port = 5701 // Default port
	}
	addr = parts[0]
	return addr, port
}
