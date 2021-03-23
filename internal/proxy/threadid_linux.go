// +build linux

package proxy

import "syscall"

func threadID() int64 {
	return int64(syscall.Gettid())
}
