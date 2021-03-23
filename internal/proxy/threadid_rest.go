// +build !linux

package proxy

func threadID() int64 {
	return int64(1)
}
