package internal

import "time"

const (
	DefaultConnectionTimeoutWithoutFailover = time.Duration(1<<63 - 1)
	DefaultConnectionTimeoutWithFailover    = 120 * time.Second
)
