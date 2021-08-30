package internal

import "time"

const (
	DefaultConnectionTimeoutWithoutFailover = 24 * 366 * time.Hour
	DefaultConnectionTimeoutWithFailover    = 120 * time.Second
)
