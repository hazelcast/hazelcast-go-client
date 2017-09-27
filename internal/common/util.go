package common

import "time"

func CheckNotNil(v interface{}) bool {
	return v != nil
}
func CheckNotEmpty(v []interface{}) bool {
	return len(v) > 0
}
func GetTimeInMilliSeconds(time int64, duration time.Duration) int64 {
	var timeInMillis int64 = time * duration.Nanoseconds() / (1000000)
	if time > 0 && timeInMillis == 0 {
		timeInMillis = 1
	}
	return timeInMillis
}
