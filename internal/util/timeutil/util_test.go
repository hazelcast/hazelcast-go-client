/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package timeutil

import (
	"math"
	"testing"
	"time"
)

func TestGetTimeInMilliSeconds(t *testing.T) {
	var expected int64 = 100
	if result := GetTimeInMilliSeconds(100 * time.Millisecond); result != expected {
		t.Fatal("An error in GetTimeInMilliSeconds()")
	}
	expected = expected * 1000
	if result := GetTimeInMilliSeconds(100 * time.Second); result != expected {
		t.Fatal("An error in GetTimeInMilliSeconds()")
	}
	expected = expected * 60
	if result := GetTimeInMilliSeconds(100 * time.Minute); result != expected {
		t.Fatal("An error in GetTimeInMilliSeconds()")
	}
	expected = expected * 60
	if result := GetTimeInMilliSeconds(100 * time.Hour); result != expected {
		t.Fatal("An error in GetTimeInMilliSeconds()")
	}
}

func TestGetTimeInMilliSecondsLessThanAMilliSecond(t *testing.T) {
	var expected int64 = 1
	if result := GetTimeInMilliSeconds(1 * time.Nanosecond); result != expected {
		t.Fatalf("Expected %d got %d", expected, result)
	}
}

func TestGetPositiveDurationOrMax(t *testing.T) {
	duration := -1 * time.Second
	if res := GetPositiveDurationOrMax(duration); res < 0 {
		t.Errorf("Expected positive time, got %d", res)
	}

	duration = 1 * time.Second

	if res := GetPositiveDurationOrMax(duration); res != duration {
		t.Errorf("Expected %d, got %d", duration, res)
	}
}

func TestGetCurrentTimeInMilliSeconds(t *testing.T) {
	now := time.Now().UnixNano()
	result := GetCurrentTimeInMilliSeconds()
	expected := now / int64(time.Millisecond)
	if math.Abs(float64(result-expected)) > float64(100*time.Millisecond) {
		t.Fatalf("expected %d got %d", expected, result)
	}

}
