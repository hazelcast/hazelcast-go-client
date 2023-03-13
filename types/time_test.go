/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package types_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestTime(t *testing.T) {
	testCases := []struct {
		f    func(t *testing.T)
		name string
	}{
		{name: "LocalDateToTime", f: localDateToTimeTest},
		{name: "LocalDateString", f: localDateStringTest},
		{name: "LocalTimeToTime", f: localTimeToTimeTest},
		{name: "LocalTimeString", f: localTimeStringTest},
		{name: "LocalDateTimeToTime", f: localDateTimeToTimeTest},
		{name: "LocalDateTimeString", f: localDateTimeStringTest},
		{name: "OffsetDateTimeToTime", f: offsetDateTimeToTimeTest},
		{name: "OffsetDateTimeString", f: offsetDateTimeStringTest},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.f)
	}
}

func localDateToTimeTest(t *testing.T) {
	testCases := []struct {
		ld   types.LocalDate
		time time.Time
	}{
		{ld: types.LocalDate(time.Date(2021, 12, 21, 0, 0, 0, 0, time.Local)), time: time.Date(2021, time.December, 21, 0, 0, 0, 0, time.Local)},
		{ld: types.LocalDate(time.Date(1914, 7, 28, 0, 0, 0, 0, time.Local)), time: time.Date(1914, time.July, 28, 0, 0, 0, 0, time.Local)},
		{ld: types.LocalDate(time.Date(1923, 4, 23, 0, 0, 0, 0, time.Local)), time: time.Date(1923, time.April, 23, 0, 0, 0, 0, time.Local)},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.time, tc.ld.ToTime())
	}
}

func localDateStringTest(t *testing.T) {
	testCases := []struct {
		ld  types.LocalDate
		str string
	}{
		{ld: types.LocalDate(time.Date(2021, 12, 21, 0, 0, 0, 0, time.UTC)), str: "2021-12-21 00:00:00 +0000 UTC"},
		{ld: types.LocalDate(time.Date(1914, 7, 28, 0, 0, 0, 0, time.UTC)), str: "1914-07-28 00:00:00 +0000 UTC"},
		{ld: types.LocalDate(time.Date(1923, 4, 23, 0, 0, 0, 0, time.UTC)), str: "1923-04-23 00:00:00 +0000 UTC"},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.str, tc.ld.String())
	}
}

func localTimeToTimeTest(t *testing.T) {
	testCases := []struct {
		lt   types.LocalTime
		time time.Time
	}{
		{lt: types.LocalTime(time.Date(0, 1, 1, 14, 15, 16, 200, time.Local)), time: time.Date(0, 1, 1, 14, 15, 16, 200, time.Local)},
		{lt: types.LocalTime(time.Date(0, 1, 1, 9, 5, 20, 400, time.Local)), time: time.Date(0, 1, 1, 9, 5, 20, 400, time.Local)},
		{lt: types.LocalTime(time.Date(0, 1, 1, 23, 59, 59, 999, time.Local)), time: time.Date(0, 1, 1, 23, 59, 59, 999, time.Local)},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.time, tc.lt.ToTime())
	}
}

func localTimeStringTest(t *testing.T) {
	testCases := []struct {
		ld  types.LocalTime
		str string
	}{
		{ld: types.LocalTime(time.Date(0, 1, 1, 14, 15, 16, 200, time.UTC)), str: "0000-01-01 14:15:16.0000002 +0000 UTC"},
		{ld: types.LocalTime(time.Date(0, 1, 1, 9, 5, 20, 400, time.UTC)), str: "0000-01-01 09:05:20.0000004 +0000 UTC"},
		{ld: types.LocalTime(time.Date(0, 1, 1, 23, 59, 59, 999, time.UTC)), str: "0000-01-01 23:59:59.000000999 +0000 UTC"},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.str, tc.ld.String())
	}
}

func localDateTimeToTimeTest(t *testing.T) {
	testCases := []struct {
		ldt  types.LocalDateTime
		time time.Time
	}{
		{ldt: types.LocalDateTime(time.Date(2021, 12, 21, 14, 15, 16, 200, time.Local)), time: time.Date(2021, 12, 21, 14, 15, 16, 200, time.Local)},
		{ldt: types.LocalDateTime(time.Date(1914, 7, 28, 9, 5, 20, 400, time.Local)), time: time.Date(1914, 7, 28, 9, 5, 20, 400, time.Local)},
		{ldt: types.LocalDateTime(time.Date(1923, 4, 23, 23, 59, 59, 999, time.Local)), time: time.Date(1923, 4, 23, 23, 59, 59, 999, time.Local)},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.time, tc.ldt.ToTime())
	}
}

func localDateTimeStringTest(t *testing.T) {
	testCases := []struct {
		ldt types.LocalDateTime
		str string
	}{
		{ldt: types.LocalDateTime(time.Date(2021, 12, 21, 14, 15, 16, 200, time.UTC)), str: "2021-12-21 14:15:16.0000002 +0000 UTC"},
		{ldt: types.LocalDateTime(time.Date(1914, 7, 28, 9, 5, 20, 400, time.UTC)), str: "1914-07-28 09:05:20.0000004 +0000 UTC"},
		{ldt: types.LocalDateTime(time.Date(1923, 4, 23, 23, 59, 59, 999, time.UTC)), str: "1923-04-23 23:59:59.000000999 +0000 UTC"},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.str, tc.ldt.String())
	}
}

func offsetDateTimeToTimeTest(t *testing.T) {
	testCases := []struct {
		odt  types.OffsetDateTime
		time time.Time
	}{
		{odt: types.OffsetDateTime(time.Date(2021, 12, 21, 14, 15, 16, 200, time.UTC)), time: time.Date(2021, 12, 21, 14, 15, 16, 200, time.UTC)},
		{odt: types.OffsetDateTime(time.Date(1914, 7, 28, 9, 5, 20, 400, time.Local)), time: time.Date(1914, 7, 28, 9, 5, 20, 400, time.Local)},
		{odt: types.OffsetDateTime(time.Date(1923, 4, 23, 23, 59, 59, 999, time.FixedZone("", 3*60*60))), time: time.Date(1923, 4, 23, 23, 59, 59, 999, time.FixedZone("", 3*60*60))},
		{odt: types.OffsetDateTime(time.Date(2025, 2, 6, 4, 07, 15, 500, time.FixedZone("", -5*60*60))), time: time.Date(2025, 2, 6, 4, 07, 15, 500, time.FixedZone("", -5*60*60))},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.time, tc.odt.ToTime())
	}
}

func offsetDateTimeStringTest(t *testing.T) {
	testCases := []struct {
		odt types.OffsetDateTime
		str string
	}{
		{odt: types.OffsetDateTime(time.Date(2021, 12, 21, 14, 15, 16, 200, time.UTC)), str: "2021-12-21 14:15:16.0000002 +0000 UTC"},
		{odt: types.OffsetDateTime(time.Date(1923, 4, 23, 23, 59, 59, 999, time.FixedZone("UTC+3", 3*60*60))), str: "1923-04-23 23:59:59.000000999 +0300 UTC+3"},
		{odt: types.OffsetDateTime(time.Date(2025, 2, 6, 4, 07, 15, 50000, time.FixedZone("UTC-5", -5*60*60))), str: "2025-02-06 04:07:15.00005 -0500 UTC-5"},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.str, tc.odt.String())
	}
}
