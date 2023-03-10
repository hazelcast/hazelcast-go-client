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
		{name: "TestLocalDateToTime", f: localDateToTimeTest},
		{name: "TestLocalDateString", f: localDateStringTest},
		{name: "TestLocalTimeToTime", f: localTimeToTimeTest},
		{name: "TestLocalTimeString", f: localTimeStringTest},
		{name: "TestLocalDateTimeToTime", f: localDateTimeToTimeTest},
		{name: "TestLocalDateTimeString", f: localDateTimeStringTest},
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
		{ld: types.LocalDate(time.Date(2021, 12, 21, 0, 0, 0, 0, time.Local)), str: "2021-12-21"},
		{ld: types.LocalDate(time.Date(1914, 7, 28, 0, 0, 0, 0, time.Local)), str: "1914-07-28"},
		{ld: types.LocalDate(time.Date(1923, 4, 23, 0, 0, 0, 0, time.Local)), str: "1923-04-23"},
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
		{ld: types.LocalTime(time.Date(0, 1, 1, 14, 15, 16, 200, time.Local)), str: "14:15:16.0000002"},
		{ld: types.LocalTime(time.Date(0, 1, 1, 9, 5, 20, 400, time.Local)), str: "09:05:20.0000004"},
		{ld: types.LocalTime(time.Date(0, 1, 1, 23, 59, 59, 999, time.Local)), str: "23:59:59.000000999"},
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
		{ldt: types.LocalDateTime(time.Date(2021, 12, 21, 14, 15, 16, 200, time.Local)), str: "2021-12-21T14:15:16.0000002"},
		{ldt: types.LocalDateTime(time.Date(1914, 7, 28, 9, 5, 20, 400, time.Local)), str: "1914-07-28T09:05:20.0000004"},
		{ldt: types.LocalDateTime(time.Date(1923, 4, 23, 23, 59, 59, 999, time.Local)), str: "1923-04-23T23:59:59.000000999"},
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
		{odt: types.OffsetDateTime(time.Date(2021, 12, 21, 14, 15, 16, 200, time.UTC)), str: "2021-12-21T14:15:16.0000002Z"},
		{odt: types.OffsetDateTime(time.Date(1923, 4, 23, 23, 59, 59, 999, time.FixedZone("UTC+3", 3*60*60))), str: "1923-04-23T23:59:59.000000999+03:00"},
		{odt: types.OffsetDateTime(time.Date(2025, 2, 6, 4, 07, 15, 500, time.FixedZone("UTC-5", -5*60*60))), str: "2025-02-06T04:07:15.0000005-05:00"},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.str, tc.odt.String())
	}
}
