package aggregate

import (
	"fmt"
	"testing"
)

func TestMakeString(t *testing.T) {
	tcs := []struct {
		name     string
		attrPath string
		want     string
	}{
		{
			name:     "test",
			attrPath: "attribute",
			want:     "test(attribute)",
		},
		{
			name:     "",
			attrPath: "",
			want:     "()",
		},
	}
	for ind, tt := range tcs {
		t.Run(fmt.Sprintf("makeString case: %d", ind), func(t *testing.T) {
			if got := makeString(tt.name, tt.attrPath); got != tt.want {
				t.Errorf("makeString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAggStringer(t *testing.T) {
	tcs := []struct {
		aggInstance fmt.Stringer
		want        string
	}{
		{
			aggInstance: Count("attribute"),
			want:        "Count(attribute)",
		},
		{
			aggInstance: DistinctValues("attribute"),
			want:        "DistinctValues(attribute)",
		},
		{
			aggInstance: DoubleSum("attribute"),
			want:        "DoubleSum(attribute)",
		},
		{
			aggInstance: DoubleAverage("attribute"),
			want:        "DoubleAverage(attribute)",
		},
		{
			aggInstance: IntSum("attribute"),
			want:        "IntSum(attribute)",
		},
		{
			aggInstance: IntAverage("attribute"),
			want:        "IntAverage(attribute)",
		},
		{
			aggInstance: LongSum("attribute"),
			want:        "LongSum(attribute)",
		},
		{
			aggInstance: LongAverage("attribute"),
			want:        "LongAverage(attribute)",
		},
		{
			aggInstance: Max("attribute"),
			want:        "Max(attribute)",
		},

		{
			aggInstance: Min("attribute"),
			want:        "Min(attribute)",
		},
	}
	for ind, tt := range tcs {
		t.Run(fmt.Sprintf("makeString case: %d", ind), func(t *testing.T) {
			if got := tt.aggInstance.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
