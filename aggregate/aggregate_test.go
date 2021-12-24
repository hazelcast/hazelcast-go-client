package aggregate

import (
	"fmt"
	"testing"
)

func Test_makeString(t *testing.T) {
	tests := []struct {
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
	for ind, tt := range tests {
		t.Run(fmt.Sprintf("makeString case: %d", ind), func(t *testing.T) {
			if got := makeString(tt.name, tt.attrPath); got != tt.want {
				t.Errorf("makeString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_aggStringer(t *testing.T) {
	tests := []struct {
		aggInstance fmt.Stringer
		want        string
	}{
		{
			aggInstance: aggCount{
				attrPath: "attribute",
			},
			want: "Count(attribute)",
		},
		{
			aggInstance: aggDistinct{
				attrPath: "attribute",
			},
			want: "DistinctValues(attribute)",
		},
		{
			aggInstance: aggDoubleSum{
				attrPath: "attribute",
			},
			want: "DoubleSum(attribute)",
		},
		{
			aggInstance: aggDoubleAverage{
				attrPath: "attribute",
			},
			want: "DoubleAverage(attribute)",
		},
		{
			aggInstance: aggIntSum{
				attrPath: "attribute",
			},
			want: "IntSum(attribute)",
		},
		{
			aggInstance: aggIntAverage{
				attrPath: "attribute",
			},
			want: "IntAverage(attribute)",
		},
		{
			aggInstance: aggLongSum{
				attrPath: "attribute",
			},
			want: "LongSum(attribute)",
		},
		{
			aggInstance: aggLongAverage{
				attrPath: "attribute",
			},
			want: "LongAverage(attribute)",
		},
		{
			aggInstance: aggMax{
				attrPath: "attribute",
			},
			want: "Max(attribute)",
		},

		{
			aggInstance: aggMin{
				attrPath: "attribute",
			},
			want: "Min(attribute)",
		},
	}
	for ind, tt := range tests {
		t.Run(fmt.Sprintf("makeString case: %d", ind), func(t *testing.T) {
			if got := tt.aggInstance.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
