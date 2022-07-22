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
	for i, tt := range tcs {
		t.Run(fmt.Sprintf("makeString case: %d", i), func(t *testing.T) {
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
	for i, tt := range tcs {
		t.Run(fmt.Sprintf("makeString case: %d", i), func(t *testing.T) {
			if got := tt.aggInstance.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
