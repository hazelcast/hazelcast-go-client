/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package nearcache

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/internal/it/skip"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

type recordCost struct {
	makeRec   func() *Record
	estimator nearCacheStorageEstimator
	name      string
	cost      int64
}

func TestGetRecordStorageMemoryCost_64bit(t *testing.T) {
	skip.IfNot(t, "arch ~ 64bit")
	cfg := pubserialization.Config{}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	ss, err := serialization.NewService(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	dataEstimator := nearCacheDataStoreAdapter{ss: ss}
	valueEstimator := nearCacheValueStoreAdapter{ss: ss}
	testCases := []recordCost{
		{
			name:      "data estimator: nil record",
			estimator: dataEstimator,
			makeRec: func() *Record {
				return nil
			},
			cost: 0,
		},
		{
			name:      "data estimator: record with nil value",
			estimator: dataEstimator,
			makeRec: func() *Record {
				rec := &Record{}
				rec.SetValue(nil)
				return rec
			},
			cost: 84,
		},
		{
			name:      "data estimator: record with nil value and serialization.Data type",
			estimator: dataEstimator,
			makeRec: func() *Record {
				rec := &Record{}
				var ds serialization.Data
				rec.SetValue(ds)
				return rec
			},
			cost: 84,
		},
		{
			name:      "data estimator: record with value",
			estimator: dataEstimator,
			makeRec: func() *Record {
				rec := &Record{}
				v, err := ss.ToData("hello")
				if err != nil {
					panic(err)
				}
				rec.SetValue(v)
				return rec
			},
			cost: 93,
		},
		{
			name:      "value estimator: nil record",
			estimator: valueEstimator,
			makeRec: func() *Record {
				return nil
			},
			cost: 0,
		},
		{
			name:      "value estimator: record with nil value",
			estimator: valueEstimator,
			makeRec: func() *Record {
				rec := &Record{}
				rec.SetValue(nil)
				return rec
			},
			cost: 0,
		},
		{
			name:      "value estimator: record with value",
			estimator: valueEstimator,
			makeRec: func() *Record {
				rec := &Record{}
				rec.SetValue("hello")
				return rec
			},
			cost: 0,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			targetCost := tc.cost
			rec := tc.makeRec()
			cost := tc.estimator.GetRecordStorageMemoryCost(rec)
			require.Equal(t, targetCost, cost)
		})
	}
}

func TestGetRecordStorageMemoryCost_32bit(t *testing.T) {
	skip.IfNot(t, "arch ~ 32bit")
	cfg := pubserialization.Config{}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	ss, err := serialization.NewService(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	dataEstimator := nearCacheDataStoreAdapter{ss: ss}
	valueEstimator := nearCacheValueStoreAdapter{ss: ss}
	testCases := []recordCost{
		{
			name:      "data estimator: nil record",
			estimator: dataEstimator,
			makeRec: func() *Record {
				return nil
			},
			cost: 0,
		},
		{
			name:      "data estimator: record with nil value",
			estimator: dataEstimator,
			makeRec: func() *Record {
				rec := &Record{}
				rec.SetValue(nil)
				return rec
			},
			cost: 80,
		},
		{
			name:      "data estimator: record with nil value and serialization.Data type",
			estimator: dataEstimator,
			makeRec: func() *Record {
				rec := &Record{}
				var ds serialization.Data
				rec.SetValue(ds)
				return rec
			},
			cost: 80,
		},
		{
			name:      "data estimator: record with value",
			estimator: dataEstimator,
			makeRec: func() *Record {
				rec := &Record{}
				v, err := ss.ToData("hello")
				if err != nil {
					panic(err)
				}
				rec.SetValue(v)
				return rec
			},
			cost: 89,
		},
		{
			name:      "value estimator: nil record",
			estimator: valueEstimator,
			makeRec: func() *Record {
				return nil
			},
			cost: 0,
		},
		{
			name:      "value estimator: record with nil value",
			estimator: valueEstimator,
			makeRec: func() *Record {
				rec := &Record{}
				rec.SetValue(nil)
				return rec
			},
			cost: 0,
		},
		{
			name:      "value estimator: record with value",
			estimator: valueEstimator,
			makeRec: func() *Record {
				rec := &Record{}
				rec.SetValue("hello")
				return rec
			},
			cost: 0,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			targetCost := tc.cost
			rec := tc.makeRec()
			cost := tc.estimator.GetRecordStorageMemoryCost(rec)
			require.Equal(t, targetCost, cost)
		})
	}
}
