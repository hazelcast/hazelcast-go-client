// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package aggregator is a utility package to create basic aggregators.
// Min/Max/Average aggregators are type specific, so an integerAvg() aggregator
// expects all elements to be integers.
// There is no conversion executed while accumulating on the server side, so if there is any other type met an exception
// will be thrown on the server side.
//
// The attributePath given in the factory method allows the aggregator to operate on the value extracted by navigating
// to the given attributePath on each object that has been returned from a query.
// The attribute path may be simple, e.g. "name", or nested "address.city".
// The given attribute path should not be empty, otherwise an error will be returned from client side.
//
// Server version should be at least 3.8.
package aggregator

import (
	"github.com/hazelcast/hazelcast-go-client/internal/aggregation"
)

// Count returns a Count aggregator that counts the input values from
// the given attribute path.
// The given attribute path should not be empty, otherwise an error will be returned.
func Count(attributePath string) (*aggregation.Count, error) {
	return aggregation.NewCount(attributePath)
}

// Float64Average returns a Float64Average aggregator that counts the input values from
// the given attribute path.
// The given attribute path should not be empty, otherwise an error will be returned.
func Float64Average(attributePath string) (*aggregation.Float64Average, error) {
	return aggregation.NewFloat64Average(attributePath)
}

// Float64Sum returns a Float64Sum aggregator that counts the input values from
// the given attribute path.
// The given attribute path should not be empty, otherwise an error will be returned.
func Float64Sum(attributePath string) (*aggregation.Float64Sum, error) {
	return aggregation.NewFloat64Sum(attributePath)
}

// FixedPointSum returns a FixedPointSum aggregator that counts the input values from
// the given attribute path.
// The given attribute path should not be empty, otherwise an error will be returned.
func FixedPointSum(attributePath string) (*aggregation.FixedPointSum, error) {
	return aggregation.NewFixedPointSum(attributePath)
}

// FloatingPointSum returns a FloatingPointSum aggregator that counts the input values from
// the given attribute path.
// The given attribute path should not be empty, otherwise an error will be returned.
func FloatingPointSum(attributePath string) (*aggregation.FloatingPointSum, error) {
	return aggregation.NewFloatingPointSum(attributePath)
}

// Max returns a Max aggregator that counts the input values from
// the given attribute path.
// The given attribute path should not be empty, otherwise an error will be returned.
func Max(attributePath string) (*aggregation.Max, error) {
	return aggregation.NewMax(attributePath)
}

// Min returns a Min aggregator that counts the input values from
// the given attribute path.
// The given attribute path should not be empty, otherwise an error will be returned.
func Min(attributePath string) (*aggregation.Min, error) {
	return aggregation.NewMin(attributePath)
}

// Int32Average returns a Int32Average aggregator that counts the input values from
// the given attribute path.
// The given attribute path should not be empty, otherwise an error will be returned.
func Int32Average(attributePath string) (*aggregation.Int32Average, error) {
	return aggregation.NewInt32Average(attributePath)
}

// Int32Sum returns a Int32Sum aggregator that counts the input values from
// the given attribute path.
// The given attribute path should not be empty, otherwise an error will be returned.
func Int32Sum(attributePath string) (*aggregation.Int32Sum, error) {
	return aggregation.NewInt32Sum(attributePath)
}

// Int64Average returns a Int64Average aggregator that counts the input values from
// the given attribute path.
// The given attribute path should not be empty, otherwise an error will be returned.
func Int64Average(attributePath string) (*aggregation.Int64Average, error) {
	return aggregation.NewInt64Average(attributePath)
}

// Int64Sum returns a Int64Sum aggregator that counts the input values from
// the given attribute path.
// The given attribute path should not be empty, otherwise an error will be returned.
func Int64Sum(attributePath string) (*aggregation.Int64Sum, error) {
	return aggregation.NewInt64Sum(attributePath)
}
