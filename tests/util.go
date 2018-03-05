// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package tests

import (
	"sync"
	"testing"
	"time"
)

var Timeout time.Duration = 1 * time.Minute

const DEFAULT_XML_CONFIG string = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config hazelcast-config-3.9.xsd\" xmlns=\"http://www.hazelcast.com/schema/config\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">" +
	"<serialization>" +
	"<data-serializable-factories>" +
	"<data-serializable-factory factory-id=\"66\">com.hazelcast.client.test.IdentifiedFactory" +
	"</data-serializable-factory>" +
	"</data-serializable-factories>" +
	"</serialization>" +
	"</hazelcast>"

func AssertEqualf(t *testing.T, err error, l interface{}, r interface{}, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if l != r {
		t.Fatalf("%v != %v : %v", l, r, message)
	}
}
func AssertNilf(t *testing.T, err error, l interface{}, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if l != nil {
		t.Fatalf("%v != nil", l)
	}
}
func AssertErrorNotNil(t *testing.T, err error, message string) {
	if err == nil {
		t.Fatal(message)
	}
}
func AssertEqual(t *testing.T, err error, l interface{}, r interface{}) {
	if err != nil {
		t.Fatal(err)
	}
	if l != r {
		t.Fatalf("%v != %v", l, r)
	}

}
func WaitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
