//go:build hazelcastinternal && hazelcastinternaltest
// +build hazelcastinternal,hazelcastinternaltest

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

package serialization_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestInternalSerializationMethods(t *testing.T) {
	testCases := []struct {
		name string
		f    func(t *testing.T)
	}{
		{name: "DefaultCompactSerializer", f: defaultCompactSerializerTest},
		{name: "DefaultPortableSerializer", f: defaultPortableSerializerTest},
		{name: "OverrideBuiltinSerializer", f: overrideBuiltinSerializerTest},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.f)
	}
}

func defaultCompactSerializerTest(t *testing.T) {
	var sc serialization.Config
	sc.Compact.SetSerializers(FooCompactSerializer{})
	tcx := it.MapTestContext{
		T: t,
		ConfigCallback: func(tcx it.MapTestContext) {
			tcx.Config.Serialization = sc
		},
	}
	tcx.Tester(func(tcx it.MapTestContext) {
		ctx := context.Background()
		hazelcast.SetDefaultCompactDeserializer(DefaultCompactDeserializer{})
		require.NoError(t, tcx.M.Set(ctx, "foo", NewFoo("abc")))
		cfg2 := *tcx.Config
		cfg2.Serialization.Compact = serialization.CompactConfig{}
		require.NoError(t, cfg2.Validate())
		client2 := it.MustClient(hazelcast.StartNewClientWithConfig(ctx, cfg2))
		defer client2.Shutdown(ctx)
		m2 := it.MustValue(client2.GetMap(ctx, tcx.MapName)).(*hazelcast.Map)
		v := it.MustValue(m2.Get(ctx, "foo"))
		require.Equal(t, NewFoo("OK"), v)
	})
}

func defaultPortableSerializerTest(t *testing.T) {
	hazelcast.SetDefaultPortableDeserializer(DefaultPortableDeserializer{})
	var cfg1 serialization.Config
	cfg1.SetPortableFactories(&FooPortableFactory{})
	ss1 := mustSerializationService(iserialization.NewService(&cfg1, nil))
	data := mustData(ss1.ToData(&FooPortableSerializer{foo: NewFoo("abc")}))
	var cfg2 serialization.Config
	ss2 := mustSerializationService(iserialization.NewService(&cfg2, nil))
	v := it.MustValue(ss2.ToObject(data))
	assert.Equal(t, &FooPortableSerializer{foo: NewFoo("OK")}, v)
}

func overrideBuiltinSerializerTest(t *testing.T) {
	hazelcast.SetBuiltinDeserializer(Int32Deserializer{})
	var cfg serialization.Config
	ss := mustSerializationService(iserialization.NewService(&cfg, nil))
	data := mustData(ss.ToData(int32(100)))
	v := it.MustValue(ss.ToObject(data))
	assert.Equal(t, int32(38), v)
}

type Foo struct {
	value *string
}

func NewFoo(v string) Foo {
	return Foo{value: &v}
}

func (f Foo) String() string {
	if f.value == nil {
		return ""
	}
	return *f.value
}

type FooCompactSerializer struct{}

func (s FooCompactSerializer) Type() reflect.Type {
	return reflect.TypeOf(Foo{})
}

func (s FooCompactSerializer) TypeName() string {
	return "foo"
}

func (s FooCompactSerializer) Read(r serialization.CompactReader) interface{} {
	return Foo{
		value: r.ReadString("value"),
	}
}

func (s FooCompactSerializer) Write(w serialization.CompactWriter, v interface{}) {
	vv := v.(Foo)
	w.WriteString("value", vv.value)
}

type DefaultCompactDeserializer struct{}

func (s DefaultCompactDeserializer) Read(schema *iserialization.Schema, reader serialization.CompactReader) interface{} {
	v := "OK"
	return Foo{value: &v}
}

type FooPortableSerializer struct {
	foo Foo
}

func (f *FooPortableSerializer) FactoryID() int32 {
	return 100
}

func (f *FooPortableSerializer) ClassID() int32 {
	return 50
}

func (f *FooPortableSerializer) WritePortable(w serialization.PortableWriter) {
	w.WriteString("value", f.foo.String())
}

func (f *FooPortableSerializer) ReadPortable(r serialization.PortableReader) {
	s := r.ReadString("value")
	f.foo = NewFoo(s)
}

type FooPortableFactory struct{}

func (f FooPortableFactory) Create(classID int32) serialization.Portable {
	if classID != 50 {
		panic("unexpected classID")
	}
	return &FooPortableSerializer{}
}

func (f FooPortableFactory) FactoryID() int32 {
	return 100
}

type DefaultPortableDeserializer struct{}

func (s DefaultPortableDeserializer) CreatePortableValue(factoryID, classID int32) serialization.Portable {
	return &FooPortableSerializer{foo: NewFoo("OK")}
}

func (s DefaultPortableDeserializer) ReadPortableWithClassDefinition(cd *serialization.ClassDefinition, reader serialization.PortableReader) {
	// pass
}

type Int32Deserializer struct{}

func (s Int32Deserializer) ID() int32 {
	return iserialization.TypeInt32
}

func (s Int32Deserializer) Read(input serialization.DataInput) interface{} {
	return int32(38)
}

func (s Int32Deserializer) Write(output serialization.DataOutput, object interface{}) {
	// pass
}
