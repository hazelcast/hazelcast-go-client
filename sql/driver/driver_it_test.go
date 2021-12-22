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

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"

	"github.com/hazelcast/hazelcast-go-client/sql/driver"
)

const (
	recordFactoryID = 100
	recordClassID   = 1
)

type Record struct {
	NullValue     interface{}
	VarcharValue  string
	DoubleValue   float64
	BigIntValue   int64
	RealValue     float32
	IntegerValue  int32
	SmallIntValue int16
	TinyIntValue  int8
	BoolValue     bool
}

func (r Record) Create(classID int32) serialization.Portable {
	if classID == recordClassID {
		return &Record{}
	}
	panic(fmt.Sprintf("unknown class ID: %d", classID))
}

func (r Record) FactoryID() int32 {
	return recordFactoryID
}

func (r Record) ClassID() int32 {
	return recordClassID
}

func (r Record) WritePortable(writer serialization.PortableWriter) {
	writer.WriteString("varcharvalue", r.VarcharValue)
	writer.WriteByte("tinyintvalue", byte(r.TinyIntValue))
	writer.WriteInt16("smallintvalue", r.SmallIntValue)
	writer.WriteInt32("integervalue", r.IntegerValue)
	writer.WriteInt64("bigintvalue", r.BigIntValue)
	writer.WriteBool("boolvalue", r.BoolValue)
	writer.WriteFloat32("realvalue", r.RealValue)
	writer.WriteFloat64("doublevalue", r.DoubleValue)
}

func (r *Record) ReadPortable(reader serialization.PortableReader) {
	r.VarcharValue = reader.ReadString("varcharvalue")
	r.TinyIntValue = int8(reader.ReadByte("tinyintvalue"))
	r.SmallIntValue = reader.ReadInt16("smallintvalue")
	r.IntegerValue = reader.ReadInt32("integervalue")
	r.BigIntValue = reader.ReadInt64("bigintvalue")
	r.BoolValue = reader.ReadBool("boolvalue")
	r.RealValue = reader.ReadFloat32("realvalue")
	r.DoubleValue = reader.ReadFloat64("doublevalue")
}

func TestSQLQuery(t *testing.T) {
	testCases := []struct {
		keyFn, valueFn   func(i int) interface{}
		keyFmt, valueFmt string
	}{
		{
			keyFmt: "int", valueFmt: "int",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return int32(i * 100) },
		},
		{
			keyFmt: "int", valueFmt: "varchar",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return fmt.Sprintf("val-%d", i*100) },
		},
	}
	for _, tc := range testCases {
		name := fmt.Sprintf("%s/%s", tc.keyFmt, tc.valueFmt)
		t.Run(name, func(t *testing.T) {
			testSQLQuery(t, context.Background(), tc.keyFmt, tc.valueFmt, tc.keyFn, tc.valueFn)
		})
	}
}

func TestSQLWithPortableData(t *testing.T) {
	cb := func(c *hz.Config) {
		c.Serialization.SetPortableFactories(&Record{})
	}
	it.SQLTesterWithConfigBuilder(t, cb, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		dsn := makeDSN(config)
		sc := &serialization.Config{}
		sc.SetPortableFactories(&Record{})
		if err := driver.SetSerializationConfig(sc); err != nil {
			t.Fatal(err)
		}
		defer driver.SetSerializationConfig(nil)
		db, err := sql.Open("hazelcast", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		it.MustValue(db.Exec(fmt.Sprintf(`
			CREATE MAPPING "%s" (
				__key BIGINT,
				varcharvalue VARCHAR,
				tinyintvalue TINYINT,
				smallintvalue SMALLINT,
				integervalue INTEGER,
				bigintvalue BIGINT,
				boolvalue BOOLEAN,
				realvalue REAL,
				doublevalue DOUBLE
			)
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'portable',
				'valuePortableFactoryId' = '100',
				'valuePortableClassId' = '1'
			)
		`, mapName)))
		rec := &Record{
			VarcharValue:  "hello",
			TinyIntValue:  -128,
			SmallIntValue: 32767,
			IntegerValue:  -27,
			BigIntValue:   38,
			BoolValue:     true,
			RealValue:     -5.32,
			DoubleValue:   12.789,
		}
		it.Must(m.Set(context.TODO(), 1, rec))
		// select the value
		rows, err := db.Query(fmt.Sprintf(`SELECT __key, this from "%s"`, mapName))
		if err != nil {
			t.Fatal(err)
		}
		var vs []interface{}
		var k int64
		var v interface{}
		for rows.Next() {
			if err := rows.Scan(&k, &v); err != nil {
				t.Fatal(err)
			}
			vs = append(vs, v)
		}
		targetThis := []interface{}{&Record{
			VarcharValue:  "hello",
			TinyIntValue:  -128,
			SmallIntValue: 32767,
			IntegerValue:  -27,
			BigIntValue:   38,
			BoolValue:     true,
			RealValue:     -5.32,
			DoubleValue:   12.789,
		}}
		assert.Equal(t, targetThis, vs)
		// select individual fields
		rows, err = db.Query(fmt.Sprintf(`
			SELECT __key, varcharvalue, tinyintvalue, smallintvalue, integervalue,	
			bigintvalue, boolvalue, realvalue, doublevalue from "%s"`, mapName))
		if err != nil {
			t.Fatal(err)
		}
		vs = nil
		var vVarchar string
		var vTinyInt int8
		var vSmallInt int16
		var vInteger int32
		var vBigInt int64
		var vBool bool
		var vReal float32
		var vDouble float64
		for rows.Next() {
			if err := rows.Scan(&k, &vVarchar, &vTinyInt, &vSmallInt, &vInteger, &vBigInt, &vBool, &vReal, &vDouble); err != nil {
				t.Fatal(err)
			}
			vs = append(vs, vVarchar, vTinyInt, vSmallInt, vInteger, vBigInt, vBool, vReal, vDouble)
		}
		target := []interface{}{
			"hello", int8(-128), int16(32767), int32(-27), int64(38), true, float32(-5.32), 12.789,
		}
		assert.Equal(t, target, vs)
	})
}

func TestSQLQueryWithCursorBufferSize(t *testing.T) {
	fn := func(i int) interface{} { return int32(i) }
	ctx := driver.WithCursorBufferSize(context.Background(), 10)
	testSQLQuery(t, ctx, "int", "int", fn, fn)
}

func TestSQLQueryWithQueryTimeout(t *testing.T) {
	fn := func(i int) interface{} { return int32(i) }
	ctx := driver.WithQueryTimeout(context.Background(), 5*time.Second)
	testSQLQuery(t, ctx, "int", "int", fn, fn)
}

func TestSetLoggerConfig(t *testing.T) {
	if err := driver.SetLoggerConfig(&logger.Config{Level: logger.ErrorLevel}); err != nil {
		t.Fatal(err)
	}
	if err := driver.SetLoggerConfig(nil); err != nil {
		t.Fatal(err)
	}
}

func TestSetSSLConfig(t *testing.T) {
	if err := driver.SetSSLConfig(&cluster.SSLConfig{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := driver.SetSSLConfig(nil); err != nil {
		t.Fatal(err)
	}
}

func testSQLQuery(t *testing.T, ctx context.Context, keyFmt, valueFmt string, keyFn, valueFn func(i int) interface{}) {
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		const rowCount = 50
		target, err := populateMap(m, rowCount, keyFn, valueFn)
		if err != nil {
			t.Fatal(err)
		}
		dsn := makeDSN(config)
		db, err := sql.Open("hazelcast", dsn)
		if err != nil {
			t.Fatal(err)
		}
		ms := createMappingStr(mapName, keyFmt, valueFmt)
		if err := createMapping(t, db, ms); err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		query := fmt.Sprintf(`SELECT __key, this FROM "%s" ORDER BY __key`, mapName)
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		entries := make([]types.Entry, len(target))
		var i int
		for rows.Next() {
			if err := rows.Scan(&entries[i].Key, &entries[i].Value); err != nil {
				log.Fatal(err)
			}
			i++
		}
		assert.Equal(t, target, entries)
	})
}

func createMapping(t *testing.T, db *sql.DB, mapping string) error {
	t.Logf("mapping: %s", mapping)
	_, err := db.Exec(mapping)
	return err
}

func createMappingStr(mapName, keyFmt, valueFmt string) string {
	return fmt.Sprintf(`
        CREATE MAPPING "%s"
        TYPE IMAP 
        OPTIONS (
            'keyFormat' = '%s',
            'valueFormat' = '%s'
        )
`, mapName, keyFmt, valueFmt)
}

func populateMap(m *hz.Map, count int, keyFn, valueFn func(i int) interface{}) ([]types.Entry, error) {
	entries := make([]types.Entry, count)
	for i := 0; i < count; i++ {
		entries[i] = types.Entry{
			Key:   keyFn(i),
			Value: valueFn(i),
		}
	}
	if err := m.PutAll(context.Background(), entries...); err != nil {
		return nil, err
	}
	return entries, nil
}

func makeDSN(config *hz.Config) string {
	ll := logger.InfoLevel
	if it.TraceLoggingEnabled() {
		ll = logger.TraceLevel
	}
	q := url.Values{}
	q.Add("cluster.name", config.Cluster.Name)
	q.Add("unisocket", strconv.FormatBool(config.Cluster.Unisocket))
	q.Add("log", string(ll))
	return fmt.Sprintf("hz://%s?%s", config.Cluster.Network.Addresses[0], q.Encode())
}
