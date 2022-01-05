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
	"math/big"
	"net/url"
	"strconv"
	"sync"
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
	factoryID                 = 100
	recordClassID             = 1
	recordWithDateTimeClassID = 2
)

type Record struct {
	DecimalValue  *types.Decimal
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

func (r Record) FactoryID() int32 {
	return factoryID
}

func (r Record) ClassID() int32 {
	return recordClassID
}

func (r Record) WritePortable(wr serialization.PortableWriter) {
	wr.WriteString("varcharvalue", r.VarcharValue)
	wr.WriteByte("tinyintvalue", byte(r.TinyIntValue))
	wr.WriteInt16("smallintvalue", r.SmallIntValue)
	wr.WriteInt32("integervalue", r.IntegerValue)
	wr.WriteInt64("bigintvalue", r.BigIntValue)
	wr.WriteBool("boolvalue", r.BoolValue)
	wr.WriteFloat32("realvalue", r.RealValue)
	wr.WriteFloat64("doublevalue", r.DoubleValue)
	wr.WriteDecimal("decimalvalue", r.DecimalValue)
}

func (r *Record) ReadPortable(rd serialization.PortableReader) {
	r.VarcharValue = rd.ReadString("varcharvalue")
	r.TinyIntValue = int8(rd.ReadByte("tinyintvalue"))
	r.SmallIntValue = rd.ReadInt16("smallintvalue")
	r.IntegerValue = rd.ReadInt32("integervalue")
	r.BigIntValue = rd.ReadInt64("bigintvalue")
	r.BoolValue = rd.ReadBool("boolvalue")
	r.RealValue = rd.ReadFloat32("realvalue")
	r.DoubleValue = rd.ReadFloat64("doublevalue")
	r.DecimalValue = rd.ReadDecimal("decimalvalue")
}

type RecordWithDateTime struct {
	DateValue                  *time.Time
	TimeValue                  *time.Time
	TimestampValue             *time.Time
	TimestampWithTimezoneValue *time.Time
}

func NewRecordWithDateTime(t *time.Time) *RecordWithDateTime {
	return &RecordWithDateTime{
		DateValue:                  t,
		TimeValue:                  t,
		TimestampValue:             t,
		TimestampWithTimezoneValue: t,
	}
}

func (r RecordWithDateTime) FactoryID() int32 {
	return factoryID
}

func (r RecordWithDateTime) ClassID() int32 {
	return recordWithDateTimeClassID
}

func (r RecordWithDateTime) WritePortable(wr serialization.PortableWriter) {
	wr.WriteDate("datevalue", r.DateValue)
	wr.WriteTime("timevalue", r.TimeValue)
	wr.WriteTimestamp("timestampvalue", r.TimestampValue)
	wr.WriteTimestampWithTimezone("timestampwithtimezonevalue", r.TimestampWithTimezoneValue)

}

func (r *RecordWithDateTime) ReadPortable(rd serialization.PortableReader) {
	r.DateValue = rd.ReadDate("datevalue")
	r.TimeValue = rd.ReadTime("timevalue")
	r.TimestampValue = rd.ReadTimestamp("timestampvalue")
	r.TimestampWithTimezoneValue = rd.ReadTimestampWithTimezone("timestampwithtimezonevalue")
}

type recordFactory struct{}

func (f recordFactory) Create(classID int32) serialization.Portable {
	switch classID {
	case recordClassID:
		return &Record{}
	case recordWithDateTimeClassID:
		return &RecordWithDateTime{}
	}
	panic(fmt.Sprintf("unknown class ID: %d", classID))
}

func (f recordFactory) FactoryID() int32 {
	return factoryID
}

func TestSQLQuery(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	testCases := []struct {
		keyFn, valueFn   func(i int) interface{}
		keyFmt, valueFmt string
	}{
		{
			keyFmt: "int", valueFmt: "varchar",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return fmt.Sprintf("val-%d", i*100) },
		},
		{
			keyFmt: "int", valueFmt: "tinyint",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return int8(i) },
		},
		{
			keyFmt: "int", valueFmt: "smallint",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return int16(i * 100) },
		},
		{
			keyFmt: "int", valueFmt: "integer",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return int32(i * 100) },
		},
		{
			keyFmt: "int", valueFmt: "bigint",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return int64(i * 100) },
		},
		{
			keyFmt: "int", valueFmt: "boolean",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return i%2 == 0 },
		},
		{
			keyFmt: "int", valueFmt: "real",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return float32(i) * 1.5 },
		},
		{
			keyFmt: "int", valueFmt: "double",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return float64(i) * 1.5 },
		},
		{
			keyFmt: "int", valueFmt: "decimal",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return types.NewDecimal(big.NewInt(int64(i)), 5) },
		},
		{
			keyFmt: "int", valueFmt: "date",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return time.Date(2021, 12, 21, 0, 0, 0, 0, time.Local) },
		},
		{
			keyFmt: "int", valueFmt: "time",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return time.Date(0, 1, 1, 14, 15, 16, 200, time.Local) },
		},
		{
			keyFmt: "int", valueFmt: "timestamp",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return time.Date(2021, 12, 23, 14, 15, 16, 200, time.Local) },
		},
		{
			keyFmt: "int", valueFmt: "timestamp with time zone",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return time.Date(2021, 12, 23, 14, 15, 16, 200, time.FixedZone("", -6000)) },
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
	it.SkipIf(t, "hz < 5.0")
	cb := func(c *hz.Config) {
		c.Serialization.SetPortableFactories(&recordFactory{})
	}
	it.SQLTesterWithConfigBuilder(t, cb, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		db := driver.Open(*config)
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
				doublevalue DOUBLE,
				decimalvalue DECIMAL
			)
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'portable',
				'valuePortableFactoryId' = '100',
				'valuePortableClassId' = '1'
			)
		`, mapName)))
		dec := types.NewDecimal(big.NewInt(123_456_789), 100)
		rec := &Record{
			VarcharValue:  "hello",
			TinyIntValue:  -128,
			SmallIntValue: 32767,
			IntegerValue:  -27,
			BigIntValue:   38,
			BoolValue:     true,
			RealValue:     -5.32,
			DoubleValue:   12.789,
			DecimalValue:  &dec,
		}
		it.Must(m.Set(context.Background(), 1, rec))
		// select the value itself
		row := db.QueryRow(fmt.Sprintf(`SELECT __key, this from "%s"`, mapName))
		var vs []interface{}
		var k int64
		var v interface{}
		if err := row.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		vs = append(vs, v)
		targetThis := []interface{}{&Record{
			VarcharValue:  "hello",
			TinyIntValue:  -128,
			SmallIntValue: 32767,
			IntegerValue:  -27,
			BigIntValue:   38,
			BoolValue:     true,
			RealValue:     -5.32,
			DoubleValue:   12.789,
			DecimalValue:  &dec,
		}}
		assert.Equal(t, targetThis, vs)
		// select individual fields
		row = db.QueryRow(fmt.Sprintf(`
			SELECT __key, varcharvalue, tinyintvalue, smallintvalue, integervalue,	
			bigintvalue, boolvalue, realvalue, doublevalue, decimalvalue from "%s"`, mapName))
		vs = nil
		var vVarchar string
		var vTinyInt int8
		var vSmallInt int16
		var vInteger int32
		var vBigInt int64
		var vBool bool
		var vReal float32
		var vDouble float64
		var vDecimal types.Decimal
		if err := row.Scan(&k, &vVarchar, &vTinyInt, &vSmallInt, &vInteger, &vBigInt, &vBool, &vReal, &vDouble, &vDecimal); err != nil {
			t.Fatal(err)
		}
		vs = append(vs, vVarchar, vTinyInt, vSmallInt, vInteger, vBigInt, vBool, vReal, vDouble, vDecimal)
		target := []interface{}{
			"hello", int8(-128), int16(32767), int32(-27), int64(38), true, float32(-5.32), 12.789, types.NewDecimal(big.NewInt(123_456_789), 100),
		}
		assert.Equal(t, target, vs)
	})
}

func TestSQLWithPortableDateTime(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	cb := func(c *hz.Config) {
		c.Serialization.SetPortableFactories(&recordFactory{})
	}
	it.SQLTesterWithConfigBuilder(t, cb, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		db := driver.Open(*config)
		defer db.Close()
		q := fmt.Sprintf(`
			CREATE MAPPING "%s" (
				__key BIGINT,
				datevalue DATE,
				timevalue TIME,
				timestampvalue TIMESTAMP,
				timestampwithtimezonevalue TIMESTAMP WITH TIME ZONE
			)
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'portable',
				'valuePortableFactoryId' = '100',
				'valuePortableClassId' = '2'
			)
		`, mapName)
		t.Logf("Query: %s", q)
		it.MustValue(db.Exec(q))
		dt := time.Date(2021, 12, 22, 23, 40, 12, 3400, time.FixedZone("A/B", -5*60*60))
		rec := NewRecordWithDateTime(&dt)
		it.Must(m.Set(context.TODO(), 1, rec))
		targetDate := time.Date(2021, 12, 22, 0, 0, 0, 0, time.Local)
		targetTime := time.Date(0, 1, 1, 23, 40, 12, 3400, time.Local)
		targetTimestamp := time.Date(2021, 12, 22, 23, 40, 12, 3400, time.Local)
		targetTimestampWithTimezone := time.Date(2021, 12, 22, 23, 40, 12, 3400, time.FixedZone("", -5*60*60))
		var k int64
		// select the value itself
		row := db.QueryRow(fmt.Sprintf(`SELECT __key, this from "%s"`, mapName))
		var v interface{}
		var vs []interface{}
		if err := row.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		vs = append(vs, v)
		targetThis := []interface{}{&RecordWithDateTime{
			DateValue:                  &targetDate,
			TimeValue:                  &targetTime,
			TimestampValue:             &targetTimestamp,
			TimestampWithTimezoneValue: &targetTimestampWithTimezone,
		}}
		assert.Equal(t, targetThis, vs)
		// select individual fields
		row = db.QueryRow(fmt.Sprintf(`
						SELECT
							__key, datevalue, timevalue, timestampvalue, timestampwithtimezonevalue
						FROM "%s" LIMIT 1
				`, mapName))
		var vDate, vTime, vTimestamp, vTimestampWithTimezone time.Time
		if err := row.Scan(&k, &vDate, &vTime, &vTimestamp, &vTimestampWithTimezone); err != nil {
			t.Fatal(err)
		}
		if !targetDate.Equal(vDate) {
			t.Fatalf("%s != %s", targetDate, vDate)
		}
		if !targetTime.Equal(vTime) {
			t.Fatalf("%s != %s", targetTime, vTime)
		}
		if !targetTimestamp.Equal(vTimestamp) {
			t.Fatalf("%s != %s", targetTimestamp, vTimestamp)
		}
		if !targetTimestampWithTimezone.Equal(vTimestampWithTimezone) {
			t.Fatalf("%s != %s", targetTimestampWithTimezone, vTimestamp)
		}
	})
}

func TestSQLQueryWithCursorBufferSize(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	fn := func(i int) interface{} { return int32(i) }
	ctx := driver.WithCursorBufferSize(context.Background(), 10)
	testSQLQuery(t, ctx, "int", "int", fn, fn)
}

func TestSQLQueryWithQueryTimeout(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
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

func TestConcurrentQueries(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		db := driver.Open(*config)
		defer db.Close()
		q := fmt.Sprintf(`
			CREATE MAPPING "%s"
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'varchar'
			)
		`, mapName)
		t.Logf("Query: %s", q)
		it.MustValue(db.Exec(q))
		it.Must(m.Set(context.TODO(), 1, "foo"))
		const cnt = 1000
		wg := &sync.WaitGroup{}
		wg.Add(cnt)
		for i := 0; i < cnt; i++ {
			go func(wg *sync.WaitGroup) {
				var k int64
				var v string
				res := db.QueryRow(fmt.Sprintf(`SELECT __key, this from "%s" LIMIT 1`, mapName))
				if err := res.Scan(&k, &v); err != nil {
					panic(err)
				}
				wg.Done()
			}(wg)
		}
		wg.Wait()
	})
}

func testSQLQuery(t *testing.T, ctx context.Context, keyFmt, valueFmt string, keyFn, valueFn func(i int) interface{}) {
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		const rowCount = 50
		target, err := populateMap(m, rowCount, keyFn, valueFn)
		if err != nil {
			t.Fatal(err)
		}
		dsn := makeDSN(config)
		sc := &serialization.Config{}
		sc.SetGlobalSerializer(&it.PanicingGlobalSerializer{})
		if err := driver.SetSerializationConfig(sc); err != nil {
			t.Fatal(err)
		}
		defer driver.SetSerializationConfig(nil)
		db, err := sql.Open("hazelcast", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		ms := createMappingStr(mapName, keyFmt, valueFmt)
		if err := createMapping(t, db, ms); err != nil {
			t.Fatal(err)
		}
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
		if valueFmt == "decimal" {
			// assert.Equal does not seem to work for *big.Int values.
			// Since types.Decimal includes a *big.Int, it doesn't work for it either.
			// Hence the special treatment for decimal.
			assert.Equal(t, len(target), len(entries))
			for i := 0; i < len(target); i++ {
				t.Run(fmt.Sprintf("decimal-%d", i), func(t *testing.T) {
					bt := target[i].Value.(types.Decimal)
					be := entries[i].Value.(types.Decimal)
					assert.Equal(t, bt.Scale(), be.Scale())
					assert.Equal(t, bt.UnscaledValue().String(), be.UnscaledValue().String())
				})
			}
		} else {
			assert.Equal(t, target, entries)
		}
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
