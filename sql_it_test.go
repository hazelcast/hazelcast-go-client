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

package hazelcast_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/internal/it/skip"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/sql"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	factoryID                  = 100
	recordClassID              = 1
	recordWithDateTimeClassID  = 2
	recordWithDateTimeClassID2 = 3
)

type Record struct {
	DecimalValue  *types.Decimal
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
	dv := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
	tv := time.Date(0, 1, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local)
	tsv := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local)
	return &RecordWithDateTime{
		DateValue:                  &dv,
		TimeValue:                  &tv,
		TimestampValue:             &tsv,
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
	wr.WriteDate("datevalue", (*types.LocalDate)(r.DateValue))
	wr.WriteTime("timevalue", (*types.LocalTime)(r.TimeValue))
	wr.WriteTimestamp("timestampvalue", (*types.LocalDateTime)(r.TimestampValue))
	wr.WriteTimestampWithTimezone("timestampwithtimezonevalue", (*types.OffsetDateTime)(r.TimestampWithTimezoneValue))

}

func (r *RecordWithDateTime) ReadPortable(rd serialization.PortableReader) {
	r.DateValue = (*time.Time)(rd.ReadDate("datevalue"))
	r.TimeValue = (*time.Time)(rd.ReadTime("timevalue"))
	r.TimestampValue = (*time.Time)(rd.ReadTimestamp("timestampvalue"))
	r.TimestampWithTimezoneValue = (*time.Time)(rd.ReadTimestampWithTimezone("timestampwithtimezonevalue"))
}

type RecordWithDateTime2 struct {
	DateValue                  *types.LocalDate
	TimeValue                  *types.LocalTime
	TimestampValue             *types.LocalDateTime
	TimestampWithTimezoneValue *types.OffsetDateTime
}

func NewRecordWithDateTime2(t *time.Time) *RecordWithDateTime2 {
	dv := types.LocalDate(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local))
	tv := types.LocalTime(time.Date(0, 1, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local))
	tsv := types.LocalDateTime(time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local))
	tt := types.OffsetDateTime(*t)
	return &RecordWithDateTime2{
		DateValue:                  &dv,
		TimeValue:                  &tv,
		TimestampValue:             &tsv,
		TimestampWithTimezoneValue: &tt,
	}
}

func (r RecordWithDateTime2) FactoryID() int32 {
	return factoryID
}

func (r RecordWithDateTime2) ClassID() int32 {
	return recordWithDateTimeClassID2
}

func (r RecordWithDateTime2) WritePortable(wr serialization.PortableWriter) {
	wr.WriteDate("datevalue", r.DateValue)
	wr.WriteTime("timevalue", r.TimeValue)
	wr.WriteTimestamp("timestampvalue", r.TimestampValue)
	wr.WriteTimestampWithTimezone("timestampwithtimezonevalue", r.TimestampWithTimezoneValue)

}

func (r *RecordWithDateTime2) ReadPortable(rd serialization.PortableReader) {
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
	case recordWithDateTimeClassID2:
		return &RecordWithDateTime2{}
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
			valueFn: func(i int) interface{} { return types.LocalDate(time.Date(2021, 12, 21, 0, 0, 0, 0, time.Local)) },
		},
		{
			keyFmt: "int", valueFmt: "time",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return types.LocalTime(time.Date(0, 1, 1, 14, 15, 16, 200, time.Local)) },
		},
		{
			keyFmt: "int", valueFmt: "timestamp",
			keyFn: func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} {
				return types.LocalDateTime(time.Date(2021, 12, 23, 14, 15, 16, 200, time.Local))
			},
		},
		{
			keyFmt: "int", valueFmt: "timestamp with time zone",
			keyFn: func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} {
				return types.OffsetDateTime(time.Date(2021, 12, 23, 14, 15, 16, 200, time.FixedZone("", -6000)))
			},
		},
	}
	for _, tc := range testCases {
		name := fmt.Sprintf("%s/%s", tc.keyFmt, tc.valueFmt)
		t.Run(name, func(t *testing.T) {
			testSQLQuery(t, tc.keyFmt, tc.valueFmt, tc.keyFn, tc.valueFn, sql.NewStatement(""))
		})
	}
}

func TestSQLWithPortableData(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	cb := func(c *hz.Config) {
		c.Serialization.SetPortableFactories(&recordFactory{})
	}
	it.SQLTesterWithConfigBuilder(t, cb, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		it.MustValue(client.SQL().Execute(context.Background(), fmt.Sprintf(`
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
		_, err := client.SQL().Execute(context.Background(), fmt.Sprintf(`INSERT INTO "%s" (__key, varcharvalue, tinyintvalue, smallintvalue, integervalue, bigintvalue, boolvalue, realvalue, doublevalue, decimalvalue) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, mapName),
			1, rec.VarcharValue, rec.TinyIntValue, rec.SmallIntValue, rec.IntegerValue, rec.BigIntValue, rec.BoolValue, rec.RealValue, rec.DoubleValue, *rec.DecimalValue)
		if err != nil {
			t.Fatal(err)
		}
		// select the value itself
		row, err := queryRow(client, fmt.Sprintf(`SELECT __key, this from "%s"`, mapName))
		if err != nil {
			t.Fatal(err)
		}
		var vs []interface{}
		var k int64
		var v interface{}
		if err := assignValues(row, &k, &v); err != nil {
			t.Fatal(err)
		}
		vs = append(vs, v)
		targetThis := []interface{}{rec}
		assert.Equal(t, targetThis, vs)
		// select individual fields
		row, err = queryRow(client, fmt.Sprintf(`
			SELECT __key, varcharvalue, tinyintvalue, smallintvalue, integervalue,
			bigintvalue, boolvalue, realvalue, doublevalue, decimalvalue from "%s"`, mapName))
		if err != nil {
			log.Fatal(err)
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
		var vDecimal types.Decimal
		if err := assignValues(row, &k, &vVarchar, &vTinyInt, &vSmallInt, &vInteger, &vBigInt, &vBool, &vReal, &vDouble, &vDecimal); err != nil {
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
				'valuePortableClassId' = '3'
			)
		`, mapName)
		t.Logf("Query: %s", q)
		it.MustValue(client.SQL().Execute(context.Background(), q))
		dt := time.Date(2021, 12, 22, 23, 40, 12, 3400, time.FixedZone("A/B", -5*60*60))
		rec := NewRecordWithDateTime2(&dt)
		_, err := client.SQL().Execute(context.Background(), fmt.Sprintf(`INSERT INTO "%s" (__key, datevalue, timevalue, timestampvalue, timestampwithtimezonevalue) VALUES(?, ?, ?, ?, ?)`, mapName),
			1, *rec.DateValue, *rec.TimeValue, *rec.TimestampValue, *rec.TimestampWithTimezoneValue)
		if err != nil {
			t.Fatal(err)
		}
		targetDate := types.LocalDate(time.Date(2021, 12, 22, 0, 0, 0, 0, time.Local))
		targetTime := types.LocalTime(time.Date(0, 1, 1, 23, 40, 12, 3400, time.Local))
		targetTimestamp := types.LocalDateTime(time.Date(2021, 12, 22, 23, 40, 12, 3400, time.Local))
		targetTimestampWithTimezone := types.OffsetDateTime(time.Date(2021, 12, 22, 23, 40, 12, 3400, time.FixedZone("", -5*60*60)))
		var k int64
		// select the value itself
		row, err := queryRow(client, fmt.Sprintf(`SELECT __key, this from "%s"`, mapName))
		if err != nil {
			t.Fatal(err)
		}
		var v interface{}
		var vs []interface{}
		if err := assignValues(row, &k, &v); err != nil {
			t.Fatal(err)
		}
		vs = append(vs, v)
		targetThis := []interface{}{&RecordWithDateTime2{
			DateValue:                  &targetDate,
			TimeValue:                  &targetTime,
			TimestampValue:             &targetTimestamp,
			TimestampWithTimezoneValue: &targetTimestampWithTimezone,
		}}
		assert.Equal(t, targetThis, vs)
		// select individual fields
		row, err = queryRow(client, fmt.Sprintf(`
						SELECT
							__key, datevalue, timevalue, timestampvalue, timestampwithtimezonevalue
						FROM "%s" LIMIT 1
				`, mapName))
		if err != nil {
			t.Fatal(err)
		}
		var vDate types.LocalDate
		var vTime types.LocalTime
		var vTimestamp types.LocalDateTime
		var vTimestampWithTimezone types.OffsetDateTime
		if err := assignValues(row, &k, &vDate, &vTime, &vTimestamp, &vTimestampWithTimezone); err != nil {
			t.Fatal(err)
		}
		if !(time.Time)(targetDate).Equal((time.Time)(vDate)) {
			t.Fatalf("%v != %v", targetDate, vDate)
		}
		if !(time.Time)(targetTime).Equal((time.Time)(vTime)) {
			t.Fatalf("%v != %v", targetTime, vTime)
		}
		if !(time.Time)(targetTimestamp).Equal((time.Time)(vTimestamp)) {
			t.Fatalf("%v != %v", targetTimestamp, vTimestamp)
		}
		if !(time.Time)(targetTimestampWithTimezone).Equal((time.Time)(vTimestampWithTimezone)) {
			t.Fatalf("%v != %v", targetTimestampWithTimezone, vTimestamp)
		}
	})
}

func TestSQLQueryWithCursorBufferSize(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	fn := func(i int) interface{} { return int32(i) }
	stmt := sql.NewStatement("")
	it.Must(stmt.SetCursorBufferSize(10))
	testSQLQuery(t, "int", "int", fn, fn, stmt)
}

func TestSQLStatementWithQueryTimeout(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	stmt := sql.NewStatement("select v from table(generate_stream(1))")
	stmt.SetQueryTimeout(3 * time.Second)
	it.Must(stmt.SetCursorBufferSize(2))
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, _ *hz.Map, _ string) {
		sqlService := client.SQL()
		result := it.MustValue(sqlService.ExecuteStatement(context.Background(), stmt)).(sql.Result)
		defer result.Close()
		iter := it.MustValue(result.Iterator()).(sql.RowsIterator)
		for iter.HasNext() {
			_, err := iter.Next()
			if err != nil {
				var sqlError *sql.Error
				assert.True(t, errors.As(err, &sqlError))
				break
			}
		}
	})
}

func TestConcurrentQueries(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		q := fmt.Sprintf(`
			CREATE MAPPING "%s"
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'varchar'
			)
		`, mapName)
		t.Logf("Query: %s", q)
		it.MustValue(client.SQL().Execute(context.Background(), q))
		it.Must(m.Set(context.TODO(), 1, "foo"))
		const cnt = 1000
		wg := &sync.WaitGroup{}
		wg.Add(cnt)
		for i := 0; i < cnt; i++ {
			go func(wg *sync.WaitGroup) {
				var k int64
				var v string
				res, err := queryRow(client, fmt.Sprintf(`SELECT __key, this from "%s" LIMIT 1`, mapName))
				if err != nil {
					panic(err)
				}
				if err := assignValues(res, &k, &v); err != nil {
					panic(err)
				}
				wg.Done()
			}(wg)
		}
		wg.Wait()
	})
}

func TestSQLService_Execute(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		ctx := context.Background()
		q := fmt.Sprintf(`
			CREATE MAPPING "%s" (
				__key BIGINT,
				name VARCHAR
			)
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'json-flat'
			)
		`, mapName)
		sqlService := client.SQL()
		result := it.MustValue(sqlService.Execute(ctx, q)).(sql.Result)
		assert.Nil(t, result.Close())
		q = fmt.Sprintf(`INSERT INTO "%s" (__key, name) VALUES(?, ?)`, mapName)
		result = it.MustValue(sqlService.Execute(ctx, q, 100, "Ford Prefect")).(sql.Result)
		assert.Nil(t, result.Close())
		// select the value itself
		q = fmt.Sprintf(`SELECT __key, this from "%s"`, mapName)
		result = it.MustValue(sqlService.Execute(ctx, q)).(sql.Result)
		iter := it.MustValue(result.Iterator()).(sql.RowsIterator)
		assert.True(t, iter.HasNext())
		row := it.MustValue(iter.Next()).(sql.Row)
		var k int64
		var v interface{}
		if err := assignValues(row, &k, &v); err != nil {
			t.Fatal(err)
		}
		target := serialization.JSON(`{"name":"Ford Prefect"}`)
		assert.Equal(t, target, v)
	})
}

func TestSQLService_ExecuteMismatchedParams(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		ctx := context.Background()
		stmt := sql.NewStatement(fmt.Sprintf(`
			CREATE MAPPING "%s"
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'varchar'
			)
		`, mapName))

		_ = it.MustValue(client.SQL().ExecuteStatement(ctx, stmt))
		// with less args
		_, err := client.SQL().Execute(ctx, fmt.Sprintf(`SELECT * FROM "%s" WHERE __key > ? AND this > ?`, mapName), 5)
		var sqlError *sql.Error
		assert.True(t, errors.As(err, &sqlError))
		// with more args
		_, err = client.SQL().Execute(ctx, fmt.Sprintf(`SELECT * FROM "%s" WHERE __key > ? AND this > ?`, mapName), 5, "abc", 5)
		sqlError = nil
		assert.True(t, errors.As(err, &sqlError))
	})
}

func TestSQLService_ExecuteStatementMismatchedParams(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		ctx := context.Background()
		stmt := sql.NewStatement(fmt.Sprintf(`
			CREATE MAPPING "%s"
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'varchar'
			)
		`, mapName))

		_ = it.MustValue(client.SQL().ExecuteStatement(ctx, stmt))
		// with less args
		_, err := client.SQL().ExecuteStatement(ctx, sql.NewStatement(fmt.Sprintf(`SELECT * FROM "%s" WHERE __key > ? AND this > ?`, mapName), 5))
		var sqlError *sql.Error
		assert.True(t, errors.As(err, &sqlError))
		// with more args
		_, err = client.SQL().ExecuteStatement(ctx, sql.NewStatement(fmt.Sprintf(`SELECT * FROM "%s" WHERE __key > ? AND this > ?`, mapName), 5, "abc", 5))
		sqlError = nil
		assert.True(t, errors.As(err, &sqlError))
	})
}

func TestSQLService_ExecuteProvidedSuggestion(t *testing.T) {
	// todo this is a flaky test possibly due to member behaviour, will be refactored.
	it.MarkFlaky(t)
	skip.If(t, "hz < 5.0")
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		ctx := context.Background()
		// for create-mapping suggestion, map must have a value
		_ = it.MustValue(m.Put(ctx, "some-key", 5))
		selectAllQuery := fmt.Sprintf(`SELECT * FROM "%s"`, mapName)
		_, err := client.SQL().Execute(ctx, selectAllQuery)
		var sqlError *sql.Error
		assert.True(t, errors.As(err, &sqlError))
		// with more args
		_ = it.MustValue(client.SQL().Execute(ctx, sqlError.Suggestion))
		resp := it.MustValue(client.SQL().Execute(ctx, selectAllQuery)).(sql.Result)
		assert.True(t, resp.IsRowSet())
		iter := it.MustValue(resp.Iterator()).(sql.RowsIterator)
		var values []interface{}
		for iter.HasNext() {
			row := it.MustValue(iter.Next()).(sql.Row)
			values = append(values, it.MustValue(row.Get(0)))
			values = append(values, it.MustValue(row.Get(1)))
		}
		assert.Equal(t, []interface{}{"some-key", int64(5)}, values)
	})
}

func TestSQLService_ExecuteMismatchExpectedResultType(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		ctx := context.Background()
		sqlRead := fmt.Sprintf(`SELECT * FROM "%s"`, mapName)
		sqlDelete := fmt.Sprintf(`DELETE FROM "%s" WHERE __key = 1`, mapName)
		it.Must(m.Set(ctx, int32(1), int32(1)))
		it.Must(createMapping(t, client, createMappingStr(mapName, "int", "int")))
		// Row results
		testCases := []struct {
			expectedResultType sql.ExpectedResultType
			expectErr          error
			count              int
			sql                string
		}{
			// Row result cases
			{
				expectedResultType: sql.ExpectedResultTypeAny,
				count:              1,
				sql:                sqlRead,
			},
			{
				expectedResultType: sql.ExpectedResultTypeRows,
				count:              1,
				sql:                sqlRead,
			},
			{
				expectedResultType: sql.ExpectedResultTypeUpdateCount,
				expectErr:          hzerrors.ErrSQL,
				sql:                sqlRead,
			},
			// Update count result cases
			{
				expectedResultType: sql.ExpectedResultTypeAny,
				count:              0,
				sql:                sqlDelete,
			},
			{
				expectedResultType: sql.ExpectedResultTypeUpdateCount,
				count:              0,
				sql:                sqlDelete,
			},
			{
				expectedResultType: sql.ExpectedResultTypeRows,
				expectErr:          hzerrors.ErrSQL,
				sql:                sqlDelete,
			},
		}
		for _, tc := range testCases {
			stmt := sql.NewStatement(tc.sql)
			it.Must(stmt.SetExpectedResultType(tc.expectedResultType))
			result, err := client.SQL().ExecuteStatement(ctx, stmt)
			if tc.expectErr != nil {
				assert.True(t, errors.Is(err, tc.expectErr))
				continue
			}
			assert.Nil(t, err)
			if result.IsRowSet() {
				iter := it.MustValue(result.Iterator()).(sql.RowsIterator)
				var rows []sql.Row
				for iter.HasNext() {
					row := it.MustValue(iter.Next()).(sql.Row)
					rows = append(rows, row)
				}
				assert.Equal(t, 1, len(rows))
				continue
			}
			assert.Equal(t, int64(tc.count), result.UpdateCount())
			assert.False(t, result.IsRowSet())
		}
	})
}

func TestSQLResult_IteratorRequestedMoreThanOnce(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		q := fmt.Sprintf(`
			CREATE MAPPING "%s"
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'varchar'
			)
		`, mapName)
		ctx := context.Background()
		it.MustValue(client.SQL().Execute(ctx, q))
		result := it.MustValue(client.SQL().Execute(ctx, fmt.Sprintf(`select * from "%s"`, mapName))).(sql.Result)
		assert.True(t, result.IsRowSet())
		_ = it.MustValue(result.Iterator())
		_, err := result.Iterator()
		if err == nil {
			t.Fatal("error must be present on second iterator request")
		}
	})
}

func TestSQLResult_ForRowAndNonRowResults(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		q := fmt.Sprintf(`
			CREATE MAPPING "%s"
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'varchar'
			)
		`, mapName)
		ctx := context.Background()
		// Non-row result
		result := it.MustValue(client.SQL().Execute(ctx, q)).(sql.Result)
		assert.Equal(t, int64(0), result.UpdateCount())
		assert.False(t, result.IsRowSet())
		_, err := result.Iterator()
		assert.True(t, errors.Is(err, hzerrors.ErrIllegalState))
		_, err = result.RowMetadata()
		assert.True(t, errors.Is(err, hzerrors.ErrIllegalState))
		assert.Nil(t, result.Close())
		// Row result
		result = it.MustValue(client.SQL().Execute(ctx, fmt.Sprintf(`select * from "%s"`, mapName))).(sql.Result)
		assert.Equal(t, int64(-1), result.UpdateCount())
		assert.True(t, result.IsRowSet())
		// assert metadata
		md := it.MustValue(result.RowMetadata()).(sql.RowMetadata)
		assert.Equal(t, 2, md.ColumnCount())
		// col1
		col := it.MustValue(md.GetColumn(0)).(sql.ColumnMetadata)
		assert.Equal(t, 0, it.MustValue(md.FindColumn("__key")))
		assert.Equal(t, sql.ColumnTypeBigInt, col.Type())
		assert.Equal(t, true, col.Nullable())
		// col2
		col = it.MustValue(md.GetColumn(1)).(sql.ColumnMetadata)
		assert.Equal(t, 1, it.MustValue(md.FindColumn("this")))
		assert.Equal(t, sql.ColumnTypeVarchar, col.Type())
		assert.Equal(t, true, col.Nullable())
		// wrong column name
		if _, err = md.FindColumn("wrongColumn"); err == nil {
			t.Fatal("error must be returned for non existing column")
		}
		if _, err = md.GetColumn(2); err == nil {
			t.Fatal("error must be returned for column index out of range")
		}
		iter := it.MustValue(result.Iterator()).(sql.RowsIterator)
		assert.False(t, iter.HasNext())
	})
}

func TestSQLRow_FindByColumnName(t *testing.T) {
	it.SkipIf(t, "hz < 5.0")
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		q := fmt.Sprintf(`
			CREATE MAPPING "%s"
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'varchar'
			)
		`, mapName)
		ctx := context.Background()
		// Non-row result
		result := it.MustValue(client.SQL().Execute(ctx, q)).(sql.Result)
		it.Must(result.Close())
		_ = it.MustValue(m.Put(ctx, 5, "value"))
		result = it.MustValue(client.SQL().Execute(ctx, fmt.Sprintf(`select * from "%s"`, mapName))).(sql.Result)
		defer it.Must(result.Close())
		iter := it.MustValue(result.Iterator()).(sql.RowsIterator)
		assert.True(t, iter.HasNext())
		row := it.MustValue(iter.Next()).(sql.Row)
		assert.EqualValues(t, 5, it.MustValue(row.GetByColumnName("__key")))
		assert.False(t, iter.HasNext())
		_, err := row.GetByColumnName("non-existing-column")
		assert.True(t, errors.Is(err, hzerrors.ErrIllegalArgument))
	})
}

func testSQLQuery(t *testing.T, keyFmt, valueFmt string, keyFn, valueFn func(i int) interface{}, stmt sql.Statement) {
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		stmt := stmt
		const rowCount = 50
		target, err := populateMap(m, rowCount, keyFn, valueFn)
		if err != nil {
			t.Fatal(err)
		}
		ms := createMappingStr(mapName, keyFmt, valueFmt)
		if err := createMapping(t, client, ms); err != nil {
			t.Fatal(err)
		}
		query := fmt.Sprintf(`SELECT __key, this FROM "%s" ORDER BY __key`, mapName)
		var result sql.Result
		sqlService := client.SQL()
		if stmt.SQL() == "" {
			it.Must(stmt.SetSQL(query))
		}
		result, err = sqlService.ExecuteStatement(context.Background(), stmt)
		if err != nil {
			log.Fatal(err)
		}
		defer result.Close()
		assert.EqualValues(t, -1, result.UpdateCount())
		assert.True(t, result.IsRowSet())
		iter, err := result.Iterator()
		if err != nil {
			log.Fatal(err)
		}
		entries := make([]types.Entry, len(target))
		var i int
		for iter.HasNext() {
			row, err := iter.Next()
			if err != nil {
				log.Fatal(err)
			}
			if entries[i].Key, err = row.Get(0); err != nil {
				log.Fatal(err)
			}
			if entries[i].Value, err = row.Get(1); err != nil {
				log.Fatal(err)
			}
			i++
		}
		if valueFmt == "decimal" {
			// assert.Equal does not seem to work for *big.Int values.
			// Since types.Decimal includes a *big.Int, it doesn't work for it either.
			// Hence, the special treatment for decimal.
			assert.Equal(t, len(target), len(entries))
			for i := 0; i < len(target); i++ {
				t.Run(fmt.Sprintf("decimal-%d", i), func(t *testing.T) {
					assert.Equal(t, target[i].Key, entries[i].Key)
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

func createMapping(t *testing.T, client *hz.Client, mapping string) error {
	t.Logf("mapping: %s", mapping)
	_, err := client.SQL().Execute(context.Background(), mapping)
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

func queryRow(client *hz.Client, q string, params ...interface{}) (sql.Row, error) {
	result, err := client.SQL().Execute(context.Background(), q, params...)
	if err != nil {
		return nil, err
	}
	iter, err := result.Iterator()
	if err != nil {
		return nil, err
	}
	for iter.HasNext() {
		return iter.Next()
	}
	return nil, nil
}

func assignValues(row sql.Row, targets ...interface{}) error {
	var err error
	for i := 0; i < row.Metadata().ColumnCount(); i++ {
		var tmp interface{}
		tmp, err = row.Get(i)
		if err != nil {
			return err
		}
		value := reflect.ValueOf(targets[i])
		iValue := value.Elem()
		x := reflect.ValueOf(tmp)
		iValue.Set(x)
	}
	return nil
}
