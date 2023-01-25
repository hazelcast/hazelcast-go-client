package compatibility

import (
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
	pactypes "go/types"
	"math"
	"math/big"
	"time"
)

const (
	IdentifiedDataSerializableFactoryId = 1
	PortableFactoryId                   = 1
	PortableClassId                     = 1
	InnerPortableClassId                = 2
	IdentifiedDataSerializableClassId   = 1
	CustomStreamSerializableId          = 1
	CustomByteArraySerializableId       = 2
)

var (
	allTestObjects map[string]interface{} = nil
)

func writeRawBytes(o serialization.DataOutput, a []byte) {
	for _, i := range a {
		o.WriteByte(i)
	}
}

func readRawBytes(i serialization.DataInput, size int) []byte {
	a := make([]byte, size)
	for idx, _ := range a {
		a[idx] = i.ReadByte()
	}
	return a
}

var (
	aNullObject  pactypes.Object = nil
	aBoolean     bool            = true
	aByte        byte            = 113
	aChar        uint16          = 'x'
	aDouble      float64         = -897543.3678909
	aShort       int16           = -500
	aFloat       float32         = 900.5678
	anInt        int32           = 56789
	aLong        int64           = -50992225
	anSqlString  string          = "this > 5 AND this < 100"
	aString      string          = anSqlString
	aUUID        types.UUID      = types.NewUUIDWith(uint64(aLong), uint64(anInt))
	aSmallString string          = "😊 Hello Приве́т नमस्ते שָׁלוֹם"

	booleans = []bool{true, false, true}
	// byte is signed in Java but unsigned in Go!
	bytes   = []byte{112, 4, 1, 4, 112, 35, 43}
	chars   = []uint16{'a', 'b', 'c'}
	doubles = []float64{-897543.3678909, 11.1, 22.2, 33.3}
	shorts  = []int16{-500, 2, 3}
	floats  = []float32{900.5678, 1.0, 2.1, 3.4}
	ints    = []int32{56789, 2, 3}
	longs   = []int64{-50992225, 1231232141, 2, 3}

	strings = []string{
		"Pijamalı hasta, yağız şoföre çabucak güvendi.",
		"イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム",
		"The quick brown fox jumps over the lazy dog",
	}
	aData iserialization.Data = []byte("111313123131313131")

	anInnerPortable           *AnInnerPortable         = &AnInnerPortable{anInt: anInt, aFloat: aFloat}
	aCustomStreamSerializable CustomStreamSerializable = CustomStreamSerializable{I: anInt, F: aFloat}

	aCustomByteArraySerializable CustomByteArraySerializable = CustomByteArraySerializable{I: anInt, F: aFloat}
	portables                                                = []serialization.Portable{anInnerPortable, anInnerPortable, anInnerPortable}

	anIdentifiedDataSerializable AnIdentifiedDataSerializable = AnIdentifiedDataSerializable{bool: aBoolean, b: aByte,
		c: aChar, d: aDouble, s: aShort, f: aFloat, i: anInt, l: aLong, str: aSmallString, booleans: booleans,
		bytes: bytes, chars: chars, doubles: doubles, shorts: shorts, floats: floats, ints: ints, longs: longs, strings: strings,
		byteSize: byte(len(bytes)), bytesFully: bytes, bytesOffset: []byte{bytes[1], bytes[2]}, strBytes: nil, strChars: nil,
		unsignedByte: math.MaxUint8, unsignedShort: math.MaxUint16, portableObject: anInnerPortable, identifiedDataSerializableObject: nil, customStreamSerializableObject: aCustomStreamSerializable,
		customByteArraySerializableObject: aCustomByteArraySerializable, data: aData}

	aDate = time.Date(1990, 2, 1, 0, 0, 0, 0, time.UTC)

	aLocalDate       = (types.LocalDate)(time.Date(2021, 6, 28, 0, 0, 0, 0, time.Local))
	aLocalTime       = (types.LocalTime)(time.Date(0, 1, 1, 11, 22, 41, 123456789, time.Local))
	aLocalDateTime   = (types.LocalDateTime)(time.Date(2021, 6, 28, 11, 22, 41, 123456789, time.Local))
	anOffsetDateTime = (types.OffsetDateTime)(time.Date(2021, 6, 28, 11, 22, 41, 123456789, time.FixedZone("", 18*60*60)))

	localDates = []types.LocalDate{
		(types.LocalDate)(time.Date(2021, 6, 28, 0, 0, 0, 0, time.Local)),
		(types.LocalDate)(time.Date(1923, 4, 23, 0, 0, 0, 0, time.Local)),
		(types.LocalDate)(time.Date(1938, 11, 10, 0, 0, 0, 0, time.Local)),
	}
	localTimes = []types.LocalTime{
		(types.LocalTime)(time.Date(0, 0, 0, 9, 5, 10, 123456789, time.Local)),
		(types.LocalTime)(time.Date(0, 0, 0, 18, 30, 55, 567891234, time.Local)),
		(types.LocalTime)(time.Date(0, 0, 0, 15, 44, 39, 192837465, time.Local)),
	}
	localDataTimes = []types.LocalDateTime{
		(types.LocalDateTime)(time.Date(1938, 11, 10, 9, 5, 10, 123456789, time.Local)),
		(types.LocalDateTime)(time.Date(1923, 4, 23, 15, 44, 39, 192837465, time.Local)),
		(types.LocalDateTime)(time.Date(2021, 6, 28, 18, 30, 55, 567891234, time.Local)),
	}
	offsetDataTimes = []types.OffsetDateTime{
		(types.OffsetDateTime)(time.Date(1938, 11, 10, 9, 5, 10, 123456789, time.FixedZone("", 18))),
		(types.OffsetDateTime)(time.Date(1923, 4, 23, 15, 44, 39, 192837465, time.FixedZone("", 5))),
		(types.OffsetDateTime)(time.Date(2021, 6, 28, 18, 30, 55, 567891234, time.FixedZone("", -10))),
	}

	aBigInteger = big.NewInt(1314432323232411)

	aClass = "java.math.BigDecimal"

	aBigDecimal = types.NewDecimal(big.NewInt(31231), 0)
	decimals    = []types.Decimal{}
	aPortable   = APortable{boolean: aBoolean, b: aByte, c: aChar, d: aDouble, s: aShort, f: aFloat, i: anInt, l: aLong,
		str: anSqlString, bd: aBigDecimal, ld: aLocalDate, lt: aLocalTime, ldt: aLocalDateTime, odt: anOffsetDateTime, p: anInnerPortable,
		booleans: booleans, bytes: bytes, chars: chars, doubles: doubles, shorts: shorts, floats: floats, ints: ints, longs: longs, strings: strings,
		decimals: decimals, dates: localDates, times: localTimes, dateTimes: localDataTimes, offsetDateTimes: offsetDataTimes, portables: portables,
	}
	//anIdentifiedDataSerializable, aCustomStreamSerializable, aCustomByteArraySerializable,
	nonNilList = []interface{}{aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, aSmallString,
		anInnerPortable, booleans, bytes, chars, doubles, shorts, floats, ints, longs, strings, aCustomStreamSerializable,
		aCustomByteArraySerializable, anIdentifiedDataSerializable, aPortable, aDate, aLocalDate, aLocalTime, aLocalDateTime,
		anOffsetDateTime, aBigInteger, aBigDecimal, aClass}
)

func initializeVariables() {
	anIdentifiedDataSerializable.strChars = make([]uint16, len(aSmallString))
	for i, r := range aSmallString {
		anIdentifiedDataSerializable.strChars[i] = uint16(r)
	}
	anIdentifiedDataSerializable.strBytes = make([]byte, len(aSmallString))
	for i, r := range aSmallString {
		anIdentifiedDataSerializable.strBytes[i] = byte(r)
	}

	allTestObjects = map[string]interface{}{
		"AnIdentifiedDataSerializable": anIdentifiedDataSerializable,
		/*
			"NULL":            aNullObject,
			"Boolean":         aBoolean,
			"Byte":            aByte,
			"Character":       aChar,
			"Double":          aDouble,
			"Short":           aShort,
			"Float":           aFloat,
			"Integer":         anInt,
			"Long":            aLong,
			"String":          anSqlString,
			"UUID":            aUUID,
			"AnInnerPortable": anInnerPortable,

			"boolean[]":                    booleans,
			"byte[]":                       bytes,
			"char[]":                       chars,
			"double[]":                     doubles,
			"short[]":                      shorts,
			"float[]":                      floats,
			"int[]":                        ints,
			"long[]":                       longs,
			"String[]":                     strings,
			"CustomStreamSerializable":     aCustomStreamSerializable,
			"CustomByteArraySerializable":  aCustomByteArraySerializable,
		*/
		// "APortable":                    aPortable,
		/*"LocalDate":      aLocalDate,
		"LocalTime":      aLocalTime,
		"LocalDateTime":  aLocalDateTime,
		"OffsetDateTime": anOffsetDateTime,
		"BigInteger":     aBigInteger,
		"BigDecimal":     aBigDecimal,
		"Class":          aClass,

		"TruePredicate":        predicate.True(),
		"FalsePredicate":       predicate.False(),
		"SqlPredicate":         predicate.SQL(anSqlString),
		"EqualPredicate":       predicate.Equal(anSqlString, anInt),
		"NotEqualPredicate":    predicate.NotEqual(anSqlString, anInt),
		"GreaterLessPredicate": predicate.Greater(anSqlString, anInt),
		"BetweenPredicate":     predicate.Between(anSqlString, anInt, anInt),
		"LikePredicate":        predicate.Like(anSqlString, anSqlString),
		"ILikePredicate":       predicate.ILike(anSqlString, anSqlString),
		"InPredicate":          predicate.In(anSqlString, anInt, anInt),
		"RegexPredicate":       predicate.Regex(anSqlString, anSqlString),
		"AndPredicate": predicate.And(
			predicate.SQL(anSqlString),
			predicate.Equal(anSqlString, anInt),
			predicate.NotEqual(anSqlString, anInt),
			predicate.Greater(anSqlString, anInt),
			predicate.GreaterOrEqual(anSqlString, anInt),
		),
		"OrPredicate": predicate.Or(
			predicate.SQL(anSqlString),
			predicate.Equal(anSqlString, anInt),
			predicate.NotEqual(anSqlString, anInt),
			predicate.Greater(anSqlString, anInt),
			predicate.GreaterOrEqual(anSqlString, anInt),
		),
		"InstanceOfPredicate": predicate.InstanceOf("com.hazelcast.nio.serialization.compatibility.CustomStreamSerializable"),

		"CountAggregator": aggregate.Count(anSqlString),
		// "DistinctValuesAggregator": aggregate.DistinctValues(anSqlString),
		// "MaxByAggregator":          aggregate.Max(anSqlString),
		// "MinByAggregator":          aggregate.Min(anSqlString),
		"MaxAggregator":            aggregate.MaxAll(),
		"MinAggregator":            aggregate.MinAll(),
		"DoubleSumAggregator":      aggregate.DoubleSum(anSqlString),
		"IntegerSumAggregator":     aggregate.IntSum(anSqlString),
		"LongSumAggregator":        aggregate.LongSum(anSqlString),
		"DoubleAverageAggregator":  aggregate.DoubleAverage(anSqlString),
		"IntegerAverageAggregator": aggregate.IntAverage(anSqlString),
		"LongAverageAggregator":    aggregate.LongAverage(anSqlString),*/
	}

}
