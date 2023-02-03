package compatibility

import (
	"encoding/binary"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	serialization2 "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"reflect"
	"regexp"
	"testing"
)

var (
	dataMap    = make(map[string]serialization.Data)
	byteOrders = []binary.ByteOrder{binary.BigEndian, binary.LittleEndian}
	versions   = []int{1}
)

func TestBinaryCompatibility(t *testing.T) {
	initializeVariables()
	readBinaryFile(t)
	for key, obj := range allTestObjects {
		for _, order := range byteOrders {
			for _, version := range versions {
				keyStr := createObjectKey(key, order, version)
				t.Run(keyStr+"-testDeserialize", func(t *testing.T) {
					// testDeserialize
					if skipOnDeserialize(key) {
						t.SkipNow()
					}
					service := createSerializationService(t, order)
					readObject, err := service.ToObject(dataMap[keyStr])
					require.NoError(t, err)
					require.Equal(t, obj, readObject)
				})
				t.Run(keyStr+"-testSerialize", func(t *testing.T) {
					// testSerialize
					if skipOnSerialize(key) {
						t.SkipNow()
					}
					service := createSerializationService(t, order)
					data, err := service.ToData(obj)
					require.NoError(t, err)
					require.Equal(t, dataMap[keyStr], data)
				})
				t.Run(keyStr+"-testSerializeDeserialize", func(t *testing.T) {
					// testSerializeDeserialize
					if skipOnDeserialize(key) || skipOnSerialize(key) {
						t.SkipNow()
					}
					service := createSerializationService(t, order)
					data, err := service.ToData(obj)
					require.NoError(t, err)
					readObject, err := service.ToObject(data)
					require.NoError(t, err)
					require.Equal(t, obj, readObject)
				})
			}
		}
	}
}

func readBinaryFile(t *testing.T) {
	for _, v := range versions {
		b, err := ioutil.ReadFile(createFileName(v))
		require.NoErrorf(t, err, "Could not locate file "+createFileName(v)+". Follow the instructions in BinaryCompatibilityFileGenerator to generate the file.")
		i := serialization.NewObjectDataInput(b, 0, nil, true)
		for i.Available() != 0 {
			buf := i.ReadUInt16()
			objectKey := string(readRawBytes(i, int(buf)))
			n := i.ReadInt32()
			if n != -1 {
				bytes = make([]byte, n)
				for j := int32(0); j < n; j++ {
					bytes[j] = i.ReadByte()
				}
				dataMap[objectKey] = bytes
			} else {
				dataMap[objectKey] = nil
			}
		}
	}
}

func createSerializationService(t *testing.T, byteOrder binary.ByteOrder) *serialization.Service {
	cfg := serialization2.Config{}
	err := cfg.SetCustomSerializer(reflect.TypeOf(CustomByteArraySerializable{}), &CustomByteArraySerializer{})
	require.NoError(t, err)
	err = cfg.SetCustomSerializer(reflect.TypeOf(CustomStreamSerializable{}), &CustomStreamSerializer{})
	require.NoError(t, err)
	cfg.PortableVersion = 0
	cd := serialization2.NewClassDefinition(PortableFactoryId, InnerPortableClassId, 0)
	err = cd.AddInt32Field("i")
	require.NoError(t, err)
	err = cd.AddFloat32Field("f")
	require.NoError(t, err)
	cfg.SetClassDefinitions(cd)
	cfg.SetPortableFactories(&PortableFactory{})
	cfg.SetIdentifiedDataSerializableFactories(&IdentifiedFactory{})
	if byteOrder == binary.LittleEndian {
		cfg.LittleEndian = true
	}
	s, err := serialization.NewService(&cfg)
	require.NoError(t, err)
	return s
}

func createObjectKey(name string, byteOrder binary.ByteOrder, version int) string {
	byteOrderString := ""
	if byteOrder == binary.BigEndian {
		byteOrderString = "BIG_ENDIAN"
	} else {
		byteOrderString = "LITTLE_ENDIAN"
	}
	return fmt.Sprintf("%d-%s-%s", version, name, byteOrderString)
}

func createFileName(version int) string {
	return fmt.Sprintf("%d.serialization.compatibility.binary", version)
}

func skipOnDeserialize(objType string) bool {
	p, _ := regexp.Compile("^.*(Predicate|Aggregator|Projection)$")
	return p.MatchString(objType)
}

func skipOnSerialize(objType string) bool {
	s := []string{
		"Class",
	}
	for _, v := range s {
		if v == objType {
			return true
		}
	}
	return false
}
