package compatibility

import (
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

var (
	byteOrders = []binary.ByteOrder{binary.BigEndian, binary.LittleEndian}
	versions   = []int{1}
)

func TestBinaryCompatibility(t *testing.T) {
	allTestObjects := makeTestObjects()
	dataMap := readBinaryFile(t)
	for objName, obj := range allTestObjects {
		for _, order := range byteOrders {
			for _, version := range versions {
				key := createObjectKey(objName, order, version)
				t.Run(key+"-testDeserialize", func(t *testing.T) {
					if skipOnDeserialize(objName) {
						t.SkipNow()
					}
					service := createSerializationService(t, order)
					toObj, err := service.ToObject(dataMap[key])
					require.NoError(t, err)
					require.Equal(t, obj, toObj)
				})
				t.Run(key+"-testSerialize", func(t *testing.T) {
					if skipOnSerialize(objName) {
						t.SkipNow()
					}
					service := createSerializationService(t, order)
					data, err := service.ToData(obj)
					require.NoError(t, err)
					require.Equal(t, dataMap[key], data)
				})
				t.Run(key+"-testSerializeDeserialize", func(t *testing.T) {
					if skipOnDeserialize(objName) || skipOnSerialize(objName) {
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

func readBinaryFile(t *testing.T) map[string]serialization.Data {
	dataMap := make(map[string]serialization.Data)
	for _, v := range versions {
		b, err := os.ReadFile(createFileName(v))
		require.NoErrorf(t, err, "Could not locate file "+createFileName(v)+". Follow the instructions in BinaryCompatibilityFileGenerator to generate the file.")
		i := serialization.NewObjectDataInput(b, 0, nil, true)
		for i.Available() != 0 {
			buf := i.ReadUInt16()
			key := string(readRawBytes(i, int(buf)))
			n := i.ReadInt32()
			dataMap[key] = nil
			if n != -1 {
				bytes := make([]byte, n)
				for j := int32(0); j < n; j++ {
					bytes[j] = i.ReadByte()
				}
				dataMap[key] = bytes
			}
		}
	}
	return dataMap
}

func createSerializationService(t *testing.T, bo binary.ByteOrder) *serialization.Service {
	cfg := pubserialization.Config{}
	err := cfg.SetCustomSerializer(reflect.TypeOf(CustomByteArraySerializable{}), &CustomByteArraySerializer{})
	require.NoError(t, err)
	err = cfg.SetCustomSerializer(reflect.TypeOf(CustomStreamSerializable{}), &CustomStreamSerializer{})
	require.NoError(t, err)
	cfg.PortableVersion = 0
	cd := pubserialization.NewClassDefinition(portableFactoryId, innerPortableClassId, 0)
	err = cd.AddInt32Field("i")
	require.NoError(t, err)
	err = cd.AddFloat32Field("f")
	require.NoError(t, err)
	cfg.SetClassDefinitions(cd)
	cfg.SetPortableFactories(&PortableFactory{})
	cfg.SetIdentifiedDataSerializableFactories(&IdentifiedFactory{})
	cfg.LittleEndian = bo == binary.LittleEndian
	s, err := serialization.NewService(&cfg)
	require.NoError(t, err)
	return s
}

func createObjectKey(name string, byteOrder binary.ByteOrder, version int) string {
	byteOrderString := "LITTLE_ENDIAN"
	if byteOrder == binary.BigEndian {
		byteOrderString = "BIG_ENDIAN"
	}
	return fmt.Sprintf("%d-%s-%s", version, name, byteOrderString)
}

func createFileName(version int) string {
	return fmt.Sprintf("%d.serialization.compatibility.binary", version)
}

func skipOnDeserialize(o string) bool {
	return strings.HasSuffix(o, "Predicate") || strings.HasSuffix(o, "Aggregator") || strings.HasSuffix(o, "Projection")
}

func skipOnSerialize(objType string) bool {
	return objType == "Class"
}
