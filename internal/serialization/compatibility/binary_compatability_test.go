package compatibility

import (
	"encoding/binary"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	serialization2 "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"reflect"
	"testing"
)

var (
	objects    = allTestObjects
	dataMap    = make(map[string]serialization.Data)
	byteOrders = []binary.ByteOrder{binary.BigEndian, binary.LittleEndian}
	versions   = []int{1}
)

func TestBinaryCompatibility(t *testing.T) {
	readBinaryFile(t)
	for key, obj := range objects {
		for _, order := range byteOrders {
			for _, version := range versions {
				t.Run(createObjectKey(key, order, version)+"-readAndVerifyBinaries", func(t *testing.T) {
					// readAndVerifyBinaries
					key := createObjectKey(key, order, version)
					service := createSerializationService(t, order)
					readObject, err := service.ToObject(dataMap[key])
					require.NoError(t, err)
					require.Equal(t, readObject, obj)
				})
				t.Run(createObjectKey(key, order, version)+"-basicSerializeDeserialize", func(t *testing.T) {
					// basicSerializeDeserialize
					service := createSerializationService(t, order)
					data, err := service.ToData(obj)
					require.NoError(t, err)
					readObject, err := service.ToObject(data)
					require.NoError(t, err)
					require.Equal(t, readObject, obj)
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
			buf := i.ReadRaw(2)
			as := int32(binary.BigEndian.Uint16(buf))
			object_key_buf := i.ReadRaw(as)
			objectKey := string(object_key_buf)
			n := i.ReadInt32()
			if n != -1 {
				bytes = i.ReadRaw(n)
				dataMap[objectKey] = bytes
			}
		}
	}
}

func createSerializationService(t *testing.T, byteOrder binary.ByteOrder) *serialization.Service {
	cfg := serialization2.Config{}
	err := cfg.SetCustomSerializer(reflect.TypeOf(&CustomByteArraySerializable{}), &CustomByteArraySerializer{})
	require.NoError(t, err)
	err = cfg.SetCustomSerializer(reflect.TypeOf(&CustomStreamSerializable{}), &CustomStreamSerializer{})
	require.NoError(t, err)
	cdb := serialization2.NewClassDefinition(PortableFactoryId, InnerPortableClassId, cfg.PortableVersion)
	err = cdb.AddInt32Field("i")
	require.NoError(t, err)
	err = cdb.AddFloat32Field("f")
	require.NoError(t, err)
	cfg.SetClassDefinitions(cdb)
	cfg.SetPortableFactories(&PortableFactory{})
	cfg.SetIdentifiedDataSerializableFactories(&IdentifiedFactory{})
	cfg.LittleEndian = true
	if byteOrder == binary.BigEndian {
		cfg.LittleEndian = false
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
