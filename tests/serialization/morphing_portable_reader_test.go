package serialization

import (
	"fmt"
	"github.com/hazelcast/go-client"
	. "github.com/hazelcast/go-client/rc"
	. "github.com/hazelcast/go-client/serialization"
	. "github.com/hazelcast/go-client/tests"
	"log"
	"testing"
)

type student struct {
	id   int16
	age  int32
	name string
}

func (*student) FactoryId() int32 {
	return 2
}

func (*student) ClassId() int32 {
	return 1
}

func (s *student) WritePortable(writer PortableWriter) {
	writer.WriteInt16("id", s.id)
	writer.WriteInt32("age", s.age)
	writer.WriteUTF("name", s.name)
}

func (s *student) ReadPortable(reader PortableReader) {
	s.id, _ = reader.ReadInt16("id")
	s.age, _ = reader.ReadInt32("age")
	s.name, _ = reader.ReadUTF("name")
}

type PortableFactory2 struct {
}

func (*PortableFactory2) Create(classId int32) Portable {
	if classId == 1 {
		return &student{}
	}
	return nil
}

func TestMorphingPortableReader(t *testing.T) {
	remoteController, err := NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", DEFAULT_XML_CONFIG)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewHazelcastConfig()
	config.SerializationConfig.AddPortableFactory(2, &PortableFactory2{})
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	mapName := "myMap"
	mp, _ := client.GetMap(&mapName)
	mp.Put("student", &student{10, 22, "Furkan Şenharputlu"})

	//config.SerializationConfig.SetPortableVersion(1)
	fmt.Println(mp.Get("student"))

}
