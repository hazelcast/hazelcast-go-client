package main

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const identifiedEntryProcessorFactoryID = 66
const identifiedEntryProcessorClassID = 1

// IdentifiedEntryProcessor Entry Processor must be implemented on the server side
// You need to add hazelcast test jar to access this class.
// This corresponds to https://github.com/hazelcast/hazelcast/blob/master/hazelcast/src/test/java/com/hazelcast/client/test/IdentifiedEntryProcessor.java
/*
	You need to enable serialization of this via:
	<serialization>
        <data-serializable-factories>
            <data-serializable-factory factory-id="66">com.hazelcast.client.test.IdentifiedFactory
            </data-serializable-factory>
            <data-serializable-factory factory-id="666">com.hazelcast.client.test.IdentifiedDataSerializableFactory
            </data-serializable-factory>
        </data-serializable-factories>
    </serialization>
*/
type IdentifiedEntryProcessor struct {
	value string
}

func (s IdentifiedEntryProcessor) FactoryID() int32 {
	return identifiedEntryProcessorFactoryID
}

func (s IdentifiedEntryProcessor) ClassID() int32 {
	return identifiedEntryProcessorClassID
}

func (s IdentifiedEntryProcessor) WriteData(output serialization.DataOutput) {
	output.WriteString(s.value)
}

func (s *IdentifiedEntryProcessor) ReadData(input serialization.DataInput) {
	s.value = input.ReadString()
}

type IdentifiedFactory struct {
}

func (f IdentifiedFactory) Create(id int32) serialization.IdentifiedDataSerializable {
	if id == identifiedEntryProcessorClassID {
		return &IdentifiedEntryProcessor{}
	}
	panic(fmt.Sprintf("unknown class ID: %d", id))
}

func (f IdentifiedFactory) FactoryID() int32 {
	return identifiedEntryProcessorFactoryID
}
