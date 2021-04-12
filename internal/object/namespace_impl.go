package object

import "github.com/hazelcast/hazelcast-go-client/internal/spi"

type NamespaceImpl struct {
	ServiceName string
	ObjectName  string
}

func NewNamespaceImpl(serviceName string, objectName string) NamespaceImpl {
	return NamespaceImpl{
		ServiceName: serviceName,
		ObjectName:  objectName,
	}
}

func (NamespaceImpl) FactoryID() int32 {
	return spi.DataSerializerFactoryID
}

func (NamespaceImpl) ClassID() int32 {
	return spi.DistributedObjectNamespaceClassID
}

func (ns NamespaceImpl) WriteData(writer DataWriter) (err error) {
	if err = writer.WriteString(ns.ServiceName); err != nil {
		return
	}
	if err = writer.WriteString(ns.ObjectName); err != nil {
		return
	}
	return
}

func (ns *NamespaceImpl) ReadData(reader DataReader) error {
	if err := reader.ReadString(&ns.ServiceName); err != nil {
		return err
	}
	if err := reader.ReadString(&ns.ObjectName); err != nil {
		return err
	}
	return nil
}
