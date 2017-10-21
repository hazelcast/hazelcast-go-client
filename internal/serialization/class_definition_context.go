package serialization

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
)

type ClassDefinitionContext struct {
	factoryId int32
	classDefs map[string]*ClassDefinition
}

func NewClassDefinitionContext(factoryId int32, portableVersion int32) *ClassDefinitionContext {
	return &ClassDefinitionContext{factoryId, make(map[string]*ClassDefinition)}
}

func (c *ClassDefinitionContext) LookUp(classId int32, version int32) *ClassDefinition {
	return c.classDefs[encodeVersionedClassId(classId, version)]
}

func (c *ClassDefinitionContext) Register(classDefinition *ClassDefinition) *ClassDefinition {
	if classDefinition == nil {
		return nil
	}
	if classDefinition.factoryId != c.factoryId {
		fmt.Printf("This factory's id is %d. Intended factory id is %d.", c.factoryId, classDefinition.factoryId)
	}
	classDefKey := encodeVersionedClassId(classDefinition.classId, classDefinition.factoryId)
	current := c.classDefs[classDefKey]
	if current == nil {
		c.classDefs[classDefKey] = classDefinition
		return classDefinition
	}
	// TODO check is problem. reflect.deepEqual can be used
	if !reflect.DeepEqual(current, classDefinition) {
		fmt.Printf("Incompatible class definition with same class id: %d", classDefinition.classId)
	}
	return classDefinition
}

func encodeVersionedClassId(classId int32, version int32) string {
	return strconv.Itoa(int(classId)) + "v" + strconv.Itoa(int(version))
}

func decodeVersionedClassId(encoded string) (int32, int32) {
	re := regexp.MustCompile("[0-9]+")
	pair := re.FindAllString(encoded, -1)
	classId, _ := strconv.Atoi(pair[0])
	version, _ := strconv.Atoi(pair[1])
	return int32(classId), int32(version)
}
