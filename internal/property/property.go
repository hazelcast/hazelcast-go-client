package property

import (
	pubproperty "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/property"
)

var defaultProperties = map[string]string{
	pubproperty.LoggingLevel: "info",
}

func UpdateWithMissingProps(props map[string]string) {
	for k, defaultValue := range defaultProperties {
		if _, ok := props[k]; !ok {
			props[k] = defaultValue
		}
	}
}
