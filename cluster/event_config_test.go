package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEventConfig_Clone(t *testing.T) {
	testConf := EventConfig{
		EventWorkerCount:   10,
		EventQueueCapacity: 10,
	}
	assert.Equal(t, testConf, testConf.Clone())
}

func TestEventConfig_Validate(t *testing.T) {
	tests := []struct {
		name            string
		toValidate      EventConfig
		afterValidation EventConfig
		wantErr         bool
	}{
		{
			name:       "default conf",
			toValidate: EventConfig{},
			afterValidation: EventConfig{
				EventWorkerCount:   defaultEventWorkerCount,
				EventQueueCapacity: defaultEventQueueCapacity,
			},
			wantErr: false,
		},
		{
			name: "modified conf",
			toValidate: EventConfig{
				EventWorkerCount:   10,
				EventQueueCapacity: 10,
			},
			afterValidation: EventConfig{
				EventWorkerCount:   10,
				EventQueueCapacity: 10,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.toValidate.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.afterValidation, tt.toValidate)
		})
	}
}
