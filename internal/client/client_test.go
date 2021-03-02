package client

import "testing"

func TestNewImpl(t *testing.T) {
	client := NewImpl()
	targetName := "hz.client_1"
	if targetName != client.Name() {
		t.Errorf("target: %v != %v", targetName, client.Name())
		return
	}
}

func TestNewImplWithConfig(t *testing.T) {
	builder := NewConfigBuilderImpl()
	builder.SetClientName("my-client")
	config, err := builder.Config()
	if err != nil {
		t.Error(err)
		return
	}
	client := NewImpl(config)
	targetClientName := "my-client"
	if targetClientName != client.Name() {
		t.Errorf("target: %v != %v", targetClientName, client.Name())
	}
}
