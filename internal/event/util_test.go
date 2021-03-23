package event

import "testing"

func TestMakeSubscriptionID(t *testing.T) {
	subscriptionID := MakeSubscriptionID(func() {})
	if subscriptionID == 0 {
		t.Fatalf("unexpected 0")
	}
}

func TestMakeSubscriptionIDFails(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Fatalf("expected a panic")
		}
	}()
	MakeSubscriptionID(42)
}
