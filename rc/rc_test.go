package rc

import (
	"testing"
)

func TestLangFromString(t *testing.T) {
	lang, _ := LangFromString("PYTHON")
	if lang != Lang_PYTHON {
		t.Fatal("Not Python")
	}
}

func TestRemoteControllerClient_Ping(t *testing.T) {
	remoteController, err := NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		t.Fatal("create remote controller failed:", err)
	}

	result, pingErr := remoteController.Ping()

	if pingErr != nil {
		t.Fatal(pingErr)
	}
	if !result {
		t.Fatal("Cannot reach server")
	}

}
