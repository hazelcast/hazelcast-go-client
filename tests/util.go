package tests

import "testing"

func assertEqualf(t *testing.T, err error, l interface{}, r interface{}, message string) {
	if err != nil {
		t.Fatal(err)
	}
	if l != r {
		t.Fatalf("%v != %v : %v", l, r, message)
	}

}

func assertEqual(t *testing.T, err error, l interface{}, r interface{}) {
	if err != nil {
		t.Fatal(err)
	}
	if l != r {
		t.Fatalf("%v != %v", l, r)
	}

}
