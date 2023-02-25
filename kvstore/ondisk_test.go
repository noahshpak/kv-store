package kvstore

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
)

func TestOnDisk(t *testing.T) {
	path, err := ioutil.TempDir(".", "test_db")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(path)

	s := Load(path)
	if len(s.onDiskIndex) != 0 {
		t.Errorf("index should be empty but found len == %d", len(s.onDiskIndex))
	}

	for i, k := range []string{"a", "b", "c"} {
		s.Set(k, strconv.Itoa(i))
	}
	if len(s.onDiskIndex) != 3 {
		t.Error("Expected 3 items")
	}
	if v := s.Get("b"); v != "1" {
		t.Errorf("Expected b => 2 but got %s", v)
	}
}
