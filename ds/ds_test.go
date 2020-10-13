package stashds

import (
	"io/ioutil"
	"os"
	"testing"

	dstest "github.com/ipfs/go-datastore/test"
)

func TestDatastore(t *testing.T) {
	dir, err := ioutil.TempDir("", "stashtest")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = os.RemoveAll(dir)
	}()

	ds, err := NewStashDS(dir)
	if err != nil {
		t.Fatal(err)
	}

	dstest.SubtestAll(t, ds)

	//dstest.SubtestBasicPutGet(t, ds)
	//dstest.SubtestBasicSync(t, ds)
}
