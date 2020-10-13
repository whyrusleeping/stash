package stash

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func randVal(size int) []byte {
	buf := make([]byte, size)

	rand.Read(buf)
	return buf
}

func TestBasicPutGet(t *testing.T) {
	dirn, err := ioutil.TempDir("", "stash-test")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = os.RemoveAll(dirn)
	}()

	db, err := NewStash(dirn)
	if err != nil {
		t.Fatal(err)
	}

	// one into the KV
	cases := [][][]byte{
		[][]byte{[]byte("cats"), []byte("are cool")},
		[][]byte{[]byte("dogs"), randVal(200)},
		[][]byte{[]byte("bears"), randVal(1024)},
		[][]byte{[]byte("fish"), randVal(8000)},
		[][]byte{[]byte("frogs"), randVal(313)},
	}

	for _, c := range cases {
		if err := db.Put(c[0], c[1]); err != nil {
			t.Fatal(err)
		}
	}

	for _, c := range cases {
		v, err := db.Get(c[0])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(c[1], v) {
			t.Fatal("output data mismatch")
		}
	}
}

func TestFillValueLog(t *testing.T) {
	dirn, err := ioutil.TempDir("", "stash-test")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = os.RemoveAll(dirn)
	}()

	db, err := NewStash(dirn)
	if err != nil {
		t.Fatal(err)
	}

	kvs := [][][]byte{}
	for i := 0; i < 2000; i++ {
		k := []byte(fmt.Sprintf("i am a key %d", i))
		v := randVal(3000)

		kvs = append(kvs, [][]byte{k, v})

		if err := db.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}

	for _, c := range kvs {
		v, err := db.Get(c[0])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(c[1], v) {
			t.Fatal("output data mismatch")
		}
	}
}

func TestHas(t *testing.T) {
	dirn, err := ioutil.TempDir("", "stash-test")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = os.RemoveAll(dirn)
	}()

	db, err := NewStash(dirn)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put([]byte("cats"), []byte("are cool")); err != nil {
		t.Fatal(err)
	}

	has, err := db.Has([]byte("cats"))
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("should have cats")
	}

	has, err = db.Has([]byte("dogs"))
	if err != nil {
		t.Fatal(err)
	}

	if has {
		t.Fatal("should not have dogs")
	}
}
