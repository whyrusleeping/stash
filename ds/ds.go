package stashds

import (
	"errors"
	"sync"

	"github.com/ipfs/go-datastore"
	"github.com/whyrusleeping/stash"
)

var _ (datastore.Batching) = (*StashDS)(nil)

type StashDS struct {
	db *stash.StashDB

	wg      sync.WaitGroup
	closing chan struct{}
}

func NewStashDS(dir string) (*StashDS, error) {
	sdb, err := stash.NewStash(dir)
	if err != nil {
		return nil, err
	}

	return &StashDS{
		db:      sdb,
		closing: make(chan struct{}),
	}, nil
}

func (ds *StashDS) Get(k datastore.Key) ([]byte, error) {
	out, err := ds.db.Get(k.Bytes())
	if err != nil {
		if errors.Is(err, stash.ErrNotFound) {
			return nil, datastore.ErrNotFound
		}
		return nil, err
	}
	return out, nil
}

func (ds *StashDS) Put(k datastore.Key, val []byte) error {
	if err := ds.db.Put(k.Bytes(), val); err != nil {
		if errors.Is(err, stash.ErrNotFound) {
			return datastore.ErrNotFound
		}
	}
	return nil
}

func (ds *StashDS) Delete(k datastore.Key) error {
	if err := ds.db.Delete(k.Bytes()); err != nil {
		if errors.Is(err, stash.ErrNotFound) {
			return datastore.ErrNotFound
		}
	}
	return nil
}

func (ds *StashDS) Has(k datastore.Key) (bool, error) {
	return ds.db.Has(k.Bytes())
}

func (ds *StashDS) Close() error {
	return ds.db.Close()
}

func (ds *StashDS) Sync(k datastore.Key) error {
	return ds.db.Sync()
}

func (ds *StashDS) GetSize(k datastore.Key) (int, error) {
	n, err := ds.db.GetSize(k.Bytes())
	if err != nil {
		if errors.Is(err, stash.ErrNotFound) {
			return -1, datastore.ErrNotFound
		}
		return -1, err
	}

	return n, nil
}

type dsqIter struct {
}

type batch struct {
	b *stash.StashBatch
}

func (b *batch) Commit() error {
	return b.b.Commit()
}

func (b *batch) Delete(k datastore.Key) error {
	return b.b.Delete(k.Bytes())
}

func (b *batch) Put(k datastore.Key, val []byte) error {
	return b.b.Put(k.Bytes(), val)
}

func (ds *StashDS) Batch() (datastore.Batch, error) {
	b := ds.db.NewBatch()
	return &batch{b}, nil
}

func (ds *StashDS) Stats() stash.Stats {
	return ds.db.Stats()
}

//func (ds *StashDS)
//func (ds *StashDS)
