package stashds

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/jbenet/goprocess"
	"github.com/whyrusleeping/stash"
)

func (d *StashDS) Query(q query.Query) (query.Results, error) {
	var (
		prefix      = ds.NewKey(q.Prefix).String()
		limit       = q.Limit
		offset      = q.Offset
		orders      = q.Orders
		filters     = q.Filters
		keysOnly    = q.KeysOnly
		_           = q.ReturnExpirations // pebble doesn't support TTL; noop
		returnSizes = q.ReturnsSizes
	)

	if prefix != "/" {
		prefix = prefix + "/"
	}

	opts := &pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: func() []byte {
			// if the prefix is 0x01..., we want 0x02 as an upper bound.
			// if the prefix is 0x0000ff..., we want 0x0001 as an upper bound.
			// if the prefix is 0x0000ff01..., we want 0x0000ff02 as an upper bound.
			// if the prefix is 0xffffff..., we don't want an upper bound.
			// if the prefix is 0xff..., we don't want an upper bound.
			// if the prefix is empty, we don't want an upper bound.
			// basically, we want to find the last byte that can be lexicographically incremented.
			var upper []byte
			for i := len(prefix) - 1; i >= 0; i-- {
				b := prefix[i]
				if b == 0xff {
					continue
				}
				upper = make([]byte, i+1)
				copy(upper, prefix)
				upper[i] = b + 1
				break
			}
			return upper
		}(),
	}

	iter := d.db.KV.NewIter(opts)

	var move func() bool
	switch l := len(orders); l {
	case 0:
		iter.First()
		move = iter.Next
	case 1:
		switch o := orders[0]; o.(type) {
		case query.OrderByKey, *query.OrderByKey:
			iter.First()
			move = iter.Next
		case query.OrderByKeyDescending, *query.OrderByKeyDescending:
			iter.Last()
			move = iter.Prev
		default:
			defer iter.Close()
			return d.inefficientOrderQuery(q, nil)
		}
	default:
		var baseOrder query.Order
		for _, o := range orders {
			if baseOrder != nil {
				return nil, fmt.Errorf("incompatible orders passed: %+v", orders)
			}
			switch o.(type) {
			case query.OrderByKey, query.OrderByKeyDescending, *query.OrderByKey, *query.OrderByKeyDescending:
				baseOrder = o
			}
		}
		defer iter.Close()
		return d.inefficientOrderQuery(q, baseOrder)
	}

	if !iter.Valid() {
		_ = iter.Close()
		// there are no valid results.
		return query.ResultsWithEntries(q, []query.Entry{}), nil
	}

	// filterFn takes an Entry and tells us if we should return it.
	filterFn := func(entry query.Entry) bool {
		for _, f := range filters {
			if !f.Filter(entry) {
				return false
			}
		}
		return true
	}
	doFilter := false
	if len(filters) > 0 {
		doFilter = true
	}

	createEntry := func() (query.Entry, error) {
		// iter.Key and iter.Value may change on the next call to iter.Next.
		// string conversion takes a copy
		entry := query.Entry{Key: string(iter.Key())}
		if !keysOnly {

			kvm, err := stash.MetaFromRaw(iter.Value())
			if err != nil {
				return query.Entry{}, err
			}

			val, err := d.db.ReadMetaValue(kvm)
			if err != nil {
				return query.Entry{}, err
			}

			// take a copy.
			entry.Value = val
		}
		if returnSizes {
			entry.Size = len(iter.Value())
		}
		return entry, nil
	}

	d.wg.Add(1)
	results := query.ResultsWithProcess(q, func(proc goprocess.Process, outCh chan<- query.Result) {
		defer d.wg.Done()
		defer iter.Close()

		const interrupted = "interrupted"

		defer func() {
			switch r := recover(); r {
			case nil, interrupted:
				// nothing, or flow interrupted; all ok.
			default:
				panic(r) // a genuine panic, propagate.
			}
		}()

		sendOrInterrupt := func(r query.Result) {
			select {
			case outCh <- r:
				return
			case <-d.closing:
			case <-proc.Closed():
			case <-proc.Closing(): // client told us to close early
			}

			// we are closing; try to send a closure error to the client.
			// but do not halt because they might have stopped receiving.
			select {
			case outCh <- query.Result{Error: fmt.Errorf("close requested")}:
			default:
			}
			panic(interrupted)
		}

		// skip over 'offset' entries; if a filter is provided, only entries
		// that match the filter will be counted as a skipped entry.
		for skipped := 0; skipped < offset && iter.Valid(); move() {
			if err := iter.Error(); err != nil {
				sendOrInterrupt(query.Result{Error: err})
			}
			e, err := createEntry()
			if err != nil {
				panic(err) // TODO:
			}
			if doFilter && !filterFn(e) {
				// if we have a filter, and this entry doesn't match it,
				// don't count it.
				continue
			}
			skipped++
		}

		// start sending results, capped at limit (if > 0)
		for sent := 0; (limit <= 0 || sent < limit) && iter.Valid(); move() {
			if err := iter.Error(); err != nil {
				sendOrInterrupt(query.Result{Error: err})
				continue
			}

			entry, err := createEntry()
			if err != nil {
				sendOrInterrupt(query.Result{Error: err})
				continue
			}
			if doFilter && !filterFn(entry) {
				// if we have a filter, and this entry doesn't match it,
				// do not sendOrInterrupt it.
				continue
			}
			sendOrInterrupt(query.Result{Entry: entry})
			sent++
		}
	})
	return results, nil
}

func (d *StashDS) inefficientOrderQuery(q query.Query, baseOrder query.Order) (query.Results, error) {
	// Ok, we have a weird order we can't handle. Let's
	// perform the _base_ query (prefix, filter, etc.), then
	// handle sort/offset/limit later.

	// Skip the stuff we can't apply.
	baseQuery := q
	baseQuery.Limit = 0
	baseQuery.Offset = 0
	baseQuery.Orders = nil
	if baseOrder != nil {
		baseQuery.Orders = []query.Order{baseOrder}
	}

	// perform the base query.
	res, err := d.Query(baseQuery)
	if err != nil {
		return nil, err
	}

	// fix the query
	res = query.ResultsReplaceQuery(res, q)

	// Remove the parts we've already applied.
	naiveQuery := q
	naiveQuery.Prefix = ""
	naiveQuery.Filters = nil

	// Apply the rest of the query
	return query.NaiveQueryApply(naiveQuery, res), nil
}
