package stash

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"golang.org/x/xerrors"
)

var ErrNotFound = fmt.Errorf("stash: not found")

const valueHeaderSize = 8

type StashDB struct {
	Dir string
	KV  *pebble.DB

	valLogLk  sync.Mutex
	valueLogs []*valueLog

	// values above threshold size get put in flat file storage, below are
	// stored in the KV
	valSizeThreshold int

	// size of value log files for larger values
	valueLogSize int64

	hcMut    sync.RWMutex
	hasCache map[string]bool

	kvCount int64
	vlCount int64

	hasCount int64

	batchkvCount int64
	batchvlCount int64
	batchCount   int64
}

func NewStash(dir string) (*StashDB, error) {
	pb, err := pebble.Open(filepath.Join(dir, "KV"), &pebble.Options{
		Cache:                    pebble.NewCache(int64(512 * 1024 * 1024)),
		MemTableSize:             (512 * 1024 * 1024) / 4,
		MaxConcurrentCompactions: runtime.NumCPU(),
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 16 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.NoCompression},
		},
	})
	if err != nil {
		return nil, err
	}

	stash := &StashDB{
		Dir:              dir,
		KV:               pb,
		valSizeThreshold: 256,
		valueLogSize:     8 << 20,
		hasCache:         make(map[string]bool),
	}

	if err := stash.loadVLogs(); err != nil {
		return nil, err
	}

	return stash, nil
}

func (sdb *StashDB) loadVLogs() error {
	vldir := filepath.Join(sdb.Dir, "vlogs")
	_, err := os.Stat(vldir)
	switch {
	default:
		return err
	case os.IsNotExist(err):
		return os.Mkdir(vldir, 0755)
	case err == nil:
		// ok
	}

	// enumerate all the vlog files...
	for i := 0; true; i++ {
		vlname := filepath.Join(vldir, fmt.Sprintf("vlog-%d", i))
		fi, err := os.Open(vlname)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}

			return err
		}

		finfo, err := fi.Stat()
		if err != nil {
			return err
		}

		sdb.valueLogs = append(sdb.valueLogs, &valueLog{
			ID:        i,
			fd:        fi,
			allocated: finfo.Size(),
		})
	}

	return nil
}

type valueLog struct {
	lk        sync.Mutex
	ID        int
	fd        *os.File
	allocated int64
}

type KVMeta struct {
	// If value is below size threshold, it will be stored here directly
	Val []byte

	// If value is above size threshold, there will be a reference to where it lives ehre
	BufRef int
	Offset int64
	Length uint32
}

func (sdb *StashDB) Put(k []byte, val []byte) error {
	/*
		// I think we can just avoid having this 'overwrite cleaning' stuff.
		// This datastore is designed explicitly for content addressed storage,
		// in which you never write the same key twice
			var oldValue bool
			prev, err := sdb.getMeta(k)
			if err == nil {
				oldValue = true
			} else {
				// TODO: assert this is a 'not found' error, otherwise propagate
			}
	*/

	if len(val) < sdb.valSizeThreshold {
		atomic.AddInt64(&sdb.kvCount, 1)
		return putKV(k, KVMeta{
			Val: val,
		}, sdb.kvSingleWrite)
	}

	atomic.AddInt64(&sdb.vlCount, 1)
	kvm, err := sdb.addToValueLog([][]byte{val})
	if err != nil {
		return xerrors.Errorf("failed to add to value log: %w", err)
	}

	if err := putKV(k, kvm[0], sdb.kvSingleWrite); err != nil {
		// TODO: mark value log entry as dead
		return xerrors.Errorf("failed to persist meta for value log entry: %w", err)
	}

	/*
		if oldValue {
			if err := sdb.markForFree(prev); err != nil {
				return xerrors.Errorf("failed to mark old value for freeing: %w", err)
			}
		}
	*/

	return nil
}

func (sdb *StashDB) addToValueLog(vals [][]byte) ([]KVMeta, error) {
	sdb.valLogLk.Lock()
	defer sdb.valLogLk.Unlock()

	vl, err := sdb.curValueLog()
	if err != nil {
		return nil, xerrors.Errorf("failed to get current value log: %w", err)
	}

	out := make([]KVMeta, 0, len(vals))
	for _, blob := range vals {

		if sdb.valueLogSize-vl.allocated < int64(len(blob)) {
			// not enough space in this log, move on to the next
			nvl, err := sdb.nextValueLog()
			if err != nil {
				return nil, xerrors.Errorf("failed to allocate next value log: %w", err)
			}

			if sdb.valueLogSize-nvl.allocated < int64(len(blob)) {
				return nil, xerrors.Errorf("newly allocated value log somehow doesnt have enough space for value")
			}

			vl = nvl
		}

		offset, err := vl.putValue(blob)
		if err != nil {
			return nil, xerrors.Errorf("failed to write value to log: %w", err)
		}

		out = append(out, KVMeta{
			BufRef: vl.ID,
			Offset: offset,
			Length: uint32(len(blob)),
		})
	}
	return out, nil
}

func (vl *valueLog) putValue(v []byte) (int64, error) {
	vl.lk.Lock() // supporting concurrent writes is easy, just being lazy for now
	defer vl.lk.Unlock()
	offset := vl.allocated
	vl.allocated += int64(valueHeaderSize + len(v))
	if err := vl.fd.Truncate(vl.allocated); err != nil {
		return 0, xerrors.Errorf("failed to expand vlog file: %w", err)
	}

	header := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(header[4:], uint32(len(v)))

	_, err := vl.fd.WriteAt(header, offset)
	if err != nil {
		return 0, xerrors.Errorf("failed to write header: %w", err)
	}

	_, err = vl.fd.WriteAt(v, offset+valueHeaderSize)
	if err != nil {
		return 0, xerrors.Errorf("failed to write value: %w", err)
	}

	return offset, nil
}

func (sdb *StashDB) lastVLogFull() bool {
	last := sdb.valueLogs[len(sdb.valueLogs)-1]
	if sdb.valueLogSize-last.allocated < int64(valueHeaderSize+sdb.valSizeThreshold) {
		return true
	}
	return false
}

func (sdb *StashDB) curValueLog() (*valueLog, error) {
	if len(sdb.valueLogs) == 0 || sdb.lastVLogFull() {
		return sdb.nextValueLog()
	}

	return sdb.valueLogs[len(sdb.valueLogs)-1], nil
}

func (sdb *StashDB) nextValueLog() (*valueLog, error) {
	num := len(sdb.valueLogs)
	vlPath := filepath.Join(sdb.Dir, "vlogs", fmt.Sprintf("vlog-%d", num))

	fi, err := os.Create(vlPath)
	if err != nil {
		return nil, xerrors.Errorf("failed to open new value log file: %w", err)
	}

	nvlog := &valueLog{
		ID:        num,
		fd:        fi,
		allocated: 0,
	}

	sdb.valueLogs = append(sdb.valueLogs, nvlog)

	return nvlog, nil
}

func (sdb *StashDB) kvSingleWrite(k []byte, v []byte) error {
	return sdb.KV.Set(k, v, pebble.NoSync)
}

func putKV(k []byte, meta KVMeta, wf func([]byte, []byte) error) error {
	// TODO: obviously not use json...
	b, err := json.Marshal(meta)
	if err != nil {
		return xerrors.Errorf("failed to serialize KVMeta: %w", err)
	}

	return wf(k, b)
}

func (sdb *StashDB) Get(k []byte) ([]byte, error) {
	kvm, err := sdb.getMeta(k)
	if err != nil {
		return nil, err
	}

	return sdb.readValue(kvm)
}

func (sdb *StashDB) readValue(kvm KVMeta) ([]byte, error) {
	if len(kvm.Val) > 0 {
		return kvm.Val, nil
	}

	if kvm.Length == 0 {
		return nil, nil
	}

	var out []byte
	if err := sdb.readValueLogEntry(kvm, func(b []byte) error {
		out = make([]byte, len(b))
		copy(out, b)
		return nil
	}); err != nil {
		return nil, err
	}

	return out, nil
}

// This is only exposed for the datastore query stuff, since it direct accesses pebble. needs improving..
func (sdb *StashDB) ReadMetaValue(kvm KVMeta) ([]byte, error) {
	return sdb.readValue(kvm)
}

func (sdb *StashDB) getMeta(k []byte) (KVMeta, error) {
	val, closer, err := sdb.KV.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return KVMeta{}, ErrNotFound
		}
		return KVMeta{}, err
	}

	defer closer.Close()

	return MetaFromRaw(val)
}

func MetaFromRaw(b []byte) (KVMeta, error) {
	var kvm KVMeta
	if err := json.Unmarshal(b, &kvm); err != nil {
		return KVMeta{}, err
	}

	return kvm, nil
}

func (sdb *StashDB) readValueLogEntry(kvm KVMeta, cb func([]byte) error) error {
	vl, err := sdb.getValueLog(kvm.BufRef)
	if err != nil {
		return err
	}

	vl.lk.Lock()
	defer vl.lk.Unlock()

	header := make([]byte, valueHeaderSize)
	if _, err := vl.fd.ReadAt(header, kvm.Offset); err != nil {
		return err
	}

	l := binary.LittleEndian.Uint32(header[4:])

	if l != kvm.Length {
		return fmt.Errorf("mismatch in lengths between kvmeta and value log: %d != %d", l, kvm.Length)
	}

	buf := make([]byte, l) // TODO: buffer pool maybe?
	if _, err := vl.fd.ReadAt(buf, kvm.Offset+valueHeaderSize); err != nil {
		return err
	}

	if err := cb(buf); err != nil {
		return err
	}

	// release 'buf' to buffer pool
	return nil
}

func (sdb *StashDB) getValueLog(n int) (*valueLog, error) {
	sdb.valLogLk.Lock()
	defer sdb.valLogLk.Unlock()

	if n >= len(sdb.valueLogs) {
		return nil, fmt.Errorf("no such value log %d", n)
	}

	return sdb.valueLogs[n], nil
}

func (sdb *StashDB) Sync() error {
	flush, err := sdb.KV.AsyncFlush()
	if err != nil {
		return err
	}

	{
		sdb.valLogLk.Lock()
		defer sdb.valLogLk.Unlock()
		for _, vl := range sdb.valueLogs {
			if err := vl.fd.Sync(); err != nil {
				return err
			}
		}
	}

	<-flush
	return nil
}

func (sdb *StashDB) Close() error {
	// hrm. probably want to consider people calling close concurrently to
	// their application still having all sorts of things in flight...

	for _, vl := range sdb.valueLogs {
		if err := vl.fd.Close(); err != nil {
			return err
		}
	}

	return sdb.KV.Close()
}

func (sdb *StashDB) Has(k []byte) (bool, error) {
	atomic.AddInt64(&sdb.hasCount, 1)

	_, closer, err := sdb.KV.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}

	closer.Close()
	return true, nil
}

type StashIter struct {
	pebiter       *pebble.Iterator
	includeValues bool

	sdb *StashDB
}

type iterVal struct {
	Key  []byte
	Val  []byte
	Size int
}

func (si *StashIter) Next() (iterVal, bool, error) {
	if !si.pebiter.Next() {
		return iterVal{}, false, nil
	}

	if err := si.pebiter.Error(); err != nil {
		return iterVal{}, false, err
	}

	iv := iterVal{
		Key: si.pebiter.Key(),
	}

	kvm, err := MetaFromRaw(si.pebiter.Value())
	if err != nil {
		return iterVal{}, false, err
	}

	iv.Size = int(kvm.Length)

	if si.includeValues {
		val, err := si.sdb.readValue(kvm)
		if err != nil {
			return iterVal{}, false, err
		}

		iv.Val = val
	}

	return iv, true, nil
}

func (si *StashIter) Close() error {
	return si.pebiter.Close()
}

func (sdb *StashDB) Iter(inclVals bool, opts *pebble.IterOptions) *StashIter {
	pebiter := sdb.KV.NewIter(opts)
	return &StashIter{
		pebiter:       pebiter,
		sdb:           sdb,
		includeValues: inclVals,
	}
}

func (sdb *StashDB) GetSize(k []byte) (int, error) {
	m, err := sdb.getMeta(k)
	if err != nil {
		return 0, err
	}

	if len(m.Val) > 0 {
		return len(m.Val), nil
	}

	return int(m.Length), nil
}

func (sdb *StashDB) Delete(k []byte) error {
	m, err := sdb.getMeta(k)
	if err != nil {
		return err
	}

	if err := sdb.KV.Delete(k, nil); err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return ErrNotFound
		}
		return err
	}

	if len(m.Val) == 0 && m.Length > 0 {
		if err := sdb.markForFree(m); err != nil {
			return xerrors.Errorf("failed to mark deleted entry as freed: %w", err)
		}
	}

	return nil
}

func (sdb *StashDB) markForFree(m KVMeta) error {
	vl, err := sdb.getValueLog(m.BufRef)
	if err != nil {
		return err
	}

	if _, err := vl.fd.WriteAt([]byte{2}, m.Offset); err != nil {
		return err
	}

	return nil
}

type StashBatch struct {
	kvb *pebble.Batch

	db *StashDB

	directKeys [][]byte

	keys [][]byte
	vals [][]byte

	deletes [][]byte
}

func (sb *StashBatch) Put(k, v []byte) error {
	if len(v) < sb.db.valSizeThreshold {
		atomic.AddInt64(&sb.db.batchkvCount, 1)
		if err := putKV(k, KVMeta{
			Val: v,
		}, func(k, v []byte) error {
			return sb.kvb.Set(k, v, pebble.NoSync)
		}); err != nil {
			return err
		}

		sb.directKeys = append(sb.directKeys, k)
		return nil
	}

	sb.keys = append(sb.keys, k)
	sb.vals = append(sb.vals, v)
	return nil
}
func (sb *StashBatch) Commit() error {
	// TODO: check for any overwriting of previously existing values and mark
	// them as 'dead'
	atomic.AddInt64(&sb.db.batchCount, 1)

	atomic.AddInt64(&sb.db.batchvlCount, int64(len(sb.vals)))
	kvms, err := sb.db.addToValueLog(sb.vals)
	if err != nil {
		return err
	}

	for i := 0; i < len(sb.keys); i++ {
		if err := putKV(sb.keys[i], kvms[i], func(k, v []byte) error {
			return sb.kvb.Set(k, v, pebble.NoSync)
		}); err != nil {
			return err
		}
	}

	delKvms := make([]KVMeta, 0, len(sb.deletes))
	for _, k := range sb.deletes {
		kvm, err := sb.db.getMeta(k)
		if err != nil {
			return err
		}

		delKvms = append(delKvms, kvm)
		if err := sb.kvb.Delete(k, nil); err != nil {
			return err
		}
	}

	if err := sb.kvb.Commit(pebble.NoSync); err != nil {
		return err
	}

	for _, kvm := range delKvms {
		if err := sb.db.markForFree(kvm); err != nil {
			return err
		}
	}

	return nil
}

func (sb *StashBatch) Delete(k []byte) error {
	sb.deletes = append(sb.deletes, k)
	return nil
}

func (sdb *StashDB) NewBatch() *StashBatch {
	b := sdb.KV.NewBatch()

	return &StashBatch{
		kvb: b,
		db:  sdb,
	}
}

type Stats struct {
	KVPutCount      int64
	VLPutCount      int64
	BatchCount      int64
	BatchKVPutCount int64
	BatchVLPutCount int64
	HasCount        int64
}

func (sdb *StashDB) Stats() Stats {
	return Stats{
		KVPutCount:      atomic.LoadInt64(&sdb.kvCount),
		VLPutCount:      atomic.LoadInt64(&sdb.vlCount),
		BatchCount:      atomic.LoadInt64(&sdb.batchCount),
		BatchKVPutCount: atomic.LoadInt64(&sdb.batchkvCount),
		BatchVLPutCount: atomic.LoadInt64(&sdb.batchvlCount),
		HasCount:        atomic.LoadInt64(&sdb.hasCount),
	}
}
