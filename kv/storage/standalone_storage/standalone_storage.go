package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf    *config.Config
	engines *engine_util.Engines
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

type StandAloneStorageIterator struct {
	it *badger.Iterator
	cf string
}

type StandAloneStorageItem struct {
	item *badger.Item
	cf   string
}

func (t StandAloneStorageItem) Key() []byte {
	return t.item.Key()[len(t.cf)+1:]
}

func (t StandAloneStorageItem) KeyCopy(dst []byte) []byte {
	return t.item.KeyCopy(dst)
}

func (t StandAloneStorageItem) Value() ([]byte, error) {
	return t.item.Value()
}

func (t StandAloneStorageItem) ValueSize() int {
	return t.item.ValueSize()
}

func (t StandAloneStorageItem) ValueCopy(dst []byte) ([]byte, error) {
	return t.item.ValueCopy(dst)
}

func (it StandAloneStorageIterator) Item() engine_util.DBItem {
	return StandAloneStorageItem{it.it.Item(), it.cf}
}

func (it StandAloneStorageIterator) Valid() bool {
	return it.it.Valid()
}

func (it StandAloneStorageIterator) Next() {
	it.it.Next()
}

func (it StandAloneStorageIterator) Seek(b []byte) {
	it.it.Seek(engine_util.KeyWithCF(it.cf, b))
}

func (it StandAloneStorageIterator) Close() {
	it.it.Close()
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if val == nil {
		err = nil
	}
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return StandAloneStorageIterator{r.txn.NewIterator(badger.DefaultIteratorOptions), cf}
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		conf:    conf,
		engines: nil,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.engines = engine_util.NewEngines(
		engine_util.CreateDB(s.conf.DBPath, false), nil, s.conf.DBPath, "")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.engines.Destroy()
	s.engines = nil
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{s.engines.Kv.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var wb engine_util.WriteBatch
	for _, kv := range batch {
		wb.SetCF(kv.Cf(), kv.Key(), kv.Value())
	}
	err := s.engines.WriteKV(&wb)
	return err
}
