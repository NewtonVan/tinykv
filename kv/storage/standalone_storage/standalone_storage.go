package standalone_storage

import (
	"path/filepath"

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
	engines *badger.DB
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		engines: nil,
		config:  conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	dbPath := s.config.DBPath
	kvPath := filepath.Join(dbPath, "kv")

	kvDB := engine_util.CreateDB(kvPath, false)
	s.engines = kvDB

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engines.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneReader(s.engines.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			engine_util.PutCF(s.engines, data.Cf, data.Key, data.Value)
		case storage.Delete:
			engine_util.DeleteCF(s.engines, data.Cf, data.Key)
		}
	}
	return nil
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if badger.ErrKeyNotFound == err {
		return nil, nil
	}

	return val, nil
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneReader) Close() {
	s.txn.Discard()
}

func NewStandAloneReader(txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		txn: txn,
	}
}
