package retroquery

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
)

type Record struct {
	Timestamp int64
	Data      map[string]interface{}
}

type RetroQuery struct {
	data     map[string][]Record // In-memory storage: key -> sorted slice of Records
	mu       sync.RWMutex
	db       *badger.DB
	inMemory bool
}

type Config struct {
	InMemory bool
	DataDir  string
}

// New initializes a new RetroQuery instance.
func New(config Config) (*RetroQuery, error) {
	rq := &RetroQuery{
		data:     make(map[string][]Record),
		inMemory: config.InMemory,
	}

	if !config.InMemory {
		opts := badger.DefaultOptions(config.DataDir)
		db, err := badger.Open(opts)
		if err != nil {
			return nil, fmt.Errorf("failed to open Badger database: %w", err)
		}
		rq.db = db
	}

	return rq, nil
}

// Close closes the RetroQuery instance.
func (rq *RetroQuery) Close() error {
	if rq.db != nil {
		return rq.db.Close()
	}
	return nil
}

// Insert inserts a record with the current timestamp.
func (rq *RetroQuery) Insert(key string, value map[string]interface{}) error {
	return rq.InsertAtTime(key, value, time.Now())
}

// InsertAtTime inserts a record with a specified timestamp.
func (rq *RetroQuery) InsertAtTime(key string, value map[string]interface{}, timestamp time.Time) error {
	ts := timestamp.UnixNano()
	record := Record{
		Timestamp: ts,
		Data:      value,
	}

	if rq.inMemory {
		rq.insertInMemory(key, record)
		return nil
	}

	return rq.insertOnDisk(key, record)
}

// insertInMemory inserts a record into the in-memory storage.
func (rq *RetroQuery) insertInMemory(key string, record Record) {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	records := rq.data[key]
	// Find the insertion point using binary search
	idx := sort.Search(len(records), func(i int) bool {
		return records[i].Timestamp >= record.Timestamp
	})
	if idx < len(records) && records[idx].Timestamp == record.Timestamp {
		records[idx] = record
	} else {
		records = append(records, Record{})
		copy(records[idx+1:], records[idx:])
		records[idx] = record
	}
	rq.data[key] = records
}

// insertOnDisk inserts a record into the BadgerDB.
func (rq *RetroQuery) insertOnDisk(key string, record Record) error {
	keyBytes := encodeKey(key, record.Timestamp)
	recordBytes, err := json.Marshal(record.Data)
	if err != nil {
		return err
	}

	return rq.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyBytes, recordBytes)
	})
}

// encodeKey encodes the key and timestamp into a byte slice.
func encodeKey(key string, timestamp int64) []byte {
	buf := make([]byte, len(key)+8)
	copy(buf, key)
	binary.BigEndian.PutUint64(buf[len(key):], uint64(timestamp))
	return buf
}

// QueryAtTime queries the record at a specific timestamp.
func (rq *RetroQuery) QueryAtTime(key string, timestamp time.Time) (map[string]interface{}, bool, error) {
	ts := timestamp.UnixNano()

	if rq.inMemory {
		return rq.queryInMemory(key, ts)
	}

	return rq.queryOnDisk(key, ts)
}

// queryInMemory queries the in-memory storage for a record at a specific timestamp.
func (rq *RetroQuery) queryInMemory(key string, ts int64) (map[string]interface{}, bool, error) {
	rq.mu.RLock()
	defer rq.mu.RUnlock()

	records := rq.data[key]
	idx := sort.Search(len(records), func(i int) bool {
		return records[i].Timestamp > ts
	})
	if idx == 0 {
		return nil, false, nil
	}
	record := records[idx-1]
	if record.Data == nil {
		return nil, false, nil
	}
	return record.Data, true, nil
}

// queryOnDisk queries the BadgerDB for a record at a specific timestamp.
func (rq *RetroQuery) queryOnDisk(key string, ts int64) (map[string]interface{}, bool, error) {
	prefix := []byte(key)
	seekKey := encodeKey(key, ts)
	var data map[string]interface{}

	err := rq.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(seekKey); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &data)
			}); err != nil {
				return err
			}
			return nil
		}
		return nil
	})

	if err != nil {
		return nil, false, err
	}
	if data == nil {
		return nil, false, nil
	}
	return data, true, nil
}

// Delete marks a key as deleted at the current timestamp.
func (rq *RetroQuery) Delete(key string) error {
	return rq.Insert(key, nil)
}

// QueryRange queries records within a specific time range.
func (rq *RetroQuery) QueryRange(key string, start, end time.Time) ([]map[string]interface{}, error) {
	if rq.inMemory {
		return rq.queryRangeInMemory(key, start.UnixNano(), end.UnixNano())
	}

	return rq.queryRangeOnDisk(key, start.UnixNano(), end.UnixNano())
}

// queryRangeInMemory queries the in-memory storage for records within a time range.
func (rq *RetroQuery) queryRangeInMemory(key string, startTs, endTs int64) ([]map[string]interface{}, error) {
	rq.mu.RLock()
	defer rq.mu.RUnlock()

	records := rq.data[key]
	var results []map[string]interface{}

	idxStart := sort.Search(len(records), func(i int) bool {
		return records[i].Timestamp >= startTs
	})

	idxEnd := sort.Search(len(records), func(i int) bool {
		return records[i].Timestamp > endTs
	})

	for i := idxStart; i < idxEnd; i++ {
		record := records[i]
		if record.Data != nil {
			results = append(results, record.Data)
		}
	}

	return results, nil
}

// queryRangeOnDisk queries the BadgerDB for records within a time range.
func (rq *RetroQuery) queryRangeOnDisk(key string, startTs, endTs int64) ([]map[string]interface{}, error) {
	prefix := []byte(key)
	startKey := encodeKey(key, startTs)
	endKey := encodeKey(key, endTs)

	var results []map[string]interface{}

	err := rq.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if bytes.Compare(k, endKey) > 0 {
				break
			}
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			var data map[string]interface{}
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &data)
			}); err != nil {
				return err
			}
			if data != nil {
				results = append(results, data)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}

// BatchInsert inserts multiple records at the current timestamp.
func (rq *RetroQuery) BatchInsert(records map[string]map[string]interface{}) error {
	timestamp := time.Now()
	if rq.inMemory {
		for key, value := range records {
			if err := rq.InsertAtTime(key, value, timestamp); err != nil {
				return err
			}
		}
		return nil
	}

	return rq.db.Update(func(txn *badger.Txn) error {
		for key, value := range records {
			recordBytes, err := json.Marshal(value)
			if err != nil {
				return err
			}
			keyBytes := encodeKey(key, timestamp.UnixNano())
			if err := txn.Set(keyBytes, recordBytes); err != nil {
				return err
			}
		}
		return nil
	})
}
