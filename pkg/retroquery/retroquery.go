package retroquery

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
)

type Record struct {
	Data      map[string]interface{}
	Timestamp time.Time
}

type RetroQuery struct {
	data     map[string][]Record
	mu       sync.RWMutex
	db       *badger.DB
	inMemory bool
}

type Config struct {
	InMemory bool
	DataDir  string
}

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

func (rq *RetroQuery) Close() error {
	if rq.db != nil {
		return rq.db.Close()
	}
	return nil
}

func (rq *RetroQuery) Insert(key string, value map[string]interface{}) error {
	record := Record{
		Data:      value,
		Timestamp: time.Now(),
	}

	if rq.inMemory {
		rq.mu.Lock()
		rq.data[key] = append(rq.data[key], record)
		rq.mu.Unlock()
		return nil
	}

	return rq.db.Update(func(txn *badger.Txn) error {
		recordBytes, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return txn.Set([]byte(fmt.Sprintf("%s_%s", key, record.Timestamp.Format(time.RFC3339Nano))), recordBytes)
	})
}

func (rq *RetroQuery) Update(key string, value map[string]interface{}) error {
	return rq.Insert(key, value)
}

func (rq *RetroQuery) Delete(key string) error {
	return rq.Insert(key, map[string]interface{}{"_deleted": true})
}

func (rq *RetroQuery) QueryAtTime(key string, timestamp time.Time) (map[string]interface{}, bool, error) {
	log.Printf("QueryAtTime called with key: %s, timestamp: %v", key, timestamp)

	if rq.inMemory {
		rq.mu.RLock()
		defer rq.mu.RUnlock()

		records, exists := rq.data[key]
		if !exists {
			log.Printf("No records found for key: %s", key)
			return nil, false, nil
		}

		sort.Slice(records, func(i, j int) bool {
			return records[i].Timestamp.After(records[j].Timestamp)
		})

		for _, record := range records {
			log.Printf("Checking record with timestamp: %v", record.Timestamp)
			if record.Timestamp.Before(timestamp) || record.Timestamp.Equal(timestamp) {
				if _, deleted := record.Data["_deleted"]; deleted {
					log.Printf("Found deleted record")
					return nil, false, nil
				}
				log.Printf("Found matching record: %v", record.Data)
				return record.Data, true, nil
			}
		}
		log.Printf("No matching records found")
		return nil, false, nil
	}

	var result Record
	var found bool
	err := rq.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(key + "_")
		log.Printf("Seeking with prefix: %s", prefix)
		for it.Seek(append(prefix, []byte(timestamp.Format(time.RFC3339Nano))...)); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			log.Printf("Examining item with key: %s", item.Key())
			err := item.Value(func(v []byte) error {
				return json.Unmarshal(v, &result)
			})
			if err != nil {
				return err
			}
			log.Printf("Unmarshaled record with timestamp: %v", result.Timestamp)
			if result.Timestamp.After(timestamp) {
				log.Printf("Record is after query timestamp, continuing")
				continue
			}
			found = true
			log.Printf("Found matching record: %v", result.Data)
			return nil
		}
		return nil
	})

	if err != nil {
		log.Printf("Error during query: %v", err)
		return nil, false, err
	}
	if !found || result.Data == nil {
		log.Printf("No matching record found")
		return nil, false, nil
	}
	if _, deleted := result.Data["_deleted"]; deleted {
		log.Printf("Found deleted record")
		return nil, false, nil
	}
	log.Printf("Returning record: %v", result.Data)
	return result.Data, true, nil
}

func (rq *RetroQuery) Backup(dst string) error {
	if rq.inMemory {
		return fmt.Errorf("backup not supported for in-memory database")
	}

	f, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer f.Close()

	_, err = rq.db.Backup(f, 0)
	if err != nil {
		return fmt.Errorf("failed to backup database: %w", err)
	}

	return nil
}
