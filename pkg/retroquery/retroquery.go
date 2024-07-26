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
	"github.com/klauspost/compress/zstd"
)

type Record struct {
	Data      map[string]interface{}
	Timestamp time.Time
	TTL       time.Duration
}

type RetroQuery struct {
	data     map[string][]Record
	mu       sync.RWMutex
	db       *badger.DB
	inMemory bool
	compress bool
	encoder  *zstd.Encoder
	decoder  *zstd.Decoder
}

type Config struct {
	InMemory bool
	DataDir  string
	Compress bool
}

func New(config Config) (*RetroQuery, error) {
	rq := &RetroQuery{
		data:     make(map[string][]Record),
		inMemory: config.InMemory,
		compress: config.Compress,
	}

	if config.Compress {
		var err error
		rq.encoder, err = zstd.NewWriter(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
		}
		rq.decoder, err = zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
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
	if rq.compress {
		rq.encoder.Close()
		rq.decoder.Close()
	}
	return nil
}

func (rq *RetroQuery) Insert(key string, value map[string]interface{}) error {
	return rq.InsertWithTTL(key, value, 0)
}

func (rq *RetroQuery) InsertWithTTL(key string, value map[string]interface{}, ttl time.Duration) error {
	return rq.insert(key, value, time.Now(), ttl)
}

func (rq *RetroQuery) insert(key string, value map[string]interface{}, timestamp time.Time, ttl time.Duration) error {
	record := Record{
		Data:      value,
		Timestamp: timestamp,
		TTL:       ttl,
	}

	if rq.inMemory {
		rq.mu.Lock()
		defer rq.mu.Unlock()

		records, ok := rq.data[key]
		if !ok {
			records = []Record{}
		}
		records = append(records, record)
		sort.Slice(records, func(i, j int) bool {
			return records[i].Timestamp.After(records[j].Timestamp)
		})
		rq.data[key] = records
		log.Printf("Inserted record for key: %s, timestamp: %v, records count: %d", key, timestamp, len(records))
		return nil
	}

	return rq.db.Update(func(txn *badger.Txn) error {
		recordBytes, err := json.Marshal(record)
		if err != nil {
			return err
		}

		if rq.compress {
			recordBytes = rq.encoder.EncodeAll(recordBytes, nil)
		}

		entry := badger.NewEntry([]byte(fmt.Sprintf("%s_%s", key, record.Timestamp.Format(time.RFC3339Nano))), recordBytes)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
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

		log.Printf("Found records for key: %s, count: %d", key, len(records))

		for _, record := range records {
			log.Printf("Examining record with timestamp: %v", record.Timestamp)
			if record.Timestamp.After(timestamp) {
				continue
			}
			if record.TTL > 0 && time.Since(record.Timestamp) > record.TTL {
				continue // Skip expired records
			}
			if _, deleted := record.Data["_deleted"]; deleted {
				log.Printf("Found deleted record")
				return nil, false, nil
			}
			log.Printf("Found matching record: %v", record.Data)
			return record.Data, true, nil
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
		for it.Seek(append(prefix, []byte(timestamp.Format(time.RFC3339Nano))...)); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			err := item.Value(func(v []byte) error {
				if rq.compress {
					decoded, err := rq.decoder.DecodeAll(v, nil)
					if err != nil {
						return err
					}
					v = decoded
				}
				return json.Unmarshal(v, &result)
			})
			if err != nil {
				return err
			}
			if result.Timestamp.After(timestamp) {
				continue
			}
			found = true
			return nil
		}
		return nil
	})

	if err != nil {
		return nil, false, err
	}
	if !found || result.Data == nil {
		return nil, false, nil
	}
	if _, deleted := result.Data["_deleted"]; deleted {
		return nil, false, nil
	}
	return result.Data, true, nil
}

func (rq *RetroQuery) BatchInsert(records map[string]map[string]interface{}) error {
	timestamp := time.Now()
	if rq.inMemory {
		for key, value := range records {
			if err := rq.insert(key, value, timestamp, 0); err != nil {
				return err
			}
		}
		return nil
	}

	return rq.db.Update(func(txn *badger.Txn) error {
		for key, value := range records {
			record := Record{
				Data:      value,
				Timestamp: timestamp,
			}
			recordBytes, err := json.Marshal(record)
			if err != nil {
				return err
			}

			if rq.compress {
				recordBytes = rq.encoder.EncodeAll(recordBytes, nil)
			}

			if err := txn.Set([]byte(fmt.Sprintf("%s_%s", key, record.Timestamp.Format(time.RFC3339Nano))), recordBytes); err != nil {
				return err
			}
		}
		return nil
	})
}

func (rq *RetroQuery) QueryRange(key string, start, end time.Time) ([]map[string]interface{}, error) {
	log.Printf("QueryRange called with key: %s, start: %v, end: %v", key, start, end)

	var results []map[string]interface{}

	if rq.inMemory {
		rq.mu.RLock()
		defer rq.mu.RUnlock()

		records, exists := rq.data[key]
		if !exists {
			log.Printf("No records found for key: %s", key)
			return results, nil
		}

		for _, record := range records {
			if record.Timestamp.Before(start) {
				continue
			}
			if record.Timestamp.After(end) {
				break
			}
			if _, deleted := record.Data["_deleted"]; !deleted {
				results = append([]map[string]interface{}{record.Data}, results...)
			}
		}

		for i := 0; i < len(results)/2; i++ {
			j := len(results) - 1 - i
			results[i], results[j] = results[j], results[i]
		}

		return results, nil
	}

	err := rq.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(key + "_")
		for it.Seek(append(prefix, []byte(end.Format(time.RFC3339Nano))...)); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			var record Record
			err := item.Value(func(v []byte) error {
				if rq.compress {
					decoded, err := rq.decoder.DecodeAll(v, nil)
					if err != nil {
						return err
					}
					v = decoded
				}
				return json.Unmarshal(v, &record)
			})
			if err != nil {
				return err
			}

			if record.Timestamp.Before(start) {
				break
			}
			if record.Timestamp.After(end) {
				continue
			}

			if _, deleted := record.Data["_deleted"]; !deleted {
				results = append(results, record.Data)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}

func (rq *RetroQuery) Compact() error {
	if rq.inMemory {
		return fmt.Errorf("compact operation not supported for in-memory storage")
	}

	return rq.db.RunValueLogGC(0.5)
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
