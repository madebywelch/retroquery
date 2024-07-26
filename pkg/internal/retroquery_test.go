package tests

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/madebywelch/retroquery/pkg/retroquery"
)

func TestRetroQuery(t *testing.T) {
	configs := []retroquery.Config{
		{InMemory: true},
		{InMemory: false, DataDir: "test_data"},
		{InMemory: true, Compress: true},
		{InMemory: false, DataDir: "test_data_compressed", Compress: true},
	}

	for _, config := range configs {
		t.Run(getTestName(config), func(t *testing.T) {
			rq, err := retroquery.New(config)
			if err != nil {
				t.Fatalf("Failed to create RetroQuery: %v", err)
			}
			defer func() {
				rq.Close()
				if !config.InMemory {
					os.RemoveAll(config.DataDir)
				}
			}()

			t.Run("BasicOperations", func(t *testing.T) {
				testBasicOperations(t, rq)
			})

			t.Run("QueryRange", func(t *testing.T) {
				testQueryRange(t, rq)
			})

			t.Run("BatchInsert", func(t *testing.T) {
				testBatchInsert(t, rq)
			})

			t.Run("TTL", func(t *testing.T) {
				testTTL(t, rq)
			})

			t.Run("ConcurrentOperations", func(t *testing.T) {
				testConcurrentOperations(t, rq)
			})

			t.Run("LargeDataset", func(t *testing.T) {
				testLargeDataset(t, rq)
			})
		})
	}
}

func testBasicOperations(t *testing.T, rq *retroquery.RetroQuery) {
	// Test Insert
	initialTime := time.Now()
	err := rq.Insert("user:1", map[string]interface{}{"name": "Alice", "age": 30})
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test QueryAtTime (current)
	data, exists, err := rq.QueryAtTime("user:1", time.Now())
	if err != nil || !exists {
		t.Fatalf("Failed to query data: error=%v, exists=%v", err, exists)
	}
	assertEqualMaps(t, data, map[string]interface{}{"name": "Alice", "age": 30})

	// Test Update
	time.Sleep(10 * time.Millisecond)
	err = rq.Update("user:1", map[string]interface{}{"name": "Alice", "age": 31})
	if err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}

	// Test QueryAtTime (updated)
	data, exists, err = rq.QueryAtTime("user:1", time.Now())
	if err != nil || !exists {
		t.Fatalf("Failed to query updated data: error=%v, exists=%v", err, exists)
	}
	assertEqualMaps(t, data, map[string]interface{}{"name": "Alice", "age": 31})

	// Test QueryAtTime (past)
	pastTime := initialTime.Add(5 * time.Millisecond)
	data, exists, err = rq.QueryAtTime("user:1", pastTime)
	if err != nil || !exists {
		t.Fatalf("Failed to query past data: error=%v, exists=%v", err, exists)
	}
	assertEqualMaps(t, data, map[string]interface{}{"name": "Alice", "age": 30})

	// Test Delete
	time.Sleep(10 * time.Millisecond)
	err = rq.Delete("user:1")
	if err != nil {
		t.Fatalf("Failed to delete data: %v", err)
	}

	// Test QueryAtTime (after deletion)
	data, exists, err = rq.QueryAtTime("user:1", time.Now())
	if err != nil || exists {
		t.Fatalf("Data should not exist after deletion: error=%v, exists=%v, data=%v", err, exists, data)
	}

	// Test QueryAtTime (non-existent key)
	data, exists, err = rq.QueryAtTime("user:2", time.Now())
	if err != nil || exists {
		t.Fatalf("Data should not exist for non-existent key: error=%v, exists=%v, data=%v", err, exists, data)
	}
}

func testQueryRange(t *testing.T, rq *retroquery.RetroQuery) {
	// Insert test data
	start := time.Now()
	for i := 0; i < 5; i++ {
		err := rq.Insert("user:1", map[string]interface{}{"count": i})
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	end := time.Now()

	// Query range
	results, err := rq.QueryRange("user:1", start, end)
	if err != nil {
		t.Fatalf("Failed to query range: %v", err)
	}

	if len(results) != 5 {
		t.Fatalf("Expected 5 results, got %d", len(results))
	}

	expectedCounts := []int{4, 3, 2, 1, 0}
	for i, result := range results {
		expectedCount := expectedCounts[i]
		count, ok := result["count"].(int)
		if !ok {
			// If it's not an int, let's check if it's a float64 that can be converted to an int
			if floatCount, isFloat := result["count"].(float64); isFloat {
				count = int(floatCount)
				ok = true
			}
		}

		if !ok {
			t.Errorf("Expected count to be an int or a float64 convertible to int, got %T", result["count"])
		} else if count != expectedCount {
			t.Errorf("Expected count %d, got %d", expectedCount, count)
		}
	}
}

func testBatchInsert(t *testing.T, rq *retroquery.RetroQuery) {
	batch := make(map[string]map[string]interface{})
	for i := 0; i < 100; i++ {
		batch[fmt.Sprintf("user:%d", i)] = map[string]interface{}{"name": fmt.Sprintf("User%d", i), "age": 20 + i%50}
	}

	err := rq.BatchInsert(batch)
	if err != nil {
		t.Fatalf("Failed to batch insert: %v", err)
	}

	// Verify inserted data
	for key, value := range batch {
		data, exists, err := rq.QueryAtTime(key, time.Now())
		if err != nil || !exists {
			t.Fatalf("Failed to query batch inserted data for key %s: error=%v, exists=%v", key, err, exists)
		}
		assertEqualMaps(t, data, value)
	}
}

func testTTL(t *testing.T, rq *retroquery.RetroQuery) {
	err := rq.InsertWithTTL("ttl_test", map[string]interface{}{"temp": true}, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to insert data with TTL: %v", err)
	}

	// Data should exist immediately
	_, exists, err := rq.QueryAtTime("ttl_test", time.Now())
	if err != nil || !exists {
		t.Fatalf("Data with TTL should exist immediately: error=%v, exists=%v", err, exists)
	}

	// Wait for TTL to expire
	time.Sleep(200 * time.Millisecond)

	// Data should no longer exist
	_, exists, err = rq.QueryAtTime("ttl_test", time.Now())
	if err != nil || exists {
		t.Fatalf("Data with TTL should no longer exist: error=%v, exists=%v", err, exists)
	}
}

func testConcurrentOperations(t *testing.T, rq *retroquery.RetroQuery) {
	const numOps = 1000
	errCh := make(chan error, numOps*2)

	for i := 0; i < numOps; i++ {
		go func(i int) {
			key := fmt.Sprintf("concurrent:%d", i)
			errCh <- rq.Insert(key, map[string]interface{}{"value": i})
			_, _, err := rq.QueryAtTime(key, time.Now())
			errCh <- err
		}(i)
	}

	for i := 0; i < numOps*2; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("Concurrent operation failed: %v", err)
		}
	}
}

func testLargeDataset(t *testing.T, rq *retroquery.RetroQuery) {
	const numRecords = 10000
	startTime := time.Now()

	for i := 0; i < numRecords; i++ {
		err := rq.Insert(fmt.Sprintf("large:%d", i), map[string]interface{}{"data": string(make([]byte, 1000))})
		if err != nil {
			t.Fatalf("Failed to insert large dataset: %v", err)
		}
	}

	endTime := time.Now()
	t.Logf("Inserted %d records in %v", numRecords, endTime.Sub(startTime))

	// Query a random record
	key := fmt.Sprintf("large:%d", numRecords/2)
	_, exists, err := rq.QueryAtTime(key, time.Now())
	if err != nil || !exists {
		t.Fatalf("Failed to query large dataset: error=%v, exists=%v", err, exists)
	}
}

func TestBackup(t *testing.T) {
	config := retroquery.Config{InMemory: false, DataDir: "test_data"}
	rq, err := retroquery.New(config)
	if err != nil {
		t.Fatalf("Failed to create RetroQuery: %v", err)
	}
	defer func() {
		rq.Close()
		os.RemoveAll(config.DataDir)
	}()

	// Insert some data
	err = rq.Insert("user:1", map[string]interface{}{"name": "Alice", "age": 30})
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Perform backup
	backupPath := "test_backup"
	err = rq.Backup(backupPath)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}
	defer os.Remove(backupPath)

	// Check if backup file exists
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		t.Fatal("Backup file does not exist")
	}

	// TODO: Add test for restoring from backup
}

func getTestName(config retroquery.Config) string {
	name := "InMemory"
	if !config.InMemory {
		name = "DiskBased"
	}
	if config.Compress {
		name += "Compressed"
	}
	return name
}

func assertEqualMaps(t *testing.T, got, want map[string]interface{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("Map lengths not equal: got %d, want %d", len(got), len(want))
	}
	for k, wantV := range want {
		gotV, ok := got[k]
		if !ok {
			t.Fatalf("Key %q not found in got map", k)
		}
		if !compareValues(gotV, wantV) {
			t.Fatalf("Values not equal for key %q: got %v (%T), want %v (%T)", k, gotV, gotV, wantV, wantV)
		}
	}
}

func compareValues(v1, v2 interface{}) bool {
	if reflect.TypeOf(v1) == reflect.TypeOf(v2) {
		return reflect.DeepEqual(v1, v2)
	}

	switch v1.(type) {
	case int:
		if v2, ok := v2.(float64); ok {
			return float64(v1.(int)) == v2
		}
	case float64:
		if v2, ok := v2.(int); ok {
			return v1.(float64) == float64(v2)
		}
	}

	return false
}
