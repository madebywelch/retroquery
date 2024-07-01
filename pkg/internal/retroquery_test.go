package tests

import (
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

			// Test Insert
			initialTime := time.Now()
			err = rq.Insert("user:1", map[string]interface{}{"name": "Alice", "age": 30})
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}

			// Test QueryAtTime (current)
			data, exists, err := rq.QueryAtTime("user:1", time.Now())
			if err != nil {
				t.Fatalf("Failed to query data: %v", err)
			}
			if !exists {
				t.Fatal("Data should exist after insertion")
			}
			expectedData := map[string]interface{}{"name": "Alice", "age": 30}
			if !compareMapsVerbose(t, data, expectedData) {
				t.Fatalf("Unexpected data: got %v, want %v", data, expectedData)
			}

			// Test Update
			time.Sleep(10 * time.Millisecond) // Ensure different timestamp
			err = rq.Update("user:1", map[string]interface{}{"name": "Alice", "age": 31})
			if err != nil {
				t.Fatalf("Failed to update data: %v", err)
			}

			// Test QueryAtTime (updated)
			data, exists, err = rq.QueryAtTime("user:1", time.Now())
			if err != nil {
				t.Fatalf("Failed to query updated data: %v", err)
			}
			if !exists {
				t.Fatal("Updated data should exist")
			}
			expectedData = map[string]interface{}{"name": "Alice", "age": 31}
			if !compareMapsVerbose(t, data, expectedData) {
				t.Fatalf("Unexpected updated data: got %v, want %v", data, expectedData)
			}

			// Test QueryAtTime (past)
			pastTime := initialTime.Add(5 * time.Millisecond)
			data, exists, err = rq.QueryAtTime("user:1", pastTime)
			if err != nil {
				t.Fatalf("Failed to query past data: %v", err)
			}
			if !exists {
				t.Fatalf("Past data should exist. Query time: %v, Initial insert time: %v", pastTime, initialTime)
			}
			expectedData = map[string]interface{}{"name": "Alice", "age": 30}
			if !compareMapsVerbose(t, data, expectedData) {
				t.Fatalf("Unexpected past data: got %v, want %v", data, expectedData)
			}

			// Test Delete
			time.Sleep(10 * time.Millisecond) // Ensure different timestamp
			err = rq.Delete("user:1")
			if err != nil {
				t.Fatalf("Failed to delete data: %v", err)
			}

			// Test QueryAtTime (after deletion)
			data, exists, err = rq.QueryAtTime("user:1", time.Now())
			if err != nil {
				t.Fatalf("Failed to query deleted data: %v", err)
			}
			if exists {
				t.Fatalf("Data should not exist after deletion, but got: %v", data)
			}

			// Test QueryAtTime (non-existent key)
			data, exists, err = rq.QueryAtTime("user:2", time.Now())
			if err != nil {
				t.Fatalf("Failed to query non-existent data: %v", err)
			}
			if exists {
				t.Fatalf("Data should not exist for non-existent key, but got: %v", data)
			}
		})
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
}

func getTestName(config retroquery.Config) string {
	if config.InMemory {
		return "InMemory"
	}
	return "DiskBased"
}

// compareMapsVerbose compares two maps of string to interface{} for equality and logs detailed information
func compareMapsVerbose(t *testing.T, map1, map2 map[string]interface{}) bool {
	t.Logf("Comparing maps: %v and %v", map1, map2)
	if len(map1) != len(map2) {
		t.Logf("Maps have different lengths: %d vs %d", len(map1), len(map2))
		return false
	}
	for k, v1 := range map1 {
		v2, ok := map2[k]
		if !ok {
			t.Logf("Key %s not found in second map", k)
			return false
		}
		t.Logf("Comparing values for key %s: %v (type %T) vs %v (type %T)", k, v1, v1, v2, v2)
		if !compareValues(v1, v2) {
			t.Logf("Values for key %s are not equal: %v vs %v", k, v1, v2)
			return false
		}
	}
	return true
}

// compareValues compares two values, allowing for int-float comparisons
func compareValues(v1, v2 interface{}) bool {
	if reflect.TypeOf(v1) == reflect.TypeOf(v2) {
		return reflect.DeepEqual(v1, v2)
	}

	// Handle int-float comparisons
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
