package retroquery_test

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/madebywelch/retroquery/pkg/retroquery"
)

func TestRetroQuery_InMemory(t *testing.T) {
	rq, err := retroquery.New(retroquery.Config{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create RetroQuery instance: %v", err)
	}
	defer rq.Close()

	key := "user1"
	initialData := map[string]interface{}{"name": "Alice"}

	// Insert initial data
	err = rq.Insert(key, initialData)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	time.Sleep(10 * time.Millisecond) // Ensure different timestamps

	// Update data
	updatedData := map[string]interface{}{"name": "Alice Smith"}
	err = rq.Insert(key, updatedData)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Delete data
	err = rq.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Query at different timestamps
	timestamps := []struct {
		time   time.Time
		exists bool
		data   map[string]interface{}
	}{
		{time.Now().Add(-30 * time.Millisecond), true, initialData},
		{time.Now().Add(-20 * time.Millisecond), true, updatedData},
		{time.Now().Add(-10 * time.Millisecond), false, nil},
	}

	for _, ts := range timestamps {
		data, found, err := rq.QueryAtTime(key, ts.time)
		if err != nil {
			t.Fatalf("QueryAtTime failed: %v", err)
		}
		if found != ts.exists {
			t.Fatalf("Expected existence: %v, got: %v", ts.exists, found)
		}
		if found && fmt.Sprintf("%v", data) != fmt.Sprintf("%v", ts.data) {
			t.Fatalf("Expected data: %v, got: %v", ts.data, data)
		}
	}
}

func TestRetroQuery_OnDisk(t *testing.T) {
	rq, err := retroquery.New(retroquery.Config{InMemory: false, DataDir: "./testdata"})
	if err != nil {
		t.Fatalf("Failed to create RetroQuery instance: %v", err)
	}
	defer func() {
		rq.Close()
		os.RemoveAll("./testdata")
	}()

	key := "user2"
	initialData := map[string]interface{}{"name": "Bob"}

	// Insert initial data
	err = rq.Insert(key, initialData)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Update data
	updatedData := map[string]interface{}{"name": "Bob Johnson"}
	err = rq.Insert(key, updatedData)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Query at different timestamps
	timestamps := []struct {
		time   time.Time
		exists bool
		data   map[string]interface{}
	}{
		{time.Now().Add(-20 * time.Millisecond), true, initialData},
		{time.Now().Add(-10 * time.Millisecond), true, updatedData},
	}

	for _, ts := range timestamps {
		data, found, err := rq.QueryAtTime(key, ts.time)
		if err != nil {
			t.Fatalf("QueryAtTime failed: %v", err)
		}
		if found != ts.exists {
			t.Fatalf("Expected existence: %v, got: %v", ts.exists, found)
		}
		if found && fmt.Sprintf("%v", data) != fmt.Sprintf("%v", ts.data) {
			t.Fatalf("Expected data: %v, got: %v", ts.data, data)
		}
	}
}

func TestRetroQuery_BatchInsert(t *testing.T) {
	rq, err := retroquery.New(retroquery.Config{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create RetroQuery instance: %v", err)
	}
	defer rq.Close()

	records := map[string]map[string]interface{}{
		"user3": {"name": "Charlie"},
		"user4": {"name": "Diana"},
	}

	err = rq.BatchInsert(records)
	if err != nil {
		t.Fatalf("BatchInsert failed: %v", err)
	}

	for key, expectedData := range records {
		data, found, err := rq.QueryAtTime(key, time.Now())
		if err != nil {
			t.Fatalf("QueryAtTime failed: %v", err)
		}
		if !found {
			t.Fatalf("Expected to find data for key: %s", key)
		}
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", expectedData) {
			t.Fatalf("Expected data: %v, got: %v", expectedData, data)
		}
	}
}

func TestRetroQuery_QueryRange(t *testing.T) {
	rq, err := retroquery.New(retroquery.Config{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create RetroQuery instance: %v", err)
	}
	defer rq.Close()

	key := "user5"
	dataPoints := []map[string]interface{}{
		{"status": "active"},
		{"status": "inactive"},
		{"status": "active"},
	}

	var timestamps []time.Time
	for _, data := range dataPoints {
		err = rq.Insert(key, data)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		timestamps = append(timestamps, time.Now())
		time.Sleep(10 * time.Millisecond)
	}

	start := timestamps[0]
	end := timestamps[2]

	records, err := rq.QueryRange(key, start, end)
	if err != nil {
		t.Fatalf("QueryRange failed: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("Expected 3 records, got: %d", len(records))
	}
}

func TestRetroQuery_Delete(t *testing.T) {
	rq, err := retroquery.New(retroquery.Config{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create RetroQuery instance: %v", err)
	}
	defer rq.Close()

	key := "user6"
	data := map[string]interface{}{"name": "Eve"}

	err = rq.Insert(key, data)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	err = rq.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Query after deletion
	queriedData, found, err := rq.QueryAtTime(key, time.Now())
	if err != nil {
		t.Fatalf("QueryAtTime failed: %v", err)
	}
	if found {
		t.Fatalf("Expected data to be deleted, but found: %v", queriedData)
	}
}

func TestRetroQuery_Concurrency(t *testing.T) {
	rq, err := retroquery.New(retroquery.Config{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create RetroQuery instance: %v", err)
	}
	defer rq.Close()

	key := "user7"

	// Insert data concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := rq.Insert(key, map[string]interface{}{"value": i})
			if err != nil {
				t.Errorf("Insert failed: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Query the latest data
	_, found, err := rq.QueryAtTime(key, time.Now())
	if err != nil {
		t.Fatalf("QueryAtTime failed: %v", err)
	}
	if !found {
		t.Fatalf("Expected to find data for key: %s", key)
	}
}

func TestRetroQuery_TimeTravel(t *testing.T) {
	rq, err := retroquery.New(retroquery.Config{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to create RetroQuery instance: %v", err)
	}
	defer rq.Close()

	key := "user8"
	history := []struct {
		data map[string]interface{}
		time time.Time
	}{
		{map[string]interface{}{"status": "online"}, time.Now()},
		{map[string]interface{}{"status": "offline"}, time.Now().Add(10 * time.Millisecond)},
		{map[string]interface{}{"status": "away"}, time.Now().Add(20 * time.Millisecond)},
	}

	for _, entry := range history {
		err := rq.InsertAtTime(key, entry.data, entry.time)
		if err != nil {
			t.Fatalf("InsertAtTime failed: %v", err)
		}
	}

	// Query at different times
	for _, entry := range history {
		data, found, err := rq.QueryAtTime(key, entry.time)
		if err != nil {
			t.Fatalf("QueryAtTime failed: %v", err)
		}
		if !found {
			t.Fatalf("Expected to find data at time: %v", entry.time)
		}
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", entry.data) {
			t.Fatalf("Expected data: %v, got: %v", entry.data, data)
		}
	}
}
