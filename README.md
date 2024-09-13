# RetroQuery

RetroQuery is a high-performance, time-travel database implementation in Go. It supports both in-memory and disk-based storage, allowing for flexible usage in various scenarios.

## Features

- Time-travel querying: retrieve data as it existed at any point in time
- Support for both in-memory and disk-based storage
- High-performance concurrent operations
- Simple and intuitive API

## Installation

To install RetroQuery, use `go get`:

```bash
go get github.com/madebywelch/retroquery
```

## Usage

Here's a quick example of how to use RetroQuery:

```go
package main

import (
	"fmt"
	"time"

	"github.com/madebywelch/retroquery/pkg/retroquery"
)

func main() {
	// Create a new RetroQuery instance
	rq, err := retroquery.New(retroquery.Config{
		InMemory: true, // Use in-memory storage
	})
	if err != nil {
		panic(err)
	}
	defer rq.Close()

	// Insert initial data
	err = rq.Insert("user:1", map[string]interface{}{
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		panic(err)
	}

	// Wait a bit to ensure a different timestamp
	time.Sleep(100 * time.Millisecond)

	// Query the current state
	data, exists, err := rq.QueryAtTime("user:1", time.Now())
	if err != nil {
		panic(err)
	}
	if exists {
		fmt.Printf("Current data: %v\n", data)
	}

	// Update the data
	err = rq.Update("user:1", map[string]interface{}{
		"name": "Alice",
		"age":  31,
	})
	if err != nil {
		panic(err)
	}

	// Wait a bit to ensure a different timestamp
	time.Sleep(100 * time.Millisecond)

	// Query the past state (before the update)
	pastTime := time.Now().Add(-150 * time.Millisecond)
	data, exists, err = rq.QueryAtTime("user:1", pastTime)
	if err != nil {
		panic(err)
	}
	if exists {
		fmt.Printf("Past data: %v\n", data)
	}

	// Query the current state again
	data, exists, err = rq.QueryAtTime("user:1", time.Now())
	if err != nil {
		panic(err)
	}
	if exists {
		fmt.Printf("Updated current data: %v\n", data)
	}
}
```

RetroQuery is released under the MIT License. See the LICENSE file for details.
