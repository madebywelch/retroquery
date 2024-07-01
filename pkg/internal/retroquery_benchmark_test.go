package tests

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/madebywelch/retroquery/pkg/retroquery"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
)

func BenchmarkRetroQueryRPS(b *testing.B) {
	printSystemInfo()

	configs := []retroquery.Config{
		{InMemory: true},
		{InMemory: false, DataDir: "benchmark_data"},
	}

	for _, config := range configs {
		b.Run(fmt.Sprintf("Config=%v", config.InMemory), func(b *testing.B) {
			rq, err := retroquery.New(config)
			if err != nil {
				b.Fatalf("Failed to create RetroQuery: %v", err)
			}
			defer func() {
				rq.Close()
				if !config.InMemory {
					os.RemoveAll(config.DataDir)
				}
			}()

			// Prepare some data
			for i := 0; i < 10000; i++ {
				err := rq.Insert(fmt.Sprintf("key:%d", i), map[string]interface{}{
					"name": fmt.Sprintf("User%d", i),
					"age":  rand.Intn(100),
				})
				if err != nil {
					b.Fatalf("Failed to insert data: %v", err)
				}
			}

			// Measure RPS
			duration := 10 * time.Second
			var ops uint64 = 0
			var wg sync.WaitGroup
			start := time.Now()

			for i := 0; i < runtime.NumCPU(); i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for time.Since(start) < duration {
						key := fmt.Sprintf("key:%d", rand.Intn(10000))
						_, _, err := rq.QueryAtTime(key, time.Now())
						if err != nil {
							b.Fatalf("Failed to query data: %v", err)
						}
						atomic.AddUint64(&ops, 1)
					}
				}()
			}

			wg.Wait()
			elapsed := time.Since(start)
			rps := float64(ops) / elapsed.Seconds()

			b.ReportMetric(rps, "rps")
			fmt.Printf("Config=%v: %.2f requests per second\n", config.InMemory, rps)
		})
	}
}

func BenchmarkConcurrentInsertAndQueryRPS(b *testing.B) {
	printSystemInfo()

	configs := []retroquery.Config{
		{InMemory: true},
		{InMemory: false, DataDir: "benchmark_data"},
	}

	for _, config := range configs {
		b.Run(fmt.Sprintf("Config=%v", config.InMemory), func(b *testing.B) {
			rq, err := retroquery.New(config)
			if err != nil {
				b.Fatalf("Failed to create RetroQuery: %v", err)
			}
			defer func() {
				rq.Close()
				if !config.InMemory {
					os.RemoveAll(config.DataDir)
				}
			}()

			// Measure RPS
			duration := 10 * time.Second
			var ops uint64 = 0
			var wg sync.WaitGroup
			start := time.Now()

			for i := 0; i < runtime.NumCPU(); i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for time.Since(start) < duration {
						key := fmt.Sprintf("key:%d", rand.Intn(10000))
						if rand.Float32() < 0.2 { // 20% inserts, 80% queries
							err := rq.Insert(key, map[string]interface{}{
								"name": fmt.Sprintf("User%d", rand.Intn(10000)),
								"age":  rand.Intn(100),
							})
							if err != nil {
								b.Fatalf("Failed to insert data: %v", err)
							}
						} else {
							_, _, err := rq.QueryAtTime(key, time.Now())
							if err != nil {
								b.Fatalf("Failed to query data: %v", err)
							}
						}
						atomic.AddUint64(&ops, 1)
					}
				}()
			}

			wg.Wait()
			elapsed := time.Since(start)
			rps := float64(ops) / elapsed.Seconds()

			b.ReportMetric(rps, "rps")
			fmt.Printf("Config=%v: %.2f requests per second\n", config.InMemory, rps)
		})
	}
}

func printSystemInfo() {
	fmt.Println("System Information:")
	fmt.Printf("OS: %s\n", runtime.GOOS)
	fmt.Printf("Architecture: %s\n", runtime.GOARCH)
	fmt.Printf("Go Version: %s\n", runtime.Version())
	fmt.Printf("CPU Cores: %d\n", runtime.NumCPU())

	v, _ := mem.VirtualMemory()
	fmt.Printf("Total Memory: %v GB\n", v.Total/1024/1024/1024)
	fmt.Printf("Free Memory: %v GB\n", v.Free/1024/1024/1024)
	fmt.Printf("Used Memory: %v GB\n", v.Used/1024/1024/1024)

	cpuPercent, _ := cpu.Percent(time.Second, false)
	fmt.Printf("CPU Usage: %.2f%%\n", cpuPercent[0])

	fmt.Println("---")
}
