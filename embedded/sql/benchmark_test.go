package sql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func BenchmarkConditionalRowReader(b *testing.B) {
	// 1. Setup
	st, err := store.Open(b.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(b, err)
	defer st.Close()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix([]byte{2}))
	require.NoError(b, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE bench_table (id INTEGER, val INTEGER, PRIMARY KEY id)", nil)
	require.NoError(b, err)

	// 2. Insert a large amount of data
	rowCount := 10000
	fmt.Printf("Inserting %d rows for benchmark...\n", rowCount)

	// Batch insert for speed
	// tx, err := engine.NewTx(context.Background(), nil)
	// require.NoError(b, err)

	for i := 0; i < rowCount; i++ {
		// Using direct SQL execution for simplicity, though batching would be faster
		// For this benchmark, we care about READ speed, so setup time is acceptable.
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO bench_table (id, val) VALUES (@id, @val)", map[string]interface{}{
			"id":  i,
			"val": i,
		})
		require.NoError(b, err)
	}

	fmt.Println("Starting benchmark...")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Query that scans all rows but selects half
		// "val % 2 = 0" is a condition that forces evaluation on every row
		r, err := engine.Query(context.Background(), nil, "SELECT id FROM bench_table WHERE val > 5000", nil)
		require.NoError(b, err)

		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(b, err)
			count++
		}
		r.Close()
	}
}

func TestPerformanceComparison(t *testing.T) {
	// This test runs a "heavy" condition to demonstrate the benefit of concurrency.
	// We simulate a slow condition using a custom function if possible,
	// or just rely on the overhead of the SQL engine.

	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer st.Close()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix([]byte{2}))
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE perf_table (id INTEGER, val INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	rowCount := 5000
	fmt.Printf("Inserting %d rows...\n", rowCount)
	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO perf_table (id, val) VALUES (@id, @val)", map[string]interface{}{
			"id":  i,
			"val": i,
		})
		require.NoError(t, err)
	}

	start := time.Now()
	// Query: Select top 50%
	r, err := engine.Query(context.Background(), nil, "SELECT id FROM perf_table WHERE val > 2500", nil)
	require.NoError(t, err)

	count := 0
	for {
		_, err := r.Read(context.Background())
		if err == ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		count++
	}
	r.Close()

	duration := time.Since(start)
	fmt.Printf("Processed %d rows in %v\n", rowCount, duration)
	fmt.Printf("Throughput: %.2f rows/sec\n", float64(rowCount)/duration.Seconds())
}
