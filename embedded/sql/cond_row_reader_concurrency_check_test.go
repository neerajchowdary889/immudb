package sql

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConditionalRowReader_ConcurrencyVerification(t *testing.T) {
	// This test verifies that multiple goroutines are actually being used
	// by checking the number of active goroutines during execution.

	rowCount := 10000
	rows := make([]*Row, rowCount)
	for i := 0; i < rowCount; i++ {
		rows[i] = &Row{
			ValuesByPosition: []TypedValue{&Integer{val: int64(i)}},
		}
	}

	mockReader := &mockRowReader{
		rows:       rows,
		tableAlias: "t1",
	}

	// Condition with a slight delay to ensure workers stay busy
	// and we can observe them running.
	condition := &mockValueExp{
		shouldPass: func(row *Row) bool {
			time.Sleep(10 * time.Microsecond) // Simulate work
			val := row.ValuesByPosition[0].(*Integer).val
			return val%2 == 0
		},
	}

	reader := newConditionalRowReader(mockReader, condition)
	defer reader.Close()

	ctx := context.Background()

	// Start reading in a separate goroutine so we can monitor
	done := make(chan bool)
	go func() {
		for {
			_, err := reader.Read(ctx)
			if err == ErrNoMoreRows {
				break
			}
			if err != nil {
				break
			}
		}
		done <- true
	}()

	// Give it a moment to start up
	time.Sleep(50 * time.Millisecond)

	// Check number of goroutines
	// We expect:
	// 1. Main test goroutine
	// 2. Reader goroutine (started above)
	// 3. Feeder goroutine (in cond_row_reader)
	// 4. Closer goroutine (in cond_row_reader)
	// 5. Worker goroutines (runtime.NumCPU())
	// Plus potentially others from the runtime/test framework.
	// So we should definitely see a significant increase compared to baseline.

	numGoroutines := runtime.NumGoroutine()
	numCPU := runtime.NumCPU()

	fmt.Printf("Number of CPUs: %d\n", numCPU)
	fmt.Printf("Active Goroutines: %d\n", numGoroutines)

	// We expect at least NumCPU workers + feeder + closer + main + reader > NumCPU + 4
	require.Greater(t, numGoroutines, numCPU, "Should have at least NumCPU worker goroutines running")

	<-done
}
