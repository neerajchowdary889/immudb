package sql

import (
	"context"
	"fmt"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestExampleQuery(t *testing.T) {
	// 1. Setup the database engine
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer st.Close()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix([]byte{2}))
	require.NoError(t, err)

	// 2. Create a table
	fmt.Println("Creating table...")
	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE mytable (id INTEGER, val INTEGER, PRIMARY KEY id)", nil)
	if err != nil {
		fmt.Printf("Create Table Error: %v\n", err)
	}
	require.NoError(t, err)

	// 3. Insert some data
	fmt.Println("Inserting 1000 rows...")
	for i := 0; i < 1000; i++ {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO mytable (id, val) VALUES (@id, @val)", map[string]interface{}{
			"id":  i,
			"val": i * 10,
		})
		if err != nil {
			fmt.Printf("Insert Error at %d: %v\n", i, err)
		}
		require.NoError(t, err)
	}

	// 4. Run a query with WHERE clause (Triggers Concurrent Reader)
	fmt.Println("Running SELECT with WHERE clause...")
	// This query will use the new concurrent conditionalRowReader because of "WHERE val > 5000"
	r, err := engine.Query(context.Background(), nil, "SELECT id, val FROM mytable WHERE val > 5000", nil)
	if err != nil {
		fmt.Printf("Query Error: %v\n", err)
	}
	require.NoError(t, err)
	defer r.Close()

	// 5. Read results
	count := 0
	for {
		row, err := r.Read(context.Background())
		if err == ErrNoMoreRows {
			break
		}
		require.NoError(t, err)

		// Just print the first few to show it works
		if count < 5 {
			// ValuesByPosition contains TypedValue interface, need to cast to specific type implementation
			// In this case, it's *Integer struct which has a .val field
			idVal := row.ValuesByPosition[0].(*Integer).val
			valVal := row.ValuesByPosition[1].(*Integer).val
			fmt.Printf("Result: id=%d, val=%d\n", idVal, valVal)
		}
		count++
	}
	fmt.Printf("Total rows returned: %d\n", count)
}
