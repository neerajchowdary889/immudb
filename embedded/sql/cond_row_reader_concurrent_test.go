package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockRowReader implements RowReader for testing
type mockRowReader struct {
	rows       []*Row
	curr       int
	tableAlias string
}

func (m *mockRowReader) Read(ctx context.Context) (*Row, error) {
	if m.curr >= len(m.rows) {
		return nil, ErrNoMoreRows
	}
	row := m.rows[m.curr]
	m.curr++
	return row, nil
}

func (m *mockRowReader) Close() error { return nil }
func (m *mockRowReader) Tx() *SQLTx   { return nil }
func (m *mockRowReader) TableAlias() string {
	return m.tableAlias
}
func (m *mockRowReader) Parameters() map[string]interface{} { return nil }
func (m *mockRowReader) OrderBy() []ColDescriptor           { return nil }
func (m *mockRowReader) ScanSpecs() *ScanSpecs              { return nil }
func (m *mockRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return nil, nil
}
func (m *mockRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	return nil, nil
}
func (m *mockRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	return nil
}
func (m *mockRowReader) onClose(f func()) {}

// mockValueExp implements ValueExp for testing
type mockValueExp struct {
	shouldPass func(row *Row) bool
}

func (m *mockValueExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return BooleanType, nil
}
func (m *mockValueExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	return nil
}
func (m *mockValueExp) substitute(params map[string]interface{}) (ValueExp, error) {
	return m, nil
}
func (m *mockValueExp) selectors() []Selector { return nil }
func (m *mockValueExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	pass := m.shouldPass(row)
	return &Bool{val: pass}, nil
}
func (m *mockValueExp) reduceSelectors(row *Row, implicitTable string) ValueExp { return m }
func (m *mockValueExp) isConstant() bool                                        { return false }
func (m *mockValueExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}
func (m *mockValueExp) String() string { return "mock_condition" }

func TestConditionalRowReader_Concurrent(t *testing.T) {
	// Create 1000 rows
	rowCount := 1000
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

	// Condition: allow only even numbers
	condition := &mockValueExp{
		shouldPass: func(row *Row) bool {
			val := row.ValuesByPosition[0].(*Integer).val
			return val%2 == 0
		},
	}

	reader := newConditionalRowReader(mockReader, condition)
	defer reader.Close()

	ctx := context.Background()

	// Read all rows
	var resultVals []int64
	for {
		row, err := reader.Read(ctx)
		if err == ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		val := row.ValuesByPosition[0].(*Integer).val
		resultVals = append(resultVals, val)
	}

	// Verify results
	expectedCount := rowCount / 2
	require.Equal(t, expectedCount, len(resultVals))

	// Verify order and content
	for i, val := range resultVals {
		expected := int64(i * 2)
		require.Equal(t, expected, val, "Order mismatch at index %d", i)
	}
}

func TestConditionalRowReader_PreservesOrder(t *testing.T) {
	// This test explicitly verifies that the reader preserves the order of the underlying reader,
	// which is critical for ORDER BY clauses that rely on index scans.

	rowCount := 500
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

	// Condition: pass everything
	condition := &mockValueExp{
		shouldPass: func(row *Row) bool { return true },
	}

	reader := newConditionalRowReader(mockReader, condition)
	defer reader.Close()

	ctx := context.Background()

	var resultVals []int64
	for {
		row, err := reader.Read(ctx)
		if err == ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		val := row.ValuesByPosition[0].(*Integer).val
		resultVals = append(resultVals, val)
	}

	require.Equal(t, rowCount, len(resultVals))
	for i, val := range resultVals {
		require.Equal(t, int64(i), val, "Order mismatch at index %d - expected %d, got %d", i, i, val)
	}
}
