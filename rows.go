package pqxd

import (
	"context"
	"database/sql/driver"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.uber.org/atomic"
	"io"
	"maps"
	"slices"
)

// compatibility check
var (
	_ driver.Rows              = (*pqxdRows)(nil)
	_ driver.RowsNextResultSet = (*pqxdRows)(nil)
)

// fetchClosure fetches the next result set.
type fetchClosure func(ctx context.Context, nextToken *string, dest *[]map[string]types.AttributeValue) (*string, error)

// pqxdRows is an implementation of driver.Rows
type pqxdRows struct {
	// columnNames is the list of column names.
	columnNames []string

	// nextToken See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#ExecuteStatementInput
	nextToken atomic.Pointer[string]

	// fetch fetches the next result set.
	fetch fetchClosure

	// fetchCancel cancels the ongoing fetch operation.
	fetchCancel atomic.Pointer[context.CancelFunc]

	// out is the current result set.
	out atomic.Pointer[[]map[string]types.AttributeValue]

	// outCursor is the current cursor position in the result set.
	outCursor atomic.Uint32
}

// Next See: driver.Rows
func (r *pqxdRows) Next(dest []driver.Value) error {
	out := *r.out.Load()
	cursor := int(r.outCursor.Load())
	if len(out)-1 < cursor {
		return io.EOF
	}

	row := out[cursor]
	r.outCursor.Store(r.outCursor.Inc())

	columns := r.Columns()
	if len(columns) == 0 {
		columns = slices.Collect(maps.Keys(row))
		slices.Sort(columns)
	}

	for i, col := range columns {
		var value any
		colVal, ok := row[col]
		if !ok {
			dest[i] = nil
			continue
		}
		err := attributevalue.Unmarshal(colVal, &value)
		if err != nil {
			return err
		}
		dest[i] = value
	}
	return nil
}

// HasNextResultSet See: driver.RowsNextResultSet
func (r *pqxdRows) HasNextResultSet() bool {
	return r.nextToken.Load() != nil
}

// NextResultSet See: driver.RowsNextResultSet
func (r *pqxdRows) NextResultSet() error {
	out := *r.out.Load()
	cursor := r.outCursor.Load()
	if len(out) != 0 && len(out)-1 != int(cursor) {
		return nil
	}

	nt := r.nextToken.Load()
	if nt == nil {
		return io.EOF
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.fetchCancel.Store(&cancel)

	var next []map[string]types.AttributeValue
	nt, err := r.fetch(ctx, nt, &next)
	if err != nil {
		return err
	}
	if len(next) == 0 && nt == nil {
		return io.EOF
	}
	r.nextToken.Store(nt)
	r.out.Store(&next)
	r.outCursor.Store(0)
	return nil
}

// Columns See: driver.Rows
func (r *pqxdRows) Columns() []string {
	return r.columnNames
}

// Close See: driver.Rows
func (r *pqxdRows) Close() (err error) {
	fcp := r.fetchCancel.Load()
	defer r.fetchCancel.Store(nil)
	if fcp == nil {
		return
	}
	fc, ok := (any)(*fcp).(context.CancelFunc)
	if !ok {
		return
	}
	fc()
	return
}

// newRows returns a new pqxdRows
func newRows(columnNames []string, nextToken *string, fetch fetchClosure, out []map[string]types.AttributeValue) *pqxdRows {
	if len(columnNames) == 1 && columnNames[0] == "*" {
		columnNames = nil
	}
	return &pqxdRows{
		columnNames: columnNames,
		nextToken:   *atomic.NewPointer(nextToken),
		fetch:       fetch,
		out:         *atomic.NewPointer(&out),
	}
}
