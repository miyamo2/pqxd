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
	"sync"
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
		if err := attributevalue.Unmarshal(colVal, &value); err != nil {
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
	if len(out) != 0 && len(out) > int(cursor) {
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

var _ driver.Rows = (*txRows)(nil)

// txRows is an implementation of driver.Rows for transaction
type txRows struct {
	pqxdRows

	// txCommiter commits the transaction
	txCommiter *transactionCommitter

	once sync.Once
}

func (r *txRows) Next(dest []driver.Value) error {
	if err := r.NextResultSet(); err != nil {
		return err
	}
	return r.pqxdRows.Next(dest)
}

func (r *txRows) HasNextResultSet() bool {
	r.txCommiter.commit()
	return r.pqxdRows.HasNextResultSet()
}

func (r *txRows) NextResultSet() error {
	r.txCommiter.commit()
	var err error
	r.once.Do(func() {
		err = r.pqxdRows.NextResultSet()
	})
	return err
}

// newTxRows returns a new txRows
func newTxRows(columnNames []string, fetch fetchClosure, txCommiter *transactionCommitter) *txRows {
	return &txRows{
		pqxdRows: pqxdRows{
			columnNames: columnNames,
			nextToken:   *atomic.NewPointer(new(string)),
			fetch:       fetch,
			out:         *atomic.NewPointer(new([]map[string]types.AttributeValue)),
		},
		txCommiter: txCommiter,
		once:       sync.Once{},
	}
}

var (
	_ driver.Rows = (*describeTableRows)(nil)
)

// describeTableRows is an implementation of driver.Rows for DescribeTable API.
type describeTableRows struct {
	// columnNames is the list of column names.
	columnNames []string

	// tableDescription See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb/types#TableDescription
	tableDescription types.TableDescription
}

// Columns See: driver.Rows
func (r *describeTableRows) Columns() []string {
	return r.columnNames
}

// Close See: driver.Rows
func (r *describeTableRows) Close() error {
	return nil
}

// Next See: driver.Rows
func (r *describeTableRows) Next(dest []driver.Value) error {
	for i, selected := range r.columnNames {
		switch selected {
		case "ArchivalSummary":
			dest[i] = r.tableDescription.ArchivalSummary
		case "AttributeDefinitions":
			dest[i] = r.tableDescription.AttributeDefinitions
		case "BillingModeSummary":
			dest[i] = r.tableDescription.BillingModeSummary
		case "CreationDateTime":
			dest[i] = r.tableDescription.CreationDateTime
		case "DeletionProtectionEnabled":
			dest[i] = r.tableDescription.DeletionProtectionEnabled
		case "GlobalSecondaryIndexes":
			dest[i] = r.tableDescription.GlobalSecondaryIndexes
		case "GlobalTableVersion":
			dest[i] = r.tableDescription.GlobalTableVersion
		case "ItemCount":
			dest[i] = r.tableDescription.ItemCount
		case "KeySchema":
			dest[i] = r.tableDescription.KeySchema
		case "LatestStreamArn":
			dest[i] = r.tableDescription.LatestStreamArn
		case "LatestStreamLabel":
			dest[i] = r.tableDescription.LatestStreamLabel
		case "LocalSecondaryIndexes":
			dest[i] = r.tableDescription.LocalSecondaryIndexes
		case "OnDemandThroughput":
			dest[i] = r.tableDescription.OnDemandThroughput
		case "ProvisionedThroughput":
			dest[i] = r.tableDescription.ProvisionedThroughput
		case "Replicas":
			dest[i] = r.tableDescription.Replicas
		case "RestoreSummary":
			dest[i] = r.tableDescription.RestoreSummary
		case "SSEDescription":
			dest[i] = r.tableDescription.SSEDescription
		case "StreamSpecification":
			dest[i] = r.tableDescription.StreamSpecification
		case "TableArn":
			dest[i] = r.tableDescription.TableArn
		case "TableClassSummary":
			dest[i] = r.tableDescription.TableClassSummary
		case "TableId":
			dest[i] = r.tableDescription.TableId
		case "TableName":
			dest[i] = r.tableDescription.TableName
		case "TableSizeBytes":
			dest[i] = r.tableDescription.TableSizeBytes
		case "TableStatus":
			dest[i] = r.tableDescription.TableStatus
		}
	}
	return nil
}

// newDescribeTableRows returns a new describeTableRows
func newDescribeTableRows(columnNames []string, tableDescription types.TableDescription) *describeTableRows {
	return &describeTableRows{
		columnNames:      columnNames,
		tableDescription: tableDescription,
	}
}
