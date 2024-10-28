package pqxd

import (
	"context"
	"database/sql/driver"
	"go.uber.org/atomic"
)

// compatibility check
var (
	_ driver.Stmt             = (*statement)(nil)
	_ driver.StmtQueryContext = (*statement)(nil)
	_ driver.StmtExecContext  = (*statement)(nil)
)

// queryWithPrepare executes the prepared statement
type queryWithPrepare func(ctx context.Context, query string, selectedList []string, args []driver.NamedValue) (driver.Rows, error)

// execContext executes the prepared statement
type execContext func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)

// statement is an implementation of driver.Stmt
type statement struct {
	// query is a string of prepared statement
	query string

	// selectedList is a list of selected items
	selectedList []string

	// numInput is the number of placeholders in the statement
	numInput int

	// queryWithPrepare executes the statement
	queryWithPrepare queryWithPrepare

	// execContext executes the statement
	execContext execContext

	// closed is a flag that indicates whether the statement is closed
	closed atomic.Bool

	// connCloseCheckClosure checks if the connection is closed
	connCloseCheckClosure func() error
}

// Close See: driver.Stmt
func (s statement) Close() error {
	if err := s.connCloseCheckClosure(); err != nil {
		s.closed.Store(true)
		return driver.ErrBadConn
	}
	if s.closed.Load() {
		return ErrStatementClosed
	}
	s.closed.Store(true)
	return nil
}

// NumInput See: driver.Stmt
func (s statement) NumInput() int {
	return s.numInput
}

// Exec See: driver.Stmt
func (s statement) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), toNamedValue(args))
}

// Query See: driver.Stmt
func (s statement) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), toNamedValue(args))
}

// QueryContext See: driver.StmtQueryContext
func (s statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.queryWithPrepare(ctx, s.query, s.selectedList, args)
}

// ExecContext See: driver.StmtExecContext
func (s statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.connCloseCheckClosure(); err != nil {
		return nil, driver.ErrBadConn
	}
	if s.closed.Load() {
		s.closed.Store(true)
		return nil, ErrStatementClosed
	}
	return s.execContext(ctx, s.query, args)
}

// newStatement returns a new statement
func newStatement(
	query string,
	selectedList []string,
	numInput int,
	queryWithPrepare queryWithPrepare,
	execContext execContext,
	connCloseCheckClosure func() error,
) *statement {
	return &statement{
		query:                 query,
		selectedList:          selectedList,
		numInput:              numInput,
		queryWithPrepare:      queryWithPrepare,
		execContext:           execContext,
		closed:                *atomic.NewBool(false),
		connCloseCheckClosure: connCloseCheckClosure,
	}
}
