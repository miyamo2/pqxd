package pqxd

import (
	"context"
	"database/sql/driver"
	"go.uber.org/atomic"
)

// compatibility check
var (
	_ driver.Stmt             = (*statementQuery)(nil)
	_ driver.StmtQueryContext = (*statementQuery)(nil)
)

// queryContext executes the prepared statement
type queryContext func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)

// execContext executes the prepared statement
type execContext func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)

// statementQuery is an implementation of driver.Stmt
type statementQuery struct {
	// query is a string of prepared statement
	query string

	// selectedList is a list of selected items
	selectedList []string

	// numInput is the number of placeholders in the statement
	numInput int

	// queryContext executes the statement
	queryContext func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)

	// closed is a flag that indicates whether the statement is closed
	closed atomic.Bool

	// connCloseCheckClosure checks if the connection is closed
	connCloseCheckClosure func() error
}

// Close See: driver.Stmt
func (s statementQuery) Close() error {
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
func (s statementQuery) NumInput() int {
	return s.numInput
}

// Exec See: driver.Stmt
func (s statementQuery) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, ErrNotSupported
}

// Query See: driver.Stmt
func (s statementQuery) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), toNamedValue(args))
}

// QueryContext See: driver.StmtQueryContext
func (s statementQuery) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.queryContext(ctx, s.query, args)
}

// newStatementQuery returns a new statementQuery
func newStatementQuery(
	query string,
	numInput int,
	queryContext queryContext,
	connCloseCheckClosure func() error,
) *statementQuery {
	return &statementQuery{
		query:                 query,
		numInput:              numInput,
		queryContext:          queryContext,
		closed:                *atomic.NewBool(false),
		connCloseCheckClosure: connCloseCheckClosure,
	}
}

// compatibility check
var (
	_ driver.Stmt            = (*statementExec)(nil)
	_ driver.StmtExecContext = (*statementExec)(nil)
)

// execClosure executes the statement
type execClosure func(ctx context.Context, args []driver.NamedValue) (driver.Result, error)

// statementExec is an implementation of driver.Stmt
type statementExec struct {
	// query is a string of prepared statement
	query string

	// numInput is the number of placeholders in the statement
	numInput int

	// execContext executes the statement
	execContext execContext

	// closed is a flag that indicates whether the statement is closed
	closed atomic.Bool

	// connCloseCheckClosure checks if the connection is closed
	connCloseCheckClosure func() error
}

// Close See: driver.Stmt
func (s statementExec) Close() error {
	if err := s.connCloseCheckClosure(); err != nil {
		return driver.ErrBadConn
	}
	if s.closed.Load() {
		return ErrStatementClosed
	}
	s.closed.Store(true)
	return nil
}

// NumInput See: driver.Stmt
func (s statementExec) NumInput() int {
	return s.numInput
}

func (s statementExec) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), toNamedValue(args))
}

func (s statementExec) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.connCloseCheckClosure(); err != nil {
		return nil, driver.ErrBadConn
	}
	if s.closed.Load() {
		s.closed.Store(true)
		return nil, ErrStatementClosed
	}
	return s.execContext(ctx, s.query, args)
}

func (s statementExec) Query(args []driver.Value) (driver.Rows, error) {
	return nil, ErrNotSupported
}

// newStatementExec returns a new statementExec
func newStatementExec(
	query string,
	numInput int,
	execContext execContext,
	connCloseCheckClosure func() error,
) *statementExec {
	return &statementExec{
		query:                 query,
		numInput:              numInput,
		execContext:           execContext,
		closed:                *atomic.NewBool(false),
		connCloseCheckClosure: connCloseCheckClosure,
	}
}
