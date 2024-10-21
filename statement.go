package pqxd

import (
	"context"
	"database/sql/driver"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.uber.org/atomic"
)

// compatibility check
var (
	_ driver.Stmt             = (*statementQuery)(nil)
	_ driver.StmtQueryContext = (*statementQuery)(nil)
)

// getFetchClosure is a function that returns fetchClosure
type getFetchClosure func(dynamodb.ExecuteStatementInput) fetchClosure

// statementQuery is an implementation of driver.Stmt
type statementQuery struct {
	// query is a string of prepared statement
	query string

	// selectedList is a list of selected items
	selectedList []string

	// numInput is the number of placeholders in the statement
	numInput int

	// getFetchClosure returns fetchClosure
	getFetchClosure getFetchClosure

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
	if err := s.connCloseCheckClosure(); err != nil {
		s.closed.Store(true)
		return nil, driver.ErrBadConn
	}
	if s.closed.Load() {
		return nil, ErrStatementClosed
	}
	params, err := toPartiQLParameters(args)
	if err != nil {
		return nil, err
	}
	input := dynamodb.ExecuteStatementInput{
		Statement:  &s.query,
		Parameters: params,
	}
	fetch := s.getFetchClosure(input)
	var items []map[string]types.AttributeValue
	nt, err := fetch(ctx, nil, &items)
	if err != nil {
		return nil, err
	}

	return newRows(s.selectedList, nt, fetch, items), nil
}

// newStatementQuery returns a new statementQuery
func newStatementQuery(
	query string,
	selectedList []string,
	numInput int,
	getFetchClosure getFetchClosure,
	connCloseCheckClosure func() error,
) *statementQuery {
	return &statementQuery{
		query:                 query,
		selectedList:          selectedList,
		numInput:              numInput,
		getFetchClosure:       getFetchClosure,
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
	// numInput is the number of placeholders in the statement
	numInput int

	// execClosure executes the statement
	execClosure execClosure

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
	return s.execClosure(ctx, args)
}

func (s statementExec) Query(args []driver.Value) (driver.Rows, error) {
	return nil, ErrNotSupported
}

// newStatementExec returns a new statementExec
func newStatementExec(
	numInput int,
	execClosure execClosure,
	connCloseCheckClosure func() error,
) *statementExec {
	return &statementExec{
		numInput:              numInput,
		execClosure:           execClosure,
		closed:                *atomic.NewBool(false),
		connCloseCheckClosure: connCloseCheckClosure,
	}
}
