package pqxd

import (
	"context"
	"database/sql/driver"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// compatibility check
var (
	_ driver.Stmt             = (*statement)(nil)
	_ driver.StmtQueryContext = (*statement)(nil)
)

// getFetchClosure is a function that returns fetchClosure
type getFetchClosure func(dynamodb.ExecuteStatementInput) fetchClosure

// statement is an implementation of driver.Stmt
type statement struct {
	query           string
	selectedList    []string
	numInput        int
	getFetchClosure getFetchClosure
	closeClosure    func() error
}

// Close See: driver.Stmt
func (s statement) Close() error {
	return s.closeClosure()
}

// NumInput See: driver.Stmt
func (s statement) NumInput() int {
	return s.numInput
}

// Exec See: driver.Stmt
func (s statement) Exec(args []driver.Value) (driver.Result, error) {
	//TODO implement me
	panic("implement me")
}

// Query See: driver.Stmt
func (s statement) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), toNamedValue(args))
}

// QueryContext See: driver.StmtQueryContext
func (s statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
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

// newStatement returns a new statement
func newStatement(
	query string,
	selectedList []string,
	numInput int,
	getFetchClosure getFetchClosure,
	closeClosure func() error,
) *statement {
	return &statement{
		query:           query,
		selectedList:    selectedList,
		numInput:        numInput,
		getFetchClosure: getFetchClosure,
		closeClosure:    closeClosure,
	}
}
