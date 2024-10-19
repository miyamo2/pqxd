package pqxd

import (
	"context"
	"database/sql/driver"
	"errors"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/miyamo2/pqxd/internal"
	"go.uber.org/atomic"
	"regexp"
	"strings"
	"sync"
)

// compatibility checks
var (
	_ driver.Conn               = (*connection)(nil)
	_ driver.QueryerContext     = (*connection)(nil)
	_ driver.ExecerContext      = (*connection)(nil)
	_ driver.ConnPrepareContext = (*connection)(nil)
	_ driver.ConnPrepareContext = (*connection)(nil)
	_ driver.ConnBeginTx        = (*connection)(nil)
	_ driver.Pinger             = (*connection)(nil)
)

// ErrClosedConnection indicates the dynamodb connection is closed
var ErrClosedConnection = errors.New("connection is closed")

// connection is an implementation of driver.Conn
type connection struct {
	// client DynamoDB Client
	client internal.DynamoDBClient

	// closed if true, the connection is closed
	closed atomic.Bool

	// tx is an ongoing transaction
	tx transaction

	// txMu is the lock for tx
	txMu sync.RWMutex
}

// Ping See: driver.Pinger
func (c *connection) Ping(ctx context.Context) error {
	if c.closed.Load() {
		return ErrClosedConnection
	}
	_, err := c.client.DescribeEndpoints(ctx, nil)
	return err
}

// Prepare See: driver.Conn
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext See: driver.ConnPrepareContext
func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	//TODO implement me
	panic("implement me")
}

// Close See: driver.Conn
func (c *connection) Close() error {
	if c.closed.Load() {
		return nil
	}
	defer c.closed.Store(true)
	c.txMu.Lock()
	defer c.txMu.Unlock()
	if c.tx != nil {
		switch c.tx.(type) {
		case *queryTx:
			// TODO implement me
		case *execTx:
			// TODO implement me
		}
	}
	return nil
}

// Begin See: driver.Conn
func (c *connection) Begin() (driver.Tx, error) {
	//TODO implement me
	panic("implement me")
}

// ExecContext See: driver.ExecerContext
func (c *connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	//TODO implement me
	panic("implement me")
}

// QueryContext See: driver.QueryerContext
func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed.Load() {
		return nil, ErrClosedConnection
	}
	match := reSELECT.FindStringSubmatch(query)
	if len(match) == 0 {
		return nil, nil
	}
	selectedList := extractSelectedListFromMatchString(match)

	c.txMu.Lock()
	defer c.txMu.Unlock()
	if c.tx != nil {
		switch c.tx.(type) {
		case *queryTx:
			// TODO implement me
		case *execTx:
			// TODO implement me
		}
	}

	params, err := toPartiQLParameters(args)
	if err != nil {
		return nil, err
	}

	input := dynamodb.ExecuteStatementInput{
		Statement:  &query,
		Parameters: params,
	}
	fetch := c.newFetchClosure(input)

	var items []map[string]types.AttributeValue
	nt, err := fetch(ctx, nil, &items)
	if err != nil {
		return nil, err
	}

	return newRows(selectedList, nt, fetch, items), nil
}

// BeginTx See: driver.ConnBeginTx
func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	//TODO implement me
	panic("implement me")
}

// regex strings
const (
	namedCaptureKeySelectedList    = "selected_list"
	reStrSelectedList              = `(?P<` + namedCaptureKeySelectedList + `>(\*|[a-z0-9_\-\.]{1,255}(,\s*[a-z0-9_\-\.]{1,255})*))`
	namedCaptureKeySELECTTableName = "table_name"
	reStrSELECTTableName           = `(?P<` + namedCaptureKeySELECTTableName + `>("[a-z0-9_\-\.]{3,255}"(\."[a-z0-9_\-\.]{3,255}")?))`
)

var (
	reSELECT = regexp.MustCompile(`(?i)(?:SELECT)\s+` + reStrSelectedList + `\s+(?:FROM)\s+` + reStrSELECTTableName)
)

// extractSelectedListFromMatchString extracts selected list from the match string
func extractSelectedListFromMatchString(match []string) (columns []string) {
	selectedListStr := match[reSELECT.SubexpIndex(namedCaptureKeySelectedList)]
	for _, v := range strings.Split(selectedListStr, ",") {
		trimmedQuot := strings.ReplaceAll(v, `'`, "")
		trimmedWQuot := strings.ReplaceAll(trimmedQuot, `"`, "")
		columns = append(columns, strings.TrimSpace(trimmedWQuot))
	}
	return
}

// extractTableNameFromMatchString extracts table name from the match string
func extractTableNameFromMatchString(match []string) string {
	v := match[reSELECT.SubexpIndex(namedCaptureKeySELECTTableName)]
	trimmedQuot := strings.ReplaceAll(v, `'`, "")
	trimmedWQuot := strings.ReplaceAll(trimmedQuot, `"`, "")
	return strings.TrimSpace(trimmedWQuot)
}

// fetchResult
type fetchResult struct {
	out       []map[string]types.AttributeValue
	nextToken *string
}

// newFetchClosure returns fetchClosure
func (c *connection) newFetchClosure(input dynamodb.ExecuteStatementInput) fetchClosure {
	return func(ctx context.Context, nextToken *string, dest *[]map[string]types.AttributeValue) (*string, error) {
		if c.closed.Load() {
			return nil, ErrClosedConnection
		}
		resultCh := make(chan fetchResult, 1)
		errCh := make(chan error, 1)
		go func() {
			output, err := c.client.ExecuteStatement(ctx, &input)
			if err != nil {
				errCh <- err
				close(errCh)
				close(resultCh)
			}
			resultCh <- fetchResult{
				out:       output.Items,
				nextToken: output.NextToken,
			}
			close(errCh)
			close(resultCh)
		}()

		for {
			select {
			case result := <-resultCh:
				*dest = result.out
				return result.nextToken, nil
			case err := <-errCh:
				return nil, err
			case <-ctx.Done():
				return nil, nil
			}
		}
	}
}

// toPartiQLParameters converts []driver.NamedValue to []types.AttributeValue
func toPartiQLParameters(args []driver.NamedValue) (params []types.AttributeValue, err error) {
	for _, arg := range args {
		av, err := toAttributeValue(arg.Value)
		if err != nil {
			return nil, err
		}
		params = append(params, av)
	}
	return
}

// toAttributeValue converts interface{} to types.AttributeValue
func toAttributeValue(value interface{}) (types.AttributeValue, error) {
	switch v := value.(type) {
	case driver.Valuer:
		dv, err := v.Value()
		if err != nil {
			return &types.AttributeValueMemberNULL{Value: true}, err
		}
		return toAttributeValue(dv)
	case types.AttributeValue:
		return v, nil
	case types.AttributeValueMemberB:
		return &v, nil
	case types.AttributeValueMemberBOOL:
		return &v, nil
	case types.AttributeValueMemberBS:
		return &v, nil
	case types.AttributeValueMemberL:
		return &v, nil
	case types.AttributeValueMemberM:
		return &v, nil
	case types.AttributeValueMemberN:
		return &v, nil
	case types.AttributeValueMemberNS:
		return &v, nil
	case types.AttributeValueMemberNULL:
		return &v, nil
	case types.AttributeValueMemberS:
		return &v, nil
	case types.AttributeValueMemberSS:
		return &v, nil
	default:
		return attributevalue.Marshal(value)
	}
}
