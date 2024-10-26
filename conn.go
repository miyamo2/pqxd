package pqxd

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/miyamo2/pqxd/internal"
	"go.uber.org/atomic"
	"regexp"
	"strings"
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

// connection is an implementation of driver.Conn
type connection struct {
	// client DynamoDB Client
	client internal.DynamoDBClient

	// closed if true, the connection is closed
	closed atomic.Bool

	// txOngoing if true, the transaction is ongoing
	txOngoing atomic.Bool

	// txStmtPub publishes statements in a transaction
	txStmtPub atomic.Pointer[transactionStatementPublisher]

	// txCommiter commits the transaction
	txCommiter atomic.Pointer[transactionCommitter]

	// txRollbacker rolls back the transaction
	txRollbacker atomic.Pointer[transactionRollbacker]
}

// Ping See: driver.Pinger
func (c *connection) Ping(ctx context.Context) error {
	if c.closed.Load() {
		return driver.ErrBadConn
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
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}

	stmt, err := c.preparedStatementFromQueryString(query)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		stmt.Close()
		return nil, ctx.Err()
	}
	return stmt, nil
}

// Close See: driver.Conn
func (c *connection) Close() error {
	if c.closed.Load() {
		return nil
	}
	defer c.closed.Store(true)
	if c.txOngoing.Load() {
		c.txRollbacker.Load().rollback()
	}
	return nil
}

// Begin See: driver.Conn
func (c *connection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// ExecContext See: driver.ExecerContext
func (c *connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}

	params, err := toPartiQLParameters(args)
	if err != nil {
		return nil, err
	}

	if c.txOngoing.Load() {
		inout := &transactionInOut{
			input: types.ParameterizedStatement{
				Statement:  &query,
				Parameters: params,
			},
		}
		c.txStmtPub.Load().publish(inout)
		return newLazyResult(c.newTxGetAffected(inout)), nil
	}

	input := dynamodb.ExecuteStatementInput{
		Statement:  &query,
		Parameters: params,
	}
	_, err = c.client.ExecuteStatement(ctx, &input)
	if err != nil {
		return nil, err
	}
	return newPqxdResult(1), nil
}

// QueryContext See: driver.QueryerContext
func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}
	match := reSELECT.FindStringSubmatch(query)
	if len(match) == 0 {
		return nil, nil
	}
	selectedList := extractSelectedListFromMatchString(match)

	params, err := toPartiQLParameters(args)
	if err != nil {
		return nil, err
	}

	if c.txOngoing.Load() {
		inout := &transactionInOut{
			input: types.ParameterizedStatement{
				Statement:  &query,
				Parameters: params,
			},
		}
		fetch := c.newTxFetchClosure(inout)
		c.txStmtPub.Load().publish(inout)
		return newTxRows(selectedList, fetch, c.txCommiter.Load()), nil
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
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}
	if c.txOngoing.Load() {
		return nil, ErrTxDualBoot
	}

	txStmtCh := make(chan *transactionInOut)
	commitCh := make(chan struct{}, 1)
	commitDone := make(chan struct{}, 1)
	rollbackCh := make(chan struct{}, 1)
	rollbackDone := make(chan struct{}, 1)

	c.txStmtPub = *atomic.NewPointer(&transactionStatementPublisher{ch: txStmtCh})
	c.txCommiter = *atomic.NewPointer(&transactionCommitter{ch: commitCh, done: commitDone})
	c.txRollbacker = *atomic.NewPointer(&transactionRollbacker{ch: rollbackCh, done: rollbackDone})
	c.txOngoing.Store(true)

	go func() {
		var inouts []*transactionInOut
		defer func() {
			c.txOngoing.Store(false)
			c.txStmtPub.Load().close()
			c.txCommiter.Load().close()
			close(commitDone)
			c.txRollbacker.Load().close()
			close(rollbackDone)
		}()
		for {
			select {
			default:
				// do nothing
			case inout, ok := <-txStmtCh:
				if !ok {
					continue
				}
				inouts = append(inouts, inout)
			case _, ok := <-commitCh:
				if !ok {
					continue
				}
				var inputs []types.ParameterizedStatement
				for _, inout := range inouts {
					inputs = append(inputs, inout.input)
				}
				txResult, err := c.client.ExecuteTransaction(ctx, &dynamodb.ExecuteTransactionInput{
					TransactStatements:     inputs,
					ReturnConsumedCapacity: types.ReturnConsumedCapacityNone,
				})
				if err != nil {
					for _, inout := range inouts {
						inout.err = err
					}
					return
				}
				for i, resp := range txResult.Responses {
					inouts[i].output = resp.Item
				}
				return
			case _, ok := <-rollbackCh:
				if !ok {
					continue
				}
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return c, nil
}

// named capture keys
const (
	// namedCaptureKeySelectedList is the named capture key for selected list
	namedCaptureKeySelectedList = "selected_list"

	// namedCaptureKeySELECTTableName is the named capture key for table name
	namedCaptureKeySELECTTableName = "table_name"

	// namedCaptureKeyWHERECondition is the named capture key for WHERE condition
	namedCaptureKeyWHERECondition = "where"

	// namedCaptureKeyINSERTValue is the named capture key for INSERT value
	namedCaptureKeyINSERTValue = "insert_value"

	// namedCaptureKeyUpdateSet is the named capture key for UPDATE set
	namedCaptureKeyINSERTClause = "insert_clause"

	// namedCaptureKeyUpdateSet is the named capture key for UPDATE set
	namedCaptureKeyUpdateSet = "update_set"

	// namedCaptureKeyUPDATEClause is the named capture key for UPDATE clause
	namedCaptureKeyUPDATEClause = "update_clause"

	// namedCaptureKeyDELETEClause is the named capture key for DELETE clause
	namedCaptureKeyDELETEClause = "delete_clause"
)

// regular expression strings
const (
	// reStrWHERECondition is the regular expression for WHERE condition
	reStrWHERECondition = `(?P<` + namedCaptureKeyWHERECondition + `>(?:WHERE\s+)(.+))`

	// reStrSelectedList is the regular expression for selected list
	reStrSelectedList = `(?P<` + namedCaptureKeySelectedList + `>(\*|[a-z0-9_\-\.]{1,255}(,\s*[a-z0-9_\-\.]{1,255})*))`

	// reStrSELECTTableName is the regular expression for table name
	reStrSELECTTableName = `(?P<` + namedCaptureKeySELECTTableName + `>("[a-z0-9_\-\.]{3,255}"(\."[a-z0-9_\-\.]{3,255}")?))`

	reStrSELECTStatement = `(?i)^\s*(?:SELECT)\s+` + reStrSelectedList + `\s+(?:FROM)\s+` + reStrSELECTTableName + `(\s+` + reStrWHERECondition + `)?` + `\s*$`

	// reStrINSERTClause is the regular expression for INSERT clause
	reStrINSERTClause = `(?P<` + namedCaptureKeyINSERTClause + `>INSERT)`

	// reStrINSERTValue is the regular expression for INSERT value
	reStrINSERTValue = `(?P<` + namedCaptureKeyINSERTValue + `>(\{.+\}))`

	// reStrINSERTStatement is the regular expression for INSERT statement
	reStrINSERTStatement = `(?i)^\s*` + reStrINSERTClause + `\s+(?:INTO)\s+("[a-z0-9_\-\.]{3,255}")\s+(?:VALUE)\s+` + reStrINSERTValue + `\s*$`

	// reStrUPDATEClause is the regular expression for UPDATE clause
	reStrUPDATEClause = `(?P<` + namedCaptureKeyUPDATEClause + `>UPDATE)`

	// reStrUPDATESet is the regular expression for UPDATE set
	reStrUPDATESet = `(?P<` + namedCaptureKeyUpdateSet + `>(((SET\s+[a-z0-9_\-\.]{3,255}=.+)|(REMOVE\s+[a-z0-9_\-\.]{3,255}=.+))+))`

	// reStrUPDATEStatement is the regular expression for UPDATE statement
	reStrUPDATEStatement = `(?i)^\s*` + reStrUPDATEClause + `(?:\s+("[a-z0-9_\-\.]{3,255}")\s+)` + reStrUPDATESet + reStrWHERECondition + `\s*$`

	// reStrDELETEClause is the regular expression for DELETE clause
	reStrDELETEClause = `(?P<` + namedCaptureKeyDELETEClause + `>DELETE)`

	// reStrDELETEStatement is the regular expression for DELETE statement
	reStrDELETEStatement = `(?i)^\s*` + reStrDELETEClause + `(?:\s+FROM\s+("[a-z0-9_\-\.]{3,255}")\s+)` + reStrWHERECondition + `\s*$`
)

// regexps
var (
	// reSELECT is the regular expression for SELECT statement
	reSELECT = regexp.MustCompile(reStrSELECTStatement)

	// reINSERT is the regular expression for INSERT statement
	reINSERT = regexp.MustCompile(reStrINSERTStatement)

	// reUPDATE is the regular expression for UPDATE statement
	reUPDATE = regexp.MustCompile(reStrUPDATEStatement)

	// reDELETE is the regular expression for DELETE statement
	reDELETE = regexp.MustCompile(reStrDELETEStatement)
)

var execRegexps = []*regexp.Regexp{reINSERT, reUPDATE, reDELETE}

// preparedStatementFromQueryString returns prepared statement from the query string
func (c *connection) preparedStatementFromQueryString(query string) (stmt driver.Stmt, err error) {
	for _, regx := range execRegexps {
		if match := regx.FindStringSubmatch(query); len(match) > 0 {
			stmt = newStatementExec(
				query,
				countPlaceHolders(match, regx),
				c.ExecContext,
				c.newCloseCheckClosure())
			return
		}
	}
	if match := reSELECT.FindStringSubmatch(query); len(match) > 0 {
		if len(match) == 0 {
			return nil, fmt.Errorf("invalid query: %s", query)
		}
		stmt = newStatementQuery(
			query,
			countPlaceHolders(match, reSELECT),
			c.QueryContext,
			c.newCloseCheckClosure())
		return
	}
	err = ErrInvalidPreparedStatement
	return
}

// newFetchClosure returns fetchClosure
func (c *connection) newFetchClosure(input dynamodb.ExecuteStatementInput) fetchClosure {
	return func(ctx context.Context, nextToken *string, dest *[]map[string]types.AttributeValue) (*string, error) {
		if c.closed.Load() {
			return nil, driver.ErrBadConn
		}

		output, err := c.client.ExecuteStatement(ctx, &input)
		if err != nil {
			return nil, err
		}
		*dest = output.Items
		return output.NextToken, nil
	}
}

// newExecClosure returns execClosure
func (c *connection) newExecClosure(input dynamodb.ExecuteStatementInput) execClosure {
	return func(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
		if c.closed.Load() {
			return nil, driver.ErrBadConn
		}
		params, err := toPartiQLParameters(args)
		if err != nil {
			return nil, err
		}

		input.Parameters = params
		_, err = c.client.ExecuteStatement(ctx, &input)
		if err != nil {
			return nil, err
		}
		return newPqxdResult(1), nil
	}
}

// newCloseCheckClosure returns closure for checking if the connection is closed
func (c *connection) newCloseCheckClosure() func() error {
	return func() error {
		if c.closed.Load() {
			return driver.ErrBadConn
		}
		return nil
	}
}

// newConnection returns a new connection
func newConnection(client internal.DynamoDBClient) *connection {
	return &connection{
		client:    client,
		closed:    *atomic.NewBool(false),
		txOngoing: *atomic.NewBool(false),
	}
}

// newTxFetchClosure returns fetchClosure
func (c *connection) newTxFetchClosure(inOut *transactionInOut) fetchClosure {
	return func(_ context.Context, _ *string, dest *[]map[string]types.AttributeValue) (*string, error) {
		if c.txOngoing.Load() {
			return nil, nil
		}
		if inOut.err != nil {
			return nil, inOut.err
		}
		*dest = []map[string]types.AttributeValue{inOut.output}
		return nil, nil
	}
}

// newTxGetAffected returns closure for getting affected rows in a transaction
func (c *connection) newTxGetAffected(inOut *transactionInOut) func() (int64, error) {
	return func() (int64, error) {
		if c.txOngoing.Load() {
			return 0, nil
		}
		if inOut.err != nil {
			return 0, inOut.err
		}
		return 1, nil
	}
}

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

// countPlaceHolders counts the number of placeholders in the query
func countPlaceHolders(match []string, regx *regexp.Regexp) int {
	var count int
	if i := regx.SubexpIndex(namedCaptureKeyWHERECondition); i != -1 {
		count += strings.Count(match[i], "?")
	}
	if i := regx.SubexpIndex(namedCaptureKeyINSERTValue); i != -1 {
		count += strings.Count(match[i], "?")
	}
	if i := regx.SubexpIndex(namedCaptureKeyUpdateSet); i != -1 {
		count += strings.Count(match[i], "?")
	}
	return count
}

// toNamedValue converts []driver.Value to []driver.NamedValue
func toNamedValue(args []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, 0, len(args))
	for i, arg := range args {
		namedValues = append(namedValues, driver.NamedValue{Ordinal: i + 1, Value: arg})
	}
	return namedValues
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
