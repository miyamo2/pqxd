package pqxd

import (
	"context"
	"database/sql/driver"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.uber.org/atomic"
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
	client DynamoDBClient

	// closed if true, the connection is closed
	closed atomic.Bool

	// txOngoing if true, the transaction is ongoing
	txOngoing atomic.Bool

	// txStmtPub publishes statements in a transaction
	txStmtPub atomic.Pointer[transactionStatementPublisher]

	// txCommit commits the transaction
	txCommit atomic.Pointer[txCommit]

	// txRollback rollback the transaction
	txRollback atomic.Pointer[txRollback]
}

// Ping See: driver.Pinger
func (c *connection) Ping(ctx context.Context) error {
	if c.closed.Load() {
		return driver.ErrBadConn
	}
	_, err := c.client.ListTables(ctx, nil)
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
		c.txRollback.Load().function()
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
		return newLazyResult(c.newTxGetAffected(inout, c.txCommit.Load())), nil
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
	tq := tokenize(query)
	if len(tq.selectedList) == 0 {
		return nil, ErrInvalidSyntaxOfQuery
	}
	if tq.listTable {
		return c.listTables(ctx)
	}
	if tq.describeTableTarget != "" {
		target := strings.TrimSpace(strings.ReplaceAll(tq.describeTableTarget, `'`, ""))
		return c.describeTable(ctx, target, tq.selectedList, args)
	}
	return c.query(ctx, tq.queryString, tq.selectedList, args)
}

// BeginTx See: driver.ConnBeginTx
func (c *connection) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}
	if c.txOngoing.Load() {
		return nil, ErrTxDualBoot
	}

	txStmtCh := make(chan *transactionInOut)

	c.txStmtPub = *atomic.NewPointer(&transactionStatementPublisher{ch: txStmtCh})

	commitCtx, commitFunc := context.WithCancel(ctx)
	receiveResultCtx, receiveResultFunc := context.WithCancel(ctx)
	c.txCommit = *atomic.NewPointer(
		&txCommit{
			ctx:           commitCtx,
			function:      commitFunc,
			receiveResult: receiveResultCtx,
		},
	)

	rollBackCtx, rollbackFunc := context.WithCancel(ctx)
	c.txRollback = *atomic.NewPointer(
		&txRollback{
			ctx:      rollBackCtx,
			function: rollbackFunc,
		},
	)
	c.txOngoing.Store(true)

	go func() {
		var inouts []*transactionInOut
		defer func() {
			c.txOngoing.Store(false)
			c.txStmtPub.Load().close()
			commitFunc()
			rollbackFunc()
			receiveResultFunc()
		}()
		for {
			select {
			case inout := <-txStmtCh:
				inouts = append(inouts, inout)
			case <-commitCtx.Done():
				var inputs []types.ParameterizedStatement
				for _, inout := range inouts {
					inputs = append(inputs, inout.input)
				}
				txResult, err := c.client.ExecuteTransaction(
					ctx, &dynamodb.ExecuteTransactionInput{
						TransactStatements:     inputs,
						ReturnConsumedCapacity: types.ReturnConsumedCapacityNone,
					},
				)
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
			case <-rollBackCtx.Done():
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return c, nil
}

// query executes a query with given query-string, selected-list and arguments.
func (c *connection) query(
	ctx context.Context, query string, selectedList []string, args []driver.NamedValue,
) (driver.Rows, error) {
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
		fetch := c.newTxFetchClosure(inout)
		c.txStmtPub.Load().publish(inout)
		return newTxRows(selectedList, fetch, c.txCommit.Load()), nil
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

	// namedCaptureKeyRETURNINGSelectedList is the named capture key for RETURNING selected list
	namedCaptureKeyRETURNINGSelectedList = "returning_selected_list"
)

// regular expression strings
const (
	// reStrWHERECondition is the regular expression for WHERE condition
	reStrWHERECondition = `(?:WHERE\s+)(?P<` + namedCaptureKeyWHERECondition + `>(.+))`

	// reStrColumnList is the common pattern for column lists supporting both quoted and unquoted column names
	// Matches: *, id, "id", id,name, "id","name", "id",name, id ,name, id , name, etc.
	reStrColumnList = `\*|("[a-z0-9_\-\.]{1,255}"|[a-z0-9_\-\.]{1,255})(\s*,\s*("[a-z0-9_\-\.]{1,255}"|[a-z0-9_\-\.]{1,255}))*`

	// reStrSelectedList is the regular expression for selected list
	reStrSelectedList = `(?P<` + namedCaptureKeySelectedList + `>(` + reStrColumnList + `))`

	// reStrSELECTTableName is the regular expression for table name
	reStrSELECTTableName = `(?P<` + namedCaptureKeySELECTTableName + `>("[a-z0-9_\-\.]{3,255}"(\."[a-z0-9_\-\.]{3,255}")?))`

	// reStrSELECTStatement is the regular expression for SELECT statement
	reStrSELECTStatement = `(?i)^\s*(?:SELECT)\s+` + reStrSelectedList + `\s+(?:FROM)\s+` + reStrSELECTTableName + `(\s+` + reStrWHERECondition + `)?` + `\s*$`

	// reStrRETURNINGClause is the regular expression for RETURNING clause
	reStrRETURNINGClause = `(?i).*(?:RETURNING\s+(ALL OLD|MODIFIED OLD|ALL NEW|MODIFIED NEW)\s+)(?P<` + namedCaptureKeyRETURNINGSelectedList + `>(` + reStrColumnList + `))\s*$`

	// reStrINSERTClause is the regular expression for INSERT clause
	reStrINSERTClause = `(?P<` + namedCaptureKeyINSERTClause + `>INSERT)`

	// reStrINSERTValue is the regular expression for INSERT value
	reStrINSERTValue = `(?P<` + namedCaptureKeyINSERTValue + `>(\{.+\}))`

	// reStrINSERTStatement is the regular expression for INSERT statement
	reStrINSERTStatement = `(?i)^\s*` + reStrINSERTClause + `\s+(?:INTO)\s+("[a-z0-9_\-\.]{3,255}")\s+(?:VALUE)\s+` + reStrINSERTValue + `\s*$`

	// reStrUPDATEClause is the regular expression for UPDATE clause
	reStrUPDATEClause = `(?P<` + namedCaptureKeyUPDATEClause + `>UPDATE)`

	// reStrListAppend is the regular expression for list_append
	reStrListAppend = `(list_append\((.+),\s*(.+)\))`

	// reStrListSetAdd is the regular expression for list_set_add
	reStrStringSetAdd = `(set_add\((.+),\s*(.+)\))`

	// reStrSet is the regular expression for set type
	reStrSet = `(<<\s*(,?\s*(.+)\s*)*\s*>>)`

	// reStrList is the regular expression for list type
	reStrList = `(\[\s*(,?\s*(.+)\s*)*\s*\])`

	// reStrMap is the regular expression for map type
	reStrMap = `(\{\s*(,?(.+)\s*:\s*(.+))*\s*\})`

	// reStrS is the regular expression for string type
	reStrS = `"(.+)"`

	// reStrN is the regular expression for number type
	reStrN = `(\d+)`

	// reStrCollectionWithIndex is the regular expression for collection with index
	reStrCollectionWithIndex = `(([a-z0-9_\-\.]{3,255}(\.[a-z0-9_\-\.]{3,255})*\[(\d+|'(.+)')\]))`

	// reStrUPDATESet is the regular expression for UPDATE set
	reStrUPDATESet = `(?P<` + namedCaptureKeyUpdateSet + `>(((\s+SET\s+[a-z0-9_\-\.]{3,255}\s*=\s*(` +
		reStrS + `|` + reStrN + `|` + reStrList + `|` + reStrSet + `|` + reStrMap + `|` + reStrListAppend + `|` + reStrStringSetAdd + `|` + `\?` +
		`))|(\s+REMOVE\s+[a-z0-9_\-\.]{3,255}\s*=\s*` + reStrCollectionWithIndex + `))+))`

	// reStrUPDATEStatement is the regular expression for UPDATE statement
	reStrUPDATEStatement = `(?i)^\s*` + reStrUPDATEClause + `(?:\s+("[a-z0-9_\-\.]{3,255}"))` + reStrUPDATESet + `\s*` + reStrWHERECondition + `\s*$`

	// reStrDELETEClause is the regular expression for DELETE clause
	reStrDELETEClause = `(?P<` + namedCaptureKeyDELETEClause + `>DELETE)`

	// reStrDELETEStatement is the regular expression for DELETE statement
	reStrDELETEStatement = `(?i)^\s*` + reStrDELETEClause + `(?:\s+FROM\s+("[a-z0-9_\-\.]{3,255}")\s+)` + reStrWHERECondition + `\s*$`

	// reStrDescribeTable is the regular expression for describe table
	reStrDescribeTable = `(?i)^\s*(?:SELECT)\s+` + reStrSelectedList + `\s+(?:FROM\s+"!pqxd_describe_table"\s+)` + `(?:WHERE\s+table_name\s*=\s*)(?P<` + namedCaptureKeyWHERECondition + `>(\?|'([a-z0-9_\-\.]{3,255})'))\s*$`

	// reStrListTable is the regular expression for describe table
	reStrListTable = `(?i)^\s*(?:SELECT)\s+\*\s+(?:FROM\s+"!pqxd_list_tables")\s*$`
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

	// reRETURNING is the regular expression for RETURNING clause
	reRETURNING = regexp.MustCompile(reStrRETURNINGClause)

	// reDescribeTable is the regular expression for describe table
	reDescribeTable = regexp.MustCompile(reStrDescribeTable)

	// reListTable is the regular expression for list table
	reListTable = regexp.MustCompile(reStrListTable)
)

var (
	// returnableStatementRegexps is the list of regular expressions for returnable statements
	returnableStatementRegexps = []*regexp.Regexp{reSELECT, reUPDATE, reDELETE}
)

// preparedStatementFromQueryString returns prepared statement from the query string
func (c *connection) preparedStatementFromQueryString(query string) (stmt driver.Stmt, err error) {
	for _, regx := range returnableStatementRegexps {
		if match := regx.FindStringSubmatch(query); len(match) > 0 {
			tq := tokenize(query)
			stmt = newStatement(
				tq.queryString,
				tq.selectedList,
				countPlaceHolders(match, regx),
				c.query,
				c.ExecContext,
				c.newCloseCheckClosure(),
			)
			return
		}
	}
	if match := reINSERT.FindStringSubmatch(query); len(match) > 0 {
		stmt = newStatement(
			query,
			nil,
			countPlaceHolders(match, reINSERT),
			c.query,
			c.ExecContext,
			c.newCloseCheckClosure(),
		)
		return
	}
	if match := reDescribeTable.FindStringSubmatch(query); len(match) > 0 {
		tq := tokenize(query)
		stmt = newStatement(
			tq.queryString,
			tq.selectedList,
			countPlaceHolders(match, reDescribeTable),
			func(ctx context.Context, _ string, _ []string, args []driver.NamedValue) (driver.Rows, error) {
				return c.describeTable(ctx, tq.describeTableTarget, tq.selectedList, args)
			},
			c.ExecContext,
			c.newCloseCheckClosure(),
		)
		return
	}
	if match := reListTable.FindStringSubmatch(query); len(match) > 0 {
		stmt = newStatement(
			query,
			[]string{"*"},
			0,
			func(ctx context.Context, _ string, _ []string, _ []driver.NamedValue) (driver.Rows, error) {
				return c.listTables(ctx)
			},
			c.ExecContext,
			c.newCloseCheckClosure(),
		)
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
func newConnection(client DynamoDBClient) *connection {
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
func (c *connection) newTxGetAffected(inOut *transactionInOut, txCommit *txCommit) func() (int64, error) {
	return func() (int64, error) {
		select {
		case <-txCommit.ctx.Done():
			<-txCommit.receiveResult.Done()
		default:
			if c.txOngoing.Load() {
				return 0, nil
			}
		}
		if inOut.err != nil {
			return 0, inOut.err
		}
		return 1, nil
	}
}

type tokenizedQuery struct {
	queryString         string
	selectedListString  string
	selectedList        []string
	tableName           string
	whereCondition      string
	placeHolders        int
	describeTableTarget string
	listTable           bool
}

// tokenize tokenizes the query string
func tokenize(query string) (tq tokenizedQuery) {
	tq.queryString = query
	if match := reSELECT.FindStringSubmatch(query); len(match) > 0 {
		tq.selectedList, _ = selectedListFromMatchString(match, reSELECT, namedCaptureKeySelectedList)
		tq.tableName = extractTableNameFromMatchString(match)
		idx := reSELECT.SubexpIndex(namedCaptureKeyWHERECondition)
		if idx != -1 {
			tq.whereCondition = match[idx]
		}
		tq.placeHolders = countPlaceHolders(match, reSELECT)
		return
	}
	if match := reRETURNING.FindStringSubmatch(query); len(match) > 0 {
		tq.selectedList, tq.selectedListString = selectedListFromMatchString(
			match,
			reRETURNING,
			namedCaptureKeyRETURNINGSelectedList,
		)
		idx := reRETURNING.SubexpIndex(namedCaptureKeyRETURNINGSelectedList)
		if idx == -1 {
			tq = tokenizedQuery{}
			return
		}
		tq.whereCondition = match[idx]
		if tq.selectedListString != "*" {
			tq.queryString = strings.Replace(tq.queryString, tq.selectedListString, "*", 1)
		}
		return
	}
	if match := reDescribeTable.FindStringSubmatch(query); len(match) > 0 {
		tq.selectedList, _ = selectedListFromMatchString(match, reDescribeTable, namedCaptureKeySelectedList)
		idx := reDescribeTable.SubexpIndex(namedCaptureKeyWHERECondition)
		if idx == -1 {
			tq = tokenizedQuery{}
			return
		}
		tq.describeTableTarget = match[idx]
		return
	}
	if match := reListTable.FindStringSubmatch(query); len(match) > 0 {
		tq.selectedList = []string{"*"}
		tq.listTable = true
		return
	}
	return
}

// selectedListFromMatchString extracts selected list from the match string
func selectedListFromMatchString(match []string, regex *regexp.Regexp, namedCaptureKey string) (
	columns []string, rawSelectedList string,
) {
	index := regex.SubexpIndex(namedCaptureKey)
	if index == -1 {
		return
	}
	rawSelectedList = strings.TrimSpace(match[index])
	for _, v := range strings.Split(rawSelectedList, ",") {
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
