package pqxd

import (
	"context"
	"database/sql/driver"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var describeTableColumns = []string{
	"ArchivalSummary",
	"AttributeDefinitions",
	"BillingModeSummary",
	"CreationDateTime",
	"DeletionProtectionEnabled",
	"KeySchema",
	"GlobalSecondaryIndexes",
	"GlobalTableVersion",
	"ItemCount",
	"LocalSecondaryIndexes",
	"OnDemandThroughput",
	"ProvisionedThroughput",
	"Replicas",
	"RestoreSummary",
	"SSEDescription",
	"StreamSpecification",
	"TableClassSummary",
	"TableStatus",
}

// describeTable performs a DescribeTable API.
// See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.DeleteTable
func (c *connection) describeTable(ctx context.Context, targetTable string, selectedList []string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}
	if c.txOngoing.Load() {
		return nil, ErrNotSupportedWithinTx
	}
	if targetTable == "?" {
		if len(args) != 0 {
			targetTable = args[0].Value.(string)
		}
	}
	output, err := c.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &targetTable,
	})
	if err != nil {
		return nil, err
	}
	if output.Table == nil {
		return nil, nil
	}
	table := output.Table
	if table == nil {
		table = &types.TableDescription{}
	}
	if selectedList[0] == "*" {
		selectedList = describeTableColumns
	}

	return newDescribeTableRows(selectedList, *table), nil
}

var listTablesRowsColumns = []string{"TableName"}

// listTables performs a ListTables API.
// See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.ListTables
func (c *connection) listTables(ctx context.Context) (driver.Rows, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}
	if c.txOngoing.Load() {
		return nil, ErrNotSupportedWithinTx
	}

	fetch := c.newListTablesFetchClosure()

	var out []map[string]types.AttributeValue
	lastEvaluatedTable, err := fetch(ctx, nil, &out)
	if err != nil {
		return nil, err
	}
	return newRows(listTablesRowsColumns, lastEvaluatedTable, fetch, out), nil
}

// newListTablesFetchClosure returns a fetchClosure for ListTables API.
func (c *connection) newListTablesFetchClosure() fetchClosure {
	return func(ctx context.Context, lastEvaluatedTableName *string, dest *[]map[string]types.AttributeValue) (*string, error) {
		output, err := c.client.ListTables(ctx, &dynamodb.ListTablesInput{ExclusiveStartTableName: lastEvaluatedTableName})
		if err != nil {
			return nil, err
		}
		*dest = tablesNamesToExecuteStatementOutputItems(output.TableNames)
		return output.LastEvaluatedTableName, nil
	}
}

// tableNamesToExecuteStatementOutputItems converts a list of table names to a list of ExecuteStatementOutput items.
func tablesNamesToExecuteStatementOutputItems(s []string) []map[string]types.AttributeValue {
	var items []map[string]types.AttributeValue
	for _, v := range s {
		items = append(items, map[string]types.AttributeValue{
			"TableName": &types.AttributeValueMemberS{Value: v},
		})
	}
	return items
}
