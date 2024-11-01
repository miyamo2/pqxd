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
