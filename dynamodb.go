//go:generate mockgen -source=dynamodb.go --package=internal -destination=./internal/dynamodb_mock.go
package pqxd

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// compatibility check
var _ DynamoDBClient = (*dynamodb.Client)(nil)

// DynamoDBClient provides access to DynamoDB API methods used by this driver.
//
// See: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client
type DynamoDBClient interface {
	BatchExecuteStatement(
		ctx context.Context, params *dynamodb.BatchExecuteStatementInput, optFns ...func(*dynamodb.Options),
	) (*dynamodb.BatchExecuteStatementOutput, error)
	ExecuteStatement(
		ctx context.Context, params *dynamodb.ExecuteStatementInput, optFns ...func(*dynamodb.Options),
	) (*dynamodb.ExecuteStatementOutput, error)
	ExecuteTransaction(
		ctx context.Context, params *dynamodb.ExecuteTransactionInput, optFns ...func(*dynamodb.Options),
	) (*dynamodb.ExecuteTransactionOutput, error)
	CreateTable(
		ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options),
	) (*dynamodb.CreateTableOutput, error)
	UpdateTable(
		ctx context.Context, params *dynamodb.UpdateTableInput, optFns ...func(*dynamodb.Options),
	) (*dynamodb.UpdateTableOutput, error)
	DeleteTable(
		ctx context.Context, params *dynamodb.DeleteTableInput, optFns ...func(*dynamodb.Options),
	) (*dynamodb.DeleteTableOutput, error)
	DescribeTable(
		ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options),
	) (*dynamodb.DescribeTableOutput, error)
	ListTables(
		ctx context.Context, params *dynamodb.ListTablesInput, optFns ...func(*dynamodb.Options),
	) (*dynamodb.ListTablesOutput, error)
}
