package pqxd

import (
	"database/sql/driver"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/ovechkin-dm/mockio/v2/mock"
)

var CmpAttributeValuesOpt = []cmp.Option{
	cmp.AllowUnexported(types.AttributeValueMemberB{}),
	cmp.AllowUnexported(types.AttributeValueMemberBOOL{}),
	cmp.AllowUnexported(types.AttributeValueMemberBS{}),
	cmp.AllowUnexported(types.AttributeValueMemberL{}),
	cmp.AllowUnexported(types.AttributeValueMemberM{}),
	cmp.AllowUnexported(types.AttributeValueMemberN{}),
	cmp.AllowUnexported(types.AttributeValueMemberNS{}),
	cmp.AllowUnexported(types.AttributeValueMemberNULL{}),
	cmp.AllowUnexported(types.AttributeValueMemberS{}),
	cmp.AllowUnexported(types.AttributeValueMemberSS{}),
}

var CmpExecuteStatementInputOpt = append(
	[]cmp.Option{
		cmpopts.IgnoreFields(
			dynamodb.ExecuteStatementInput{},
			"noSmithyDocumentSerde",
			"NextToken",
		),
	}, CmpAttributeValuesOpt...,
)

func ExecuteStatementInputEqual(
	want *dynamodb.ExecuteStatementInput,
) func() *dynamodb.ExecuteStatementInput {
	return CreateMatcher[*dynamodb.ExecuteStatementInput](
		"ExecuteStatementInput",
		func(allArgs []any, actual *dynamodb.ExecuteStatementInput) bool {
			if want == nil {
				return false
			}
			if actual == nil {
				return false
			}
			if diff := cmp.Diff(*actual, *want, CmpExecuteStatementInputOpt...); diff != "" {
				return false
			}
			return true
		},
	)
}

func MustPartiQLParameters(t *testing.T, args []driver.NamedValue) []types.AttributeValue {
	t.Helper()
	params, err := toPartiQLParameters(args)
	if err != nil {
		t.Fatalf("failed to convert to PartiQL parameters: %v", err)
	}
	return params
}

type ExecuteStatementResult struct {
	out *dynamodb.ExecuteStatementOutput
	err error
}

func ExceptExecuteStatement(
	t *testing.T,
	client DynamoDBClient,
	input dynamodb.ExecuteStatementInput,
	executeStatementResults []ExecuteStatementResult,
) {
	t.Helper()
	returnDouble := WhenDouble(client.ExecuteStatement(AnyContext(), ExecuteStatementInputEqual(&input)()))
	for _, esr := range executeStatementResults {
		returnDouble = returnDouble.ThenReturn(esr.out, esr.err)
	}
}

func GetAllResultSet(t *testing.T, rows driver.Rows) ([][]map[string]types.AttributeValue, error) {
	t.Helper()
	var results [][]map[string]types.AttributeValue
	switch rs := (any)(rows).(type) {
	case *pqxdRows:
		for rs.NextResultSet() == nil {
			resultSet := *rs.out.Load()
			results = append(results, resultSet)
			rs.outCursor.Store(uint32(len(resultSet)))
		}
	}
	return results, nil
}
