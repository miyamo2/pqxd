package pqxd

import (
	"database/sql/driver"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/miyamo2/pqxd/internal"
	"go.uber.org/mock/gomock"
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
		),
	}, CmpAttributeValuesOpt...,
)

func CndExecuteStatementInput(t *testing.T, want *dynamodb.ExecuteStatementInput) func(x any) bool {
	t.Helper()
	return func(got any) bool {
		if got == nil {
			return want == nil
		}
		if want == nil {
			return false
		}
		actual, ok := got.(*dynamodb.ExecuteStatementInput)
		if !ok {
			return false
		}
		if actual == nil {
			return false
		}
		if diff := cmp.Diff(*actual, *want, CmpExecuteStatementInputOpt...); diff != "" {
			t.Fatalf("unexpected difference: %v", diff)
			return false
		}
		return true
	}
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

type MockDynamoDBClientOption func(*internal.MockDynamoDBClient)

func MockDynamoDBClientWithExecuteStatement(
	t *testing.T, input dynamodb.ExecuteStatementInput, executeStatementResults []ExecuteStatementResult,
) func(client *internal.MockDynamoDBClient) {
	return func(client *internal.MockDynamoDBClient) {
		for _, esr := range executeStatementResults {
			client.EXPECT().
				ExecuteStatement(gomock.Any(), gomock.Cond(CndExecuteStatementInput(t, &input))).
				Times(1).
				Return(esr.out, esr.err)
			input = dynamodb.ExecuteStatementInput{
				Statement:  input.Statement,
				Parameters: input.Parameters,
				NextToken:  esr.out.NextToken,
			}
		}
	}
}

func MockDynamoDBClient(
	t *testing.T, ctrl *gomock.Controller, opts ...MockDynamoDBClientOption,
) DynamoDBClient {
	t.Helper()
	client := internal.NewMockDynamoDBClient(ctrl)
	for _, opt := range opts {
		opt(client)
	}
	return client
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
