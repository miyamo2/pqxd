package pqxd

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/miyamo2/pqxd/internal"
	"go.uber.org/atomic"
	"go.uber.org/mock/gomock"
)

func Test_Connection_Ping(t *testing.T) {
	type test struct {
		ctx            context.Context
		dynamoDBClient func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient
		sut            func(client internal.DynamoDBClient) *connection
		want           error
	}

	someErr := errors.New("some error")
	tests := map[string]test{
		"common": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient) *connection {
				return newConnection(client)
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				client.EXPECT().DescribeEndpoints(ctx, nil).Times(1).Return(nil, nil)
				return client
			},
		},
		"client-returns-error": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient) *connection {
				return newConnection(client)
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				client.EXPECT().DescribeEndpoints(ctx, nil).Times(1).Return(nil, someErr)
				return client
			},
			want: someErr,
		},
		"closed-connection": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient) *connection {
				return &connection{
					client: client,
					closed: *atomic.NewBool(true),
				}
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				client.EXPECT().DescribeEndpoints(gomock.Any(), gomock.Any()).Times(0)
				return client
			},
			want: driver.ErrBadConn,
		},
	}

	for name, tt := range tests {
		t.Run(
			name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				client := tt.dynamoDBClient(tt.ctx, ctrl)
				sut := tt.sut(client)
				got := sut.Ping(tt.ctx)
				if !errors.Is(tt.want, got) {
					t.Errorf("Ping() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func Test_Connection_Close(t *testing.T) {
	type test struct {
		ctx            context.Context
		dynamoDBClient func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient
		sut            func(client internal.DynamoDBClient) *connection
		want           error
	}

	tests := map[string]test{
		"common": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient) *connection {
				return newConnection(client)
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				return client
			},
		},
		"ongoing-query-tx": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient) *connection {
				return newConnection(client)
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				return client
			},
		},
		"ongoing-exec-tx": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient) *connection {
				return newConnection(client)
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				return client
			},
		},
		"closed-connection": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient) *connection {
				return &connection{
					client: client,
					closed: *atomic.NewBool(true),
				}
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				return client
			},
		},
	}

	for name, tt := range tests {
		t.Run(
			name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				client := tt.dynamoDBClient(tt.ctx, ctrl)
				sut := tt.sut(client)
				got := sut.Close()
				if !errors.Is(tt.want, got) {
					t.Errorf("function() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func Test_pqxdRows_QueryContext(t *testing.T) {
	type want struct {
		resultSets [][]map[string]types.AttributeValue
		err        error
	}
	type args struct {
		query string
		args  []driver.NamedValue
	}
	type test struct {
		ctx                     context.Context
		sut                     func(client internal.DynamoDBClient) *connection
		executeStatementResults []ExecuteStatementResult
		args                    args
		want                    want
	}

	tests := map[string]test{
		"common": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient) *connection {
				return newConnection(client)
			},
			executeStatementResults: []ExecuteStatementResult{
				{
					out: &dynamodb.ExecuteStatementOutput{
						Items: []map[string]types.AttributeValue{
							{
								"id":   &types.AttributeValueMemberS{Value: "1"},
								"name": &types.AttributeValueMemberS{Value: "Alice"},
							},
							{
								"id":   &types.AttributeValueMemberS{Value: "2"},
								"name": &types.AttributeValueMemberS{Value: "Bob"},
							},
						},
						NextToken: aws.String("1"),
					},
				},
				{
					out: &dynamodb.ExecuteStatementOutput{
						Items: []map[string]types.AttributeValue{
							{
								"id":   &types.AttributeValueMemberS{Value: "3"},
								"name": &types.AttributeValueMemberS{Value: "Charlie"},
							},
							{
								"id":   &types.AttributeValueMemberS{Value: "4"},
								"name": &types.AttributeValueMemberS{Value: "David"},
							},
						},
					},
				},
			},
			args: args{
				query: `SELECT id, name FROM "users" WHERE disabled = ?`,
				args: []driver.NamedValue{
					{Value: false},
				},
			},
			want: want{
				resultSets: [][]map[string]types.AttributeValue{
					{
						{
							"id":   &types.AttributeValueMemberS{Value: "1"},
							"name": &types.AttributeValueMemberS{Value: "Alice"},
						},
						{
							"id":   &types.AttributeValueMemberS{Value: "2"},
							"name": &types.AttributeValueMemberS{Value: "Bob"},
						},
					},
					{
						{
							"id":   &types.AttributeValueMemberS{Value: "3"},
							"name": &types.AttributeValueMemberS{Value: "Charlie"},
						},
						{
							"id":   &types.AttributeValueMemberS{Value: "4"},
							"name": &types.AttributeValueMemberS{Value: "David"},
						},
					},
				},
			},
		},
		"closed-connection": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient) *connection {
				return &connection{
					client: client,
					closed: *atomic.NewBool(true),
				}
			},
			args: args{
				query: `SELECT id, name FROM "users" WHERE disabled = ?`,
				args: []driver.NamedValue{
					{Value: false},
				},
			},
			want: want{
				err: driver.ErrBadConn,
			},
		},
	}

	for name, tt := range tests {
		t.Run(
			name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				qArgs := tt.args
				input := dynamodb.ExecuteStatementInput{
					Statement:  &qArgs.query,
					Parameters: MustPartiQLParameters(t, qArgs.args),
				}

				client := MockDynamoDBClient(
					t,
					ctrl,
					MockDynamoDBClientWithExecuteStatement(t, input, tt.executeStatementResults),
				)
				sut := tt.sut(client)

				got, err := sut.QueryContext(tt.ctx, qArgs.query, qArgs.args)
				if !errors.Is(err, tt.want.err) {
					t.Errorf("QueryContext().error %+v, want %+v", err, tt.want.err)
				}
				results, err := GetAllResultSet(t, got)
				if err != nil {
					t.Fatalf("failed to scan results: %v", err)
					return
				}
				if diff := cmp.Diff(tt.want.resultSets, results, CmpAttributeValuesOpt...); diff != "" {
					t.Errorf("QueryContext().out mismatch (-want +got):\n%s", diff)
				}
			},
		)
	}
}
