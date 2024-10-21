package pqxd

import (
	"context"
	"database/sql/driver"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/miyamo2/pqxd/internal"
	"go.uber.org/atomic"
	"go.uber.org/mock/gomock"
	"testing"
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
				return &connection{
					client: client,
				}
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
				return &connection{
					client: client,
				}
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
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			client := tt.dynamoDBClient(tt.ctx, ctrl)
			sut := tt.sut(client)
			got := sut.Ping(tt.ctx)
			if !errors.Is(tt.want, got) {
				t.Errorf("Ping() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_Connection_Close(t *testing.T) {
	type test struct {
		ctx            context.Context
		tx             func() transaction
		dynamoDBClient func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient
		sut            func(client internal.DynamoDBClient, tx transaction) *connection
		want           error
	}

	tests := map[string]test{
		"common": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient, tx transaction) *connection {
				return &connection{
					client: client,
					tx:     tx,
				}
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				return client
			},
			tx: func() transaction { return nil },
		},
		"ongoing-query-tx": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient, tx transaction) *connection {
				return &connection{
					client: client,
					tx:     tx,
				}
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				return client
			},
			tx: func() transaction { return &queryTx{} },
		},
		"ongoing-exec-tx": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient, tx transaction) *connection {
				return &connection{
					client: client,
					tx:     tx,
				}
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				return client
			},
			tx: func() transaction { return &execTx{} },
		},
		"closed-connection": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient, tx transaction) *connection {
				return &connection{
					client: client,
					tx:     tx,
					closed: *atomic.NewBool(true),
				}
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) internal.DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				return client
			},
			tx: func() transaction { return nil },
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			client := tt.dynamoDBClient(tt.ctx, ctrl)
			tx := tt.tx()
			sut := tt.sut(client, tx)
			got := sut.Close()
			if !errors.Is(tt.want, got) {
				t.Errorf("Close() = %v, want %v", got, tt.want)
			}
		})
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
		sut                     func(client internal.DynamoDBClient, tx transaction) *connection
		tx                      func() transaction
		executeStatementResults []ExecuteStatementResult
		args                    args
		want                    want
	}

	tests := map[string]test{
		"common": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient, tx transaction) *connection {
				return &connection{
					client: client,
					tx:     tx,
				}
			},
			tx: func() transaction { return nil },
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
		"ongoing-query-tx": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient, tx transaction) *connection {
				return &connection{
					client: client,
					tx:     tx,
				}
			},
			tx: func() transaction { return &queryTx{} },
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
		"ongoing-exec-tx": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient, tx transaction) *connection {
				return &connection{
					client: client,
					tx:     tx,
				}
			},
			tx: func() transaction { return &execTx{} },
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
			sut: func(client internal.DynamoDBClient, tx transaction) *connection {
				return &connection{
					client: client,
					tx:     tx,
					closed: *atomic.NewBool(true),
				}
			},
			tx: func() transaction { return nil },
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
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			qArgs := tt.args
			input := dynamodb.ExecuteStatementInput{
				Statement:  &qArgs.query,
				Parameters: MustPartiQLParameters(t, qArgs.args),
			}

			client := MockDynamoDBClient(t, ctrl, MockDynamoDBClientWithExecuteStatement(t, input, tt.executeStatementResults))
			tx := tt.tx()
			sut := tt.sut(client, tx)

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
		})
	}
}
