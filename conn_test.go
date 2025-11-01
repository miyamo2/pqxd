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
		dynamoDBClient func(ctx context.Context, ctrl *gomock.Controller) DynamoDBClient
		sut            func(client DynamoDBClient) *connection
		want           error
	}

	someErr := errors.New("some error")
	tests := map[string]test{
		"common": {
			ctx: context.Background(),
			sut: func(client DynamoDBClient) *connection {
				return newConnection(client)
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				client.EXPECT().ListTables(ctx, nil).Times(1).Return(nil, nil)
				return client
			},
		},
		"client-returns-error": {
			ctx: context.Background(),
			sut: func(client DynamoDBClient) *connection {
				return newConnection(client)
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				client.EXPECT().ListTables(ctx, nil).Times(1).Return(nil, someErr)
				return client
			},
			want: someErr,
		},
		"closed-connection": {
			ctx: context.Background(),
			sut: func(client DynamoDBClient) *connection {
				return &connection{
					client: client,
					closed: *atomic.NewBool(true),
				}
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				client.EXPECT().ListTables(gomock.Any(), gomock.Any()).Times(0)
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
		dynamoDBClient func(ctx context.Context, ctrl *gomock.Controller) DynamoDBClient
		sut            func(client DynamoDBClient) *connection
		want           error
	}

	tests := map[string]test{
		"common": {
			ctx: context.Background(),
			sut: func(client DynamoDBClient) *connection {
				return newConnection(client)
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				return client
			},
		},
		"ongoing-query-tx": {
			ctx: context.Background(),
			sut: func(client DynamoDBClient) *connection {
				return newConnection(client)
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				return client
			},
		},
		"ongoing-exec-tx": {
			ctx: context.Background(),
			sut: func(client DynamoDBClient) *connection {
				return newConnection(client)
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) DynamoDBClient {
				client := internal.NewMockDynamoDBClient(ctrl)
				return client
			},
		},
		"closed-connection": {
			ctx: context.Background(),
			sut: func(client DynamoDBClient) *connection {
				return &connection{
					client: client,
					closed: *atomic.NewBool(true),
				}
			},
			dynamoDBClient: func(ctx context.Context, ctrl *gomock.Controller) DynamoDBClient {
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
		sut                     func(client DynamoDBClient) *connection
		executeStatementResults []ExecuteStatementResult
		args                    args
		want                    want
	}

	tests := map[string]test{
		"common": {
			ctx: context.Background(),
			sut: func(client DynamoDBClient) *connection {
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
			sut: func(client DynamoDBClient) *connection {
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
		"double-quoted-columns": {
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
						},
					},
				},
			},
			args: args{
				query: `SELECT "id", "name" FROM "users"`,
				args:  []driver.NamedValue{},
			},
			want: want{
				resultSets: [][]map[string]types.AttributeValue{
					{
						{
							"id":   &types.AttributeValueMemberS{Value: "1"},
							"name": &types.AttributeValueMemberS{Value: "Alice"},
						},
					},
				},
			},
		},
		"mixed-quoted-and-unquoted-columns": {
			ctx: context.Background(),
			sut: func(client internal.DynamoDBClient) *connection {
				return newConnection(client)
			},
			executeStatementResults: []ExecuteStatementResult{
				{
					out: &dynamodb.ExecuteStatementOutput{
						Items: []map[string]types.AttributeValue{
							{
								"id":   &types.AttributeValueMemberS{Value: "2"},
								"name": &types.AttributeValueMemberS{Value: "Bob"},
							},
						},
					},
				},
			},
			args: args{
				query: `SELECT "id", name FROM "users" WHERE disabled = ?`,
				args: []driver.NamedValue{
					{Value: false},
				},
			},
			want: want{
				resultSets: [][]map[string]types.AttributeValue{
					{
						{
							"id":   &types.AttributeValueMemberS{Value: "2"},
							"name": &types.AttributeValueMemberS{Value: "Bob"},
						},
					},
				},
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

func Test_tokenize_with_double_quoted_columns(t *testing.T) {
	type test struct {
		query       string
		wantColumns []string
	}

	tests := map[string]test{
		"unquoted-columns": {
			query:       `SELECT id, name FROM "users"`,
			wantColumns: []string{"id", "name"},
		},
		"double-quoted-columns": {
			query:       `SELECT "id", "name" FROM "users"`,
			wantColumns: []string{"id", "name"},
		},
		"mixed-quoted-columns": {
			query:       `SELECT "id", name FROM "users"`,
			wantColumns: []string{"id", "name"},
		},
		"single-quoted-column": {
			query:       `SELECT "id" FROM "users"`,
			wantColumns: []string{"id"},
		},
		"with-where-clause": {
			query:       `SELECT "id", "name" FROM "users" WHERE id = ?`,
			wantColumns: []string{"id", "name"},
		},
		"asterisk": {
			query:       `SELECT * FROM "users"`,
			wantColumns: []string{"*"},
		},
		"returning-clause-unquoted": {
			query:       `UPDATE "users" SET name = ? WHERE id = ? RETURNING ALL OLD id, name`,
			wantColumns: []string{"id", "name"},
		},
		"returning-clause-quoted": {
			query:       `UPDATE "users" SET name = ? WHERE id = ? RETURNING ALL OLD "id", "name"`,
			wantColumns: []string{"id", "name"},
		},
		"returning-clause-mixed": {
			query:       `UPDATE "users" SET name = ? WHERE id = ? RETURNING MODIFIED OLD "id", name`,
			wantColumns: []string{"id", "name"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			tq := tokenize(tt.query)

			// Check if we got the expected number of columns
			if len(tq.selectedList) != len(tt.wantColumns) {
				t.Errorf("tokenize() got %d columns, want %d", len(tq.selectedList), len(tt.wantColumns))
				return
			}

			// Check each column matches
			for i, wantCol := range tt.wantColumns {
				if tq.selectedList[i] != wantCol {
					t.Errorf("tokenize() column[%d] = %q, want %q", i, tq.selectedList[i], wantCol)
				}
			}
		})
	}
}

func Test_Connection_PrepareContext_with_double_quoted_columns(t *testing.T) {
	type test struct {
		query     string
		wantError error
	}

	tests := map[string]test{
		"select-with-double-quoted-columns": {
			query:     `SELECT "id", "name" FROM "users" WHERE id = ?`,
			wantError: nil,
		},
		"select-with-mixed-quoted-columns": {
			query:     `SELECT "id", name FROM "users"`,
			wantError: nil,
		},
		"update-returning-double-quoted": {
			query:     `UPDATE "users" SET name = ? WHERE id = ? RETURNING ALL OLD "id", "name"`,
			wantError: nil,
		},
		"insert-statement": {
			query:     `INSERT INTO "users" VALUE { 'id': ?, 'name': ? }`,
			wantError: nil,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			client := internal.NewMockDynamoDBClient(ctrl)
			conn := newConnection(client)

			stmt, err := conn.PrepareContext(context.Background(), tt.query)
			if tt.wantError != nil {
				if !errors.Is(err, tt.wantError) {
					t.Errorf("PrepareContext() error = %v, want %v", err, tt.wantError)
				}
				return
			}

			if err != nil {
				t.Errorf("PrepareContext() unexpected error = %v", err)
				return
			}

			if stmt == nil {
				t.Error("PrepareContext() returned nil statement")
				return
			}

			// Clean up
			stmt.Close()
		})
	}
}
