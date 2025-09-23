package integration

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/miyamo2/pqxd"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var (
	_ suite.SetupSubTest    = (*ExecTestSuite)(nil)
	_ suite.TearDownSubTest = (*ExecTestSuite)(nil)
)

type ExecTestSuite struct {
	suite.Suite
	client *dynamodb.Client
}

func TestExecTestSuite(t *testing.T) {
	suite.Run(t, &ExecTestSuite{client: GetClient(t)})
}

func (s *ExecTestSuite) SetupSubTest() {
	mu.Lock()
}

func (s *ExecTestSuite) TearDownSubTest() {
	defer mu.Unlock()
	testData := make([]map[string]types.AttributeValue, 0)

	var lastEvaluatedKey map[string]types.AttributeValue
	for {
		queryOutput, err := s.client.Query(
			context.Background(), &dynamodb.QueryInput{
				TableName:              aws.String("test_tables"),
				KeyConditionExpression: aws.String("#pk = :pk"),
				ExpressionAttributeNames: map[string]string{
					"#pk": "pk",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":pk": &types.AttributeValueMemberS{Value: "TestExecTestSuite"},
				},
				ExclusiveStartKey: lastEvaluatedKey,
			},
		)
		require.NoError(s.T(), err)
		testData = append(testData, queryOutput.Items...)
		lastEvaluatedKey = queryOutput.LastEvaluatedKey
		if len(lastEvaluatedKey) == 0 {
			break
		}
	}

	for _, item := range testData {
		input := &dynamodb.DeleteItemInput{
			Key: map[string]types.AttributeValue{
				"pk": item["pk"],
				"sk": item["sk"],
			},
			TableName: aws.String("test_tables"),
		}
		_, err := s.client.DeleteItem(context.Background(), input)
		require.NoError(s.T(), err)
	}
}

func (s *ExecTestSuite) Test_ExecContext() {
	s.Run(
		"insert/common", func() {
			db := GetDB(s.T())
			result, err := db.ExecContext(
				context.Background(),
				`INSERT INTO "test_tables" VALUE {'pk': ?, 'sk': ?, 'gsi_pk': ?, 'gsi_sk': ?}`,
				"TestExecTestSuite",
				1.0,
				"TestExecTestSuite1",
				"1",
			)
			require.NoError(s.T(), err)

			rowAffected, err := result.RowsAffected()
			require.NoError(s.T(), err)
			require.Equal(s.T(), int64(1), rowAffected)

			lastInsertedID, err := result.LastInsertId()
			require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
			require.Equal(s.T(), int64(0), lastInsertedID)

			expect := []TestTables{
				{
					PK:    "TestExecTestSuite",
					SK:    1.0,
					GSIPK: "TestExecTestSuite1",
					GSISK: "1",
				},
			}

			queryOutput, err := s.client.Query(
				context.Background(), &dynamodb.QueryInput{
					TableName:              aws.String("test_tables"),
					KeyConditionExpression: aws.String("#pk = :pk"),
					ExpressionAttributeNames: map[string]string{
						"#pk": "pk",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "TestExecTestSuite"},
					},
				},
			)
			require.NoError(s.T(), err)
			require.Len(s.T(), queryOutput.Items, 1)

			var actual []TestTables
			attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
			require.Exactly(s.T(), expect, actual)
		},
	)

	s.Run(
		"update/common", func() {
			PutTestTable(
				s.T(), TestTables{
					PK:    "TestExecTestSuite",
					SK:    1.0,
					GSIPK: "TestExecTestSuite1",
					GSISK: "1",
				},
			)

			db := GetDB(s.T())
			result, err := db.ExecContext(
				context.Background(),
				`UPDATE "test_tables" SET gsi_pk=? SET gsi_sk=? WHERE pk=? AND sk=?`,
				"TestExecTestSuite2",
				"2",
				"TestExecTestSuite",
				1.0,
			)
			require.NoError(s.T(), err)

			rowAffected, err := result.RowsAffected()
			require.NoError(s.T(), err)
			require.Equal(s.T(), int64(1), rowAffected)

			lastInsertedID, err := result.LastInsertId()
			require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
			require.Equal(s.T(), int64(0), lastInsertedID)

			expect := []TestTables{
				{
					PK:    "TestExecTestSuite",
					SK:    1.0,
					GSIPK: "TestExecTestSuite2",
					GSISK: "2",
				},
			}

			queryOutput, err := s.client.Query(
				context.Background(), &dynamodb.QueryInput{
					TableName:              aws.String("test_tables"),
					KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
					ExpressionAttributeNames: map[string]string{
						"#pk": "pk",
						"#sk": "sk",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "TestExecTestSuite"},
						":sk": &types.AttributeValueMemberN{Value: "1.0"},
					},
				},
			)
			require.NoError(s.T(), err)
			require.Len(s.T(), queryOutput.Items, 1)

			var actual []TestTables
			attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
			require.Exactly(s.T(), expect, actual)
		},
	)

	s.Run(
		"delete/common", func() {
			PutTestTable(
				s.T(), TestTables{
					PK:    "TestExecTestSuite",
					SK:    1.0,
					GSIPK: "TestExecTestSuite1",
					GSISK: "1",
				},
			)

			db := GetDB(s.T())
			result, err := db.ExecContext(
				context.Background(),
				`DELETE FROM "test_tables" WHERE pk=? AND sk=?`,
				"TestExecTestSuite",
				1.0,
			)
			require.NoError(s.T(), err)

			rowAffected, err := result.RowsAffected()
			require.NoError(s.T(), err)
			require.Equal(s.T(), int64(1), rowAffected)

			lastInsertedID, err := result.LastInsertId()
			require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
			require.Equal(s.T(), int64(0), lastInsertedID)

			queryOutput, err := s.client.Query(
				context.Background(), &dynamodb.QueryInput{
					TableName:              aws.String("test_tables"),
					KeyConditionExpression: aws.String("#pk = :pk"),
					ExpressionAttributeNames: map[string]string{
						"#pk": "pk",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "TestExecTestSuite"},
					},
				},
			)
			require.NoError(s.T(), err)
			require.Len(s.T(), queryOutput.Items, 0)
		},
	)
}

func (s *ExecTestSuite) Test_Exec() {
	s.Run(
		"insert/common", func() {
			db := GetDB(s.T())
			result, err := db.Exec(
				`INSERT INTO "test_tables" VALUE {'pk': ?, 'sk': ?, 'gsi_pk': ?, 'gsi_sk': ?}`,
				"TestExecTestSuite",
				1.0,
				"TestExecTestSuite1",
				"1",
			)
			require.NoError(s.T(), err)

			rowAffected, err := result.RowsAffected()
			require.NoError(s.T(), err)
			require.Equal(s.T(), int64(1), rowAffected)

			lastInsertedID, err := result.LastInsertId()
			require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
			require.Equal(s.T(), int64(0), lastInsertedID)

			expect := []TestTables{
				{
					PK:    "TestExecTestSuite",
					SK:    1.0,
					GSIPK: "TestExecTestSuite1",
					GSISK: "1",
				},
			}

			queryOutput, err := s.client.Query(
				context.Background(), &dynamodb.QueryInput{
					TableName:              aws.String("test_tables"),
					KeyConditionExpression: aws.String("#pk = :pk"),
					ExpressionAttributeNames: map[string]string{
						"#pk": "pk",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "TestExecTestSuite"},
					},
				},
			)
			require.NoError(s.T(), err)
			require.Len(s.T(), queryOutput.Items, 1)

			var actual []TestTables
			attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
			require.Exactly(s.T(), expect, actual)
		},
	)

	s.Run(
		"update/common", func() {
			PutTestTable(
				s.T(), TestTables{
					PK:    "TestExecTestSuite",
					SK:    1.0,
					GSIPK: "TestExecTestSuite1",
					GSISK: "1",
				},
			)

			db := GetDB(s.T())
			result, err := db.Exec(
				`UPDATE "test_tables" SET gsi_pk=? SET gsi_sk=? WHERE pk=? AND sk=?`,
				"TestExecTestSuite2",
				"2",
				"TestExecTestSuite",
				1.0,
			)
			require.NoError(s.T(), err)

			rowAffected, err := result.RowsAffected()
			require.NoError(s.T(), err)
			require.Equal(s.T(), int64(1), rowAffected)

			lastInsertedID, err := result.LastInsertId()
			require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
			require.Equal(s.T(), int64(0), lastInsertedID)

			expect := []TestTables{
				{
					PK:    "TestExecTestSuite",
					SK:    1.0,
					GSIPK: "TestExecTestSuite2",
					GSISK: "2",
				},
			}

			queryOutput, err := s.client.Query(
				context.Background(), &dynamodb.QueryInput{
					TableName:              aws.String("test_tables"),
					KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
					ExpressionAttributeNames: map[string]string{
						"#pk": "pk",
						"#sk": "sk",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "TestExecTestSuite"},
						":sk": &types.AttributeValueMemberN{Value: "1.0"},
					},
				},
			)
			require.NoError(s.T(), err)
			require.Len(s.T(), queryOutput.Items, 1)

			var actual []TestTables
			attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
			require.Exactly(s.T(), expect, actual)
		},
	)

	s.Run(
		"delete/common", func() {
			PutTestTable(
				s.T(), TestTables{
					PK:    "TestExecTestSuite",
					SK:    1.0,
					GSIPK: "TestExecTestSuite1",
					GSISK: "1",
				},
			)

			db := GetDB(s.T())
			result, err := db.Exec(`DELETE FROM "test_tables" WHERE pk=? AND sk=?`, "TestExecTestSuite", 1.0)
			require.NoError(s.T(), err)

			rowAffected, err := result.RowsAffected()
			require.NoError(s.T(), err)
			require.Equal(s.T(), int64(1), rowAffected)

			lastInsertedID, err := result.LastInsertId()
			require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
			require.Equal(s.T(), int64(0), lastInsertedID)

			queryOutput, err := s.client.Query(
				context.Background(), &dynamodb.QueryInput{
					TableName:              aws.String("test_tables"),
					KeyConditionExpression: aws.String("#pk = :pk"),
					ExpressionAttributeNames: map[string]string{
						"#pk": "pk",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "TestExecTestSuite"},
					},
				},
			)
			require.NoError(s.T(), err)
			require.Len(s.T(), queryOutput.Items, 0)
		},
	)
}

func (s *ExecTestSuite) Test_PrepareContext() {
	s.Run(
		"insert/common", func() {
			db := GetDB(s.T())
			stmt, err := db.PrepareContext(
				context.Background(),
				`INSERT INTO "test_tables" VALUE {'pk': ?, 'sk': ?, 'gsi_pk': ?, 'gsi_sk': ?}`,
			)
			require.NoError(s.T(), err)

			result, err := stmt.ExecContext(context.Background(), "TestExecTestSuite", 1.0, "TestExecTestSuite1", "1")
			require.NoError(s.T(), err)

			rowAffected, err := result.RowsAffected()
			require.NoError(s.T(), err)
			require.Equal(s.T(), int64(1), rowAffected)

			lastInsertedID, err := result.LastInsertId()
			require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
			require.Equal(s.T(), int64(0), lastInsertedID)

			expect := []TestTables{
				{
					PK:    "TestExecTestSuite",
					SK:    1.0,
					GSIPK: "TestExecTestSuite1",
					GSISK: "1",
				},
			}

			queryOutput, err := s.client.Query(
				context.Background(), &dynamodb.QueryInput{
					TableName:              aws.String("test_tables"),
					KeyConditionExpression: aws.String("#pk = :pk"),
					ExpressionAttributeNames: map[string]string{
						"#pk": "pk",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "TestExecTestSuite"},
					},
				},
			)
			require.NoError(s.T(), err)
			require.Len(s.T(), queryOutput.Items, 1)

			var actual []TestTables
			attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
			require.Exactly(s.T(), expect, actual)
		},
	)

	s.Run(
		"update/common", func() {
			PutTestTable(
				s.T(), TestTables{
					PK:    "TestExecTestSuite",
					SK:    1.0,
					GSIPK: "TestExecTestSuite1",
					GSISK: "1",
				},
			)

			db := GetDB(s.T())
			stmt, err := db.PrepareContext(
				context.Background(),
				`UPDATE "test_tables" SET gsi_pk=? SET gsi_sk=? WHERE pk = ? AND sk = ?`,
			)
			require.NoError(s.T(), err)

			result, err := stmt.ExecContext(context.Background(), "TestExecTestSuite2", "2", "TestExecTestSuite", 1.0)
			require.NoError(s.T(), err)

			rowAffected, err := result.RowsAffected()
			require.NoError(s.T(), err)
			require.Equal(s.T(), int64(1), rowAffected)

			lastInsertedID, err := result.LastInsertId()
			require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
			require.Equal(s.T(), int64(0), lastInsertedID)

			expect := []TestTables{
				{
					PK:    "TestExecTestSuite",
					SK:    1.0,
					GSIPK: "TestExecTestSuite2",
					GSISK: "2",
				},
			}

			queryOutput, err := s.client.Query(
				context.Background(), &dynamodb.QueryInput{
					TableName:              aws.String("test_tables"),
					KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
					ExpressionAttributeNames: map[string]string{
						"#pk": "pk",
						"#sk": "sk",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "TestExecTestSuite"},
						":sk": &types.AttributeValueMemberN{Value: "1.0"},
					},
				},
			)
			require.NoError(s.T(), err)
			require.Len(s.T(), queryOutput.Items, 1)

			var actual []TestTables
			attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
			require.Exactly(s.T(), expect, actual)
		},
	)

	s.Run(
		"delete/common", func() {
			PutTestTable(
				s.T(), TestTables{
					PK:    "TestExecTestSuite",
					SK:    1.0,
					GSIPK: "TestExecTestSuite1",
					GSISK: "1",
				},
			)

			db := GetDB(s.T())
			stmt, err := db.PrepareContext(context.Background(), `DELETE FROM "test_tables" WHERE pk = ? AND sk = ?`)
			require.NoError(s.T(), err)

			result, err := stmt.ExecContext(context.Background(), "TestExecTestSuite", 1.0)
			require.NoError(s.T(), err)

			rowAffected, err := result.RowsAffected()
			require.NoError(s.T(), err)
			require.Equal(s.T(), int64(1), rowAffected)

			lastInsertedID, err := result.LastInsertId()
			require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
			require.Equal(s.T(), int64(0), lastInsertedID)

			queryOutput, err := s.client.Query(
				context.Background(), &dynamodb.QueryInput{
					TableName:              aws.String("test_tables"),
					KeyConditionExpression: aws.String("#pk = :pk"),
					ExpressionAttributeNames: map[string]string{
						"#pk": "pk",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "TestExecTestSuite"},
					},
				},
			)
			require.NoError(s.T(), err)
			require.Len(s.T(), queryOutput.Items, 0)
		},
	)
}
