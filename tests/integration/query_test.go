package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/miyamo2/pqxd"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"strconv"
	"testing"
)

type QueryTestSuite struct {
	suite.Suite
	client *dynamodb.Client
}

func TestQueryTestSuite(t *testing.T) {
	suite.Run(t, &QueryTestSuite{client: GetClient(t)})
}

// tenTimes for testing custom scanner
type tenTimes string

func (t *tenTimes) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		*t = tenTimes(strconv.Itoa(i * 10))
	}
	return nil
}

func (s *QueryTestSuite) testData() []map[string]types.AttributeValue {
	return []map[string]types.AttributeValue{
		{
			"pk": &types.AttributeValueMemberS{
				Value: "TestQueryTestSuite",
			},
			"sk": &types.AttributeValueMemberN{
				Value: "1.0",
			},
			"gsi_pk": &types.AttributeValueMemberS{
				Value: "TestQueryTestSuite1",
			},
			"gsi_sk": &types.AttributeValueMemberS{
				Value: "1",
			},
		},
		{
			"pk": &types.AttributeValueMemberS{
				Value: "TestQueryTestSuite",
			},
			"sk": &types.AttributeValueMemberN{
				Value: "2.0",
			},
			"gsi_pk": &types.AttributeValueMemberS{
				Value: "TestQueryTestSuite2",
			},
			"gsi_sk": &types.AttributeValueMemberS{
				Value: "2",
			},
		},
		{
			"pk": &types.AttributeValueMemberS{
				Value: "TestQueryTestSuite",
			},
			"sk": &types.AttributeValueMemberN{
				Value: "3",
			},
			"gsi_pk": &types.AttributeValueMemberS{
				Value: "TestQueryTestSuite3",
			},
			"gsi_sk": &types.AttributeValueMemberS{
				Value: "3",
			},
		},
		{
			"pk": &types.AttributeValueMemberS{
				Value: "TestQueryTestSuite",
			},
			"sk": &types.AttributeValueMemberN{
				Value: "4",
			},
			"gsi_pk": &types.AttributeValueMemberS{
				Value: "TestQueryTestSuite4",
			},
			"gsi_sk": &types.AttributeValueMemberS{
				Value: "4",
			},
		},
		{
			"pk": &types.AttributeValueMemberS{
				Value: "TestQueryTestSuite",
			},
			"sk": &types.AttributeValueMemberN{
				Value: "5",
			},
			"gsi_pk": &types.AttributeValueMemberS{
				Value: "TestQueryTestSuite5",
			},
			"gsi_sk": &types.AttributeValueMemberS{
				Value: "5",
			},
		},
	}
}

func (s *QueryTestSuite) SetupSubTest() {
	var items []types.TransactWriteItem
	for _, item := range s.testData() {
		put := &types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String("test_tables"),
				Item:      item,
			},
		}
		items = append(items, *put)
	}
	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	}
	_, err := s.client.TransactWriteItems(context.Background(), input)
	require.NoError(s.T(), err)
}

func (s *QueryTestSuite) TearDownSubTest() {
	for _, item := range s.testData() {
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

func (s *QueryTestSuite) Test_QueryRowContext() {
	s.Run("with-pk-and-sk", func() {
		db := GetDB(s.T())
		row := db.QueryRowContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "TestQueryTestSuite", 1.0)

		expect := TestTables{
			PK:    "TestQueryTestSuite",
			SK:    1.0,
			GSIPK: "TestQueryTestSuite1",
			GSISK: "1",
		}

		var (
			pk    string
			sk    float64
			gsiPk string
			gsiSk string
		)

		require.NoError(s.T(), row.Scan(&pk, &sk, &gsiPk, &gsiSk))
		require.Equal(s.T(), expect.PK, pk)
		require.Equal(s.T(), expect.SK, sk)
		require.Equal(s.T(), expect.GSIPK, gsiPk)
		require.Equal(s.T(), expect.GSISK, gsiSk)
	})
	s.Run("with-gsi", func() {
		db := GetDB(s.T())
		row := db.QueryRowContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables"."gsi_pk-gsi_sk-index" WHERE gsi_pk = ? AND gsi_sk = ?`, "TestQueryTestSuite3", "3")

		expect := TestTables{
			PK:    "TestQueryTestSuite",
			SK:    3,
			GSIPK: "TestQueryTestSuite3",
			GSISK: "3",
		}

		var (
			pk    string
			sk    float64
			gsiPk string
			gsiSk string
		)

		require.NoError(s.T(), row.Scan(&pk, &sk, &gsiPk, &gsiSk))
		require.Equal(s.T(), expect.PK, pk)
		require.Equal(s.T(), expect.SK, sk)
		require.Equal(s.T(), expect.GSIPK, gsiPk)
		require.Equal(s.T(), expect.GSISK, gsiSk)
	})
	s.Run("update-returning", func() {
		db := GetDB(s.T())
		row := db.QueryRowContext(context.Background(), `UPDATE "test_tables" SET gsi_pk = ? SET gsi_sk = ? WHERE pk = ? AND sk = ? RETURNING ALL OLD pk, gsi_sk, sk`, "TestQueryTestSuite3", "3.5", "TestQueryTestSuite", 3)

		expect := TestTables{
			PK:    "TestQueryTestSuite",
			SK:    3,
			GSIPK: "TestQueryTestSuite3",
			GSISK: "3",
		}

		var (
			pk    string
			gsiSk string
			sk    float64
		)

		require.NoError(s.T(), row.Scan(&pk, &gsiSk, &sk))
		require.Equal(s.T(), expect.PK, pk)
		require.Equal(s.T(), expect.SK, sk)
		require.Equal(s.T(), expect.GSISK, gsiSk)
	})
	s.Run("delete-returning", func() {
		db := GetDB(s.T())
		row := db.QueryRowContext(context.Background(), `DELETE FROM "test_tables" WHERE pk = ? AND sk = ? RETURNING ALL OLD pk, gsi_sk, sk`, "TestQueryTestSuite", 3)

		expect := TestTables{
			PK:    "TestQueryTestSuite",
			SK:    3,
			GSIPK: "TestQueryTestSuite3",
			GSISK: "3",
		}

		var (
			pk    string
			gsiSk string
			sk    float64
		)

		require.NoError(s.T(), row.Scan(&pk, &gsiSk, &sk))
		require.Equal(s.T(), expect.PK, pk)
		require.Equal(s.T(), expect.SK, sk)
		require.Equal(s.T(), expect.GSISK, gsiSk)
	})
	s.Run("describe-table", func() {
		db := GetDB(s.T())
		row := db.QueryRowContext(context.Background(), `SELECT * FROM "!pqxd_describe_table" WHERE table_name = ?`, "test_tables")

		var (
			archivalSummary           pqxd.ArchivalSummary
			attributeDefinitions      pqxd.AttributeDefinitions
			billingModeSummary        pqxd.BillingModeSummary
			creationDateTime          pqxd.CreationDateTime
			deletionProtectionEnabled pqxd.DeletionProtectionEnabled
			keySchema                 pqxd.KeySchema
			globalSecondaryIndexes    pqxd.GlobalSecondaryIndexes
			globalTableVersion        pqxd.GlobalTableVersion
			itemCount                 pqxd.ItemCount
			localSecondaryIndexes     pqxd.LocalSecondaryIndexes
			onDemandThroughput        pqxd.OnDemandThroughput
			provisionedThroughput     pqxd.ProvisionedThroughput
			replicas                  pqxd.Replicas
			restoreSummary            pqxd.RestoreSummary
			sseDescription            pqxd.SSEDescription
			streamSpecification       pqxd.StreamSpecification
			tableClassSummary         pqxd.TableClassSummary
			tableStatus               pqxd.TableStatus
		)
		err := row.Scan(&archivalSummary, &attributeDefinitions, &billingModeSummary, &creationDateTime, &deletionProtectionEnabled, &keySchema, &globalSecondaryIndexes, &globalTableVersion, &itemCount, &localSecondaryIndexes, &onDemandThroughput, &provisionedThroughput, &replicas, &restoreSummary, &sseDescription, &streamSpecification, &tableClassSummary, &tableStatus)
		require.NoError(s.T(), err)

		require.False(s.T(), archivalSummary.Valid)

		require.Len(s.T(), attributeDefinitions, 4)

		require.Equal(s.T(), aws.String("pk"), attributeDefinitions[0].AttributeName)
		require.Equal(s.T(), types.ScalarAttributeTypeS, attributeDefinitions[0].AttributeType)
		require.Equal(s.T(), aws.String("sk"), attributeDefinitions[1].AttributeName)
		require.Equal(s.T(), types.ScalarAttributeTypeN, attributeDefinitions[1].AttributeType)
		require.Equal(s.T(), aws.String("gsi_pk"), attributeDefinitions[2].AttributeName)
		require.Equal(s.T(), types.ScalarAttributeTypeS, attributeDefinitions[2].AttributeType)
		require.Equal(s.T(), aws.String("gsi_sk"), attributeDefinitions[3].AttributeName)
		require.Equal(s.T(), types.ScalarAttributeTypeS, attributeDefinitions[3].AttributeType)

		require.False(s.T(), billingModeSummary.Valid)
		require.True(s.T(), creationDateTime.Valid)

		require.True(s.T(), deletionProtectionEnabled.Valid)
		require.False(s.T(), deletionProtectionEnabled.Bool)

		require.Len(s.T(), keySchema, 2)
		require.Equal(s.T(), aws.String("pk"), keySchema[0].AttributeName)
		require.Equal(s.T(), types.KeyTypeHash, keySchema[0].KeyType)
		require.Equal(s.T(), aws.String("sk"), keySchema[1].AttributeName)
		require.Equal(s.T(), types.KeyTypeRange, keySchema[1].KeyType)

		require.Len(s.T(), globalSecondaryIndexes, 1)
		require.Equal(s.T(), aws.String("gsi_pk-gsi_sk-index"), globalSecondaryIndexes[0].IndexName)
		require.Len(s.T(), globalSecondaryIndexes[0].KeySchema, 2)
		require.Equal(s.T(), aws.String("gsi_pk"), globalSecondaryIndexes[0].KeySchema[0].AttributeName)
		require.Equal(s.T(), types.KeyTypeHash, globalSecondaryIndexes[0].KeySchema[0].KeyType)
		require.Equal(s.T(), aws.String("gsi_sk"), globalSecondaryIndexes[0].KeySchema[1].AttributeName)
		require.Equal(s.T(), types.KeyTypeRange, globalSecondaryIndexes[0].KeySchema[1].KeyType)
		require.Equal(s.T(), types.ProjectionTypeAll, globalSecondaryIndexes[0].Projection.ProjectionType)
		require.Equal(s.T(), aws.Int64(1), globalSecondaryIndexes[0].ProvisionedThroughput.ReadCapacityUnits)
		require.Equal(s.T(), aws.Int64(1), globalSecondaryIndexes[0].ProvisionedThroughput.WriteCapacityUnits)
		require.Equal(s.T(), types.IndexStatusActive, globalSecondaryIndexes[0].IndexStatus)

		require.False(s.T(), globalTableVersion.Valid)

		require.True(s.T(), itemCount.Valid)
		require.Equal(s.T(), int64(5), itemCount.Int64)

		require.Len(s.T(), localSecondaryIndexes, 0)
		require.False(s.T(), onDemandThroughput.Valid)

		require.True(s.T(), provisionedThroughput.Valid)
		require.Equal(s.T(), aws.Int64(1), provisionedThroughput.V.ReadCapacityUnits)
		require.Equal(s.T(), aws.Int64(1), provisionedThroughput.V.WriteCapacityUnits)

		require.False(s.T(), replicas.Valid)
		require.False(s.T(), restoreSummary.Valid)
		require.False(s.T(), sseDescription.Valid)
		require.False(s.T(), streamSpecification.Valid)
		require.False(s.T(), tableClassSummary.Valid)
		require.Equal(s.T(), "ACTIVE", tableStatus.String())
	})
}

func (s *QueryTestSuite) Test_QueryContext() {
	s.Run("full-scan", func() {
		db := GetDB(s.T())
		rows, err := db.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables"`)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "TestQueryTestSuite",
				SK:    1.0,
				GSIPK: "TestQueryTestSuite1",
				GSISK: "1",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    2.0,
				GSIPK: "TestQueryTestSuite2",
				GSISK: "2",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    3,
				GSIPK: "TestQueryTestSuite3",
				GSISK: "3",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    4,
				GSIPK: "TestQueryTestSuite4",
				GSISK: "4",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    5,
				GSIPK: "TestQueryTestSuite5",
				GSISK: "5",
			},
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
	})
	s.Run("with-pk", func() {
		db := GetDB(s.T())
		rows, err := db.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ?`, "TestQueryTestSuite")
		require.NoError(s.T(), err)
		expect := []TestTables{
			{
				PK:    "TestQueryTestSuite",
				SK:    1.0,
				GSIPK: "TestQueryTestSuite1",
				GSISK: "1",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    2.0,
				GSIPK: "TestQueryTestSuite2",
				GSISK: "2",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    3,
				GSIPK: "TestQueryTestSuite3",
				GSISK: "3",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    4,
				GSIPK: "TestQueryTestSuite4",
				GSISK: "4",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    5,
				GSIPK: "TestQueryTestSuite5",
				GSISK: "5",
			},
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
	})
	s.Run("with-pk-and-sk", func() {
		db := GetDB(s.T())
		rows, err := db.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "TestQueryTestSuite", 3)
		require.NoError(s.T(), err)
		expect := []TestTables{
			{
				PK:    "TestQueryTestSuite",
				SK:    3,
				GSIPK: "TestQueryTestSuite3",
				GSISK: "3",
			},
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
	})
	s.Run("with-sk", func() {
		db := GetDB(s.T())
		rows, err := db.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE sk = ?`, 3)
		require.NoError(s.T(), err)
		expect := []TestTables{
			{
				PK:    "TestQueryTestSuite",
				SK:    3,
				GSIPK: "TestQueryTestSuite3",
				GSISK: "3",
			},
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
	})
	s.Run("with-gsi-pk", func() {
		db := GetDB(s.T())
		rows, err := db.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE gsi_pk = ?`, "TestQueryTestSuite3")
		require.NoError(s.T(), err)
		expect := []TestTables{
			{
				PK:    "TestQueryTestSuite",
				SK:    3,
				GSIPK: "TestQueryTestSuite3",
				GSISK: "3",
			},
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
	})
	s.Run("with-gsi-pk-and-sk", func() {
		db := GetDB(s.T())
		rows, err := db.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE gsi_pk = ? AND gsi_sk = ?`, "TestQueryTestSuite3", "3")
		require.NoError(s.T(), err)
		expect := []TestTables{
			{
				PK:    "TestQueryTestSuite",
				SK:    3,
				GSIPK: "TestQueryTestSuite3",
				GSISK: "3",
			},
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
	})
	s.Run("with-scanner", func() {
		db := GetDB(s.T())
		rows, err := db.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE gsi_pk = ?`, "TestQueryTestSuite3")
		require.NoError(s.T(), err)
		expect := []TestTables{
			{
				PK:    "TestQueryTestSuite",
				SK:    3,
				GSIPK: "TestQueryTestSuite3",
				GSISK: "30",
			},
		}

		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk tenTimes
				)

				require.NoError(s.T(), rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, string(gsiSk))
				i++
			}
		}
	})
	s.Run("update-returning", func() {
		db := GetDB(s.T())
		rows, err := db.QueryContext(context.Background(), `UPDATE "test_tables" SET gsi_pk = ? SET gsi_sk = ? WHERE pk = ? AND sk = ? RETURNING ALL OLD pk, gsi_sk, sk`, "TestQueryTestSuite3", "3.5", "TestQueryTestSuite", 3)
		require.NoError(s.T(), err)

		expect := TestTables{
			PK:    "TestQueryTestSuite",
			SK:    3,
			GSIPK: "TestQueryTestSuite3",
			GSISK: "3",
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					gsiSk string
					sk    float64
				)

				require.NoError(s.T(), rows.Scan(&pk, &gsiSk, &sk))
				require.Equal(s.T(), expect.PK, pk)
				require.Equal(s.T(), expect.GSISK, gsiSk)
				require.Equal(s.T(), expect.SK, sk)
				i++
			}
		}
		require.Equal(s.T(), 1, i)
	})
	s.Run("delete-returning", func() {
		db := GetDB(s.T())
		rows, err := db.QueryContext(context.Background(), `DELETE FROM "test_tables" WHERE pk = ? AND sk = ? RETURNING ALL OLD pk, gsi_sk, sk`, "TestQueryTestSuite", 3)
		require.NoError(s.T(), err)

		expect := TestTables{
			PK:    "TestQueryTestSuite",
			SK:    3,
			GSIPK: "TestQueryTestSuite3",
			GSISK: "3",
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					gsiSk string
					sk    float64
				)

				require.NoError(s.T(), rows.Scan(&pk, &gsiSk, &sk))
				require.Equal(s.T(), expect.PK, pk)
				require.Equal(s.T(), expect.GSISK, gsiSk)
				require.Equal(s.T(), expect.SK, sk)
				i++
			}
		}
		require.Equal(s.T(), 1, i)
	})
}

func (s *QueryTestSuite) Test_Query() {
	s.Run("full-scan", func() {
		db := GetDB(s.T())
		rows, err := db.Query(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables"`)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "TestQueryTestSuite",
				SK:    1.0,
				GSIPK: "TestQueryTestSuite1",
				GSISK: "1",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    2.0,
				GSIPK: "TestQueryTestSuite2",
				GSISK: "2",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    3,
				GSIPK: "TestQueryTestSuite3",
				GSISK: "3",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    4,
				GSIPK: "TestQueryTestSuite4",
				GSISK: "4",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    5,
				GSIPK: "TestQueryTestSuite5",
				GSISK: "5",
			},
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
	})
}

func (s *QueryTestSuite) Test_PrepareContext() {
	s.Run("Query", func() {
		db := GetDB(s.T())
		query, err := db.PrepareContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`)
		require.NoError(s.T(), err)
		rows, err := query.Query("TestQueryTestSuite", 1.0)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "TestQueryTestSuite",
				SK:    1.0,
				GSIPK: "TestQueryTestSuite1",
				GSISK: "1",
			},
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
		require.Equal(s.T(), 1, i)
	})
	s.Run("QueryContext", func() {
		db := GetDB(s.T())
		query, err := db.PrepareContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`)
		require.NoError(s.T(), err)
		rows, err := query.Query("TestQueryTestSuite", 1.0)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "TestQueryTestSuite",
				SK:    1.0,
				GSIPK: "TestQueryTestSuite1",
				GSISK: "1",
			},
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
		require.Equal(s.T(), i, 1)
	})
	s.Run("update-returning", func() {
		db := GetDB(s.T())
		query, err := db.PrepareContext(context.Background(), `UPDATE "test_tables" SET gsi_pk = ? SET gsi_sk = ? WHERE pk = ? AND sk = ? RETURNING ALL OLD pk, gsi_sk, sk`)
		require.NoError(s.T(), err)

		row := query.QueryRowContext(context.Background(), "TestQueryTestSuite3", "3.5", "TestQueryTestSuite", 3)

		expect := TestTables{
			PK:    "TestQueryTestSuite",
			SK:    3,
			GSIPK: "TestQueryTestSuite3",
			GSISK: "3",
		}

		var (
			pk    string
			gsiSk string
			sk    float64
		)

		require.NoError(s.T(), row.Scan(&pk, &gsiSk, &sk))
		require.Equal(s.T(), expect.PK, pk)
		require.Equal(s.T(), expect.SK, sk)
		require.Equal(s.T(), expect.GSISK, gsiSk)
	})
	s.Run("delete-returning", func() {
		db := GetDB(s.T())
		query, err := db.PrepareContext(context.Background(), `DELETE FROM "test_tables" WHERE pk = ? AND sk = ? RETURNING ALL OLD pk, gsi_sk, sk`)
		require.NoError(s.T(), err)

		row := query.QueryRowContext(context.Background(), "TestQueryTestSuite", 3)

		expect := TestTables{
			PK:    "TestQueryTestSuite",
			SK:    3,
			GSIPK: "TestQueryTestSuite3",
			GSISK: "3",
		}

		var (
			pk    string
			gsiSk string
			sk    float64
		)

		require.NoError(s.T(), row.Scan(&pk, &gsiSk, &sk))
		require.Equal(s.T(), expect.PK, pk)
		require.Equal(s.T(), expect.SK, sk)
		require.Equal(s.T(), expect.GSISK, gsiSk)
	})
	s.Run("describe-table", func() {
		db := GetDB(s.T())
		stmt, err := db.PrepareContext(context.Background(), `SELECT * FROM "!pqxd_describe_table" WHERE table_name = ?`)
		require.NoError(s.T(), err)
		row := stmt.QueryRowContext(context.Background(), "test_tables")

		var (
			archivalSummary           pqxd.ArchivalSummary
			attributeDefinitions      pqxd.AttributeDefinitions
			billingModeSummary        pqxd.BillingModeSummary
			creationDateTime          pqxd.CreationDateTime
			deletionProtectionEnabled pqxd.DeletionProtectionEnabled
			keySchema                 pqxd.KeySchema
			globalSecondaryIndexes    pqxd.GlobalSecondaryIndexes
			globalTableVersion        pqxd.GlobalTableVersion
			itemCount                 pqxd.ItemCount
			localSecondaryIndexes     pqxd.LocalSecondaryIndexes
			onDemandThroughput        pqxd.OnDemandThroughput
			provisionedThroughput     pqxd.ProvisionedThroughput
			replicas                  pqxd.Replicas
			restoreSummary            pqxd.RestoreSummary
			sseDescription            pqxd.SSEDescription
			streamSpecification       pqxd.StreamSpecification
			tableClassSummary         pqxd.TableClassSummary
			tableStatus               pqxd.TableStatus
		)
		err = row.Scan(&archivalSummary, &attributeDefinitions, &billingModeSummary, &creationDateTime, &deletionProtectionEnabled, &keySchema, &globalSecondaryIndexes, &globalTableVersion, &itemCount, &localSecondaryIndexes, &onDemandThroughput, &provisionedThroughput, &replicas, &restoreSummary, &sseDescription, &streamSpecification, &tableClassSummary, &tableStatus)
		require.NoError(s.T(), err)

		require.False(s.T(), archivalSummary.Valid)

		require.Len(s.T(), attributeDefinitions, 4)

		require.Equal(s.T(), aws.String("pk"), attributeDefinitions[0].AttributeName)
		require.Equal(s.T(), types.ScalarAttributeTypeS, attributeDefinitions[0].AttributeType)
		require.Equal(s.T(), aws.String("sk"), attributeDefinitions[1].AttributeName)
		require.Equal(s.T(), types.ScalarAttributeTypeN, attributeDefinitions[1].AttributeType)
		require.Equal(s.T(), aws.String("gsi_pk"), attributeDefinitions[2].AttributeName)
		require.Equal(s.T(), types.ScalarAttributeTypeS, attributeDefinitions[2].AttributeType)
		require.Equal(s.T(), aws.String("gsi_sk"), attributeDefinitions[3].AttributeName)
		require.Equal(s.T(), types.ScalarAttributeTypeS, attributeDefinitions[3].AttributeType)

		require.False(s.T(), billingModeSummary.Valid)
		require.True(s.T(), creationDateTime.Valid)

		require.True(s.T(), deletionProtectionEnabled.Valid)
		require.False(s.T(), deletionProtectionEnabled.Bool)

		require.Len(s.T(), keySchema, 2)
		require.Equal(s.T(), aws.String("pk"), keySchema[0].AttributeName)
		require.Equal(s.T(), types.KeyTypeHash, keySchema[0].KeyType)
		require.Equal(s.T(), aws.String("sk"), keySchema[1].AttributeName)
		require.Equal(s.T(), types.KeyTypeRange, keySchema[1].KeyType)

		require.Len(s.T(), globalSecondaryIndexes, 1)
		require.Equal(s.T(), aws.String("gsi_pk-gsi_sk-index"), globalSecondaryIndexes[0].IndexName)
		require.Len(s.T(), globalSecondaryIndexes[0].KeySchema, 2)
		require.Equal(s.T(), aws.String("gsi_pk"), globalSecondaryIndexes[0].KeySchema[0].AttributeName)
		require.Equal(s.T(), types.KeyTypeHash, globalSecondaryIndexes[0].KeySchema[0].KeyType)
		require.Equal(s.T(), aws.String("gsi_sk"), globalSecondaryIndexes[0].KeySchema[1].AttributeName)
		require.Equal(s.T(), types.KeyTypeRange, globalSecondaryIndexes[0].KeySchema[1].KeyType)
		require.Equal(s.T(), types.ProjectionTypeAll, globalSecondaryIndexes[0].Projection.ProjectionType)
		require.Equal(s.T(), aws.Int64(1), globalSecondaryIndexes[0].ProvisionedThroughput.ReadCapacityUnits)
		require.Equal(s.T(), aws.Int64(1), globalSecondaryIndexes[0].ProvisionedThroughput.WriteCapacityUnits)
		require.Equal(s.T(), types.IndexStatusActive, globalSecondaryIndexes[0].IndexStatus)

		require.False(s.T(), globalTableVersion.Valid)

		require.True(s.T(), itemCount.Valid)
		require.Equal(s.T(), int64(5), itemCount.Int64)

		require.Len(s.T(), localSecondaryIndexes, 0)
		require.False(s.T(), onDemandThroughput.Valid)

		require.True(s.T(), provisionedThroughput.Valid)
		require.Equal(s.T(), aws.Int64(1), provisionedThroughput.V.ReadCapacityUnits)
		require.Equal(s.T(), aws.Int64(1), provisionedThroughput.V.WriteCapacityUnits)

		require.False(s.T(), replicas.Valid)
		require.False(s.T(), restoreSummary.Valid)
		require.False(s.T(), sseDescription.Valid)
		require.False(s.T(), streamSpecification.Valid)
		require.False(s.T(), tableClassSummary.Valid)
		require.Equal(s.T(), "ACTIVE", tableStatus.String())
	})
}

func (s *QueryTestSuite) Test_Prepare() {
	s.Run("Query", func() {
		db := GetDB(s.T())
		query, err := db.Prepare(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ?`)
		require.NoError(s.T(), err)
		rows, err := query.Query("TestQueryTestSuite")
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "TestQueryTestSuite",
				SK:    1.0,
				GSIPK: "TestQueryTestSuite1",
				GSISK: "1",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    2.0,
				GSIPK: "TestQueryTestSuite2",
				GSISK: "2",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    3,
				GSIPK: "TestQueryTestSuite3",
				GSISK: "3",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    4,
				GSIPK: "TestQueryTestSuite4",
				GSISK: "4",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    5,
				GSIPK: "TestQueryTestSuite5",
				GSISK: "5",
			},
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
	})
	s.Run("QueryContext", func() {
		db := GetDB(s.T())
		query, err := db.PrepareContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ?`)
		require.NoError(s.T(), err)
		rows, err := query.QueryContext(context.Background(), "TestQueryTestSuite")
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "TestQueryTestSuite",
				SK:    1.0,
				GSIPK: "TestQueryTestSuite1",
				GSISK: "1",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    2.0,
				GSIPK: "TestQueryTestSuite2",
				GSISK: "2",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    3,
				GSIPK: "TestQueryTestSuite3",
				GSISK: "3",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    4,
				GSIPK: "TestQueryTestSuite4",
				GSISK: "4",
			},
			{
				PK:    "TestQueryTestSuite",
				SK:    5,
				GSIPK: "TestQueryTestSuite5",
				GSISK: "5",
			},
		}
		i := 0
		for rows.NextResultSet() {
			for rows.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
	})
}
