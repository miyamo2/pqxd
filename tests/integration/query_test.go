package integration

import (
	"context"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/miyamo2/pqxd"
	"github.com/stretchr/testify/suite"
)

type QueryTestSuite struct {
	suite.Suite
	client *dynamodb.Client
}

func TestQueryTestSuite(t *testing.T) {
	suite.Run(t, &QueryTestSuite{client: GetClient(t)})
}

func (s *QueryTestSuite) SetupSubTest() {
	mu.Lock()
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
	s.Require().NoError(err)
}

func (s *QueryTestSuite) TearDownSubTest() {
	defer mu.Unlock()
	for _, item := range s.testData() {
		input := &dynamodb.DeleteItemInput{
			Key: map[string]types.AttributeValue{
				"pk": item["pk"],
				"sk": item["sk"],
			},
			TableName: aws.String("test_tables"),
		}
		_, err := s.client.DeleteItem(context.Background(), input)
		s.Require().NoError(err)
	}
}

func (s *QueryTestSuite) Test_QueryRowContext() {
	s.Run(
		"with-pk-and-sk", func() {
			db := GetDB(s.T())
			row := db.QueryRowContext(
				context.Background(),
				`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`,
				"TestQueryTestSuite",
				1.0,
			)

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

			s.Require().NoError(row.Scan(&pk, &sk, &gsiPk, &gsiSk))
			s.Require().Equal(expect.PK, pk)
			s.Require().Equal(expect.SK, sk)
			s.Require().Equal(expect.GSIPK, gsiPk)
			s.Require().Equal(expect.GSISK, gsiSk)
		},
	)
	s.Run(
		"with-gsi", func() {
			db := GetDB(s.T())
			row := db.QueryRowContext(
				context.Background(),
				`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables"."gsi_pk-gsi_sk-index" WHERE gsi_pk = ? AND gsi_sk = ?`,
				"TestQueryTestSuite3",
				"3",
			)

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

			s.Require().NoError(row.Scan(&pk, &sk, &gsiPk, &gsiSk))
			s.Require().Equal(expect.PK, pk)
			s.Require().Equal(expect.SK, sk)
			s.Require().Equal(expect.GSIPK, gsiPk)
			s.Require().Equal(expect.GSISK, gsiSk)
		},
	)
	s.Run(
		"without-selected-column-list", func() {
			db := GetDB(s.T())
			row := db.QueryRowContext(
				context.Background(),
				`SELECT * FROM "test_tables" WHERE pk = ? AND sk = ?`,
				"TestQueryTestSuite",
				1.0,
			)

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

			s.Require().NoError(row.Scan(&gsiPk, &gsiSk, &pk, &sk))
			s.Require().Equal(expect.PK, pk)
			s.Require().Equal(expect.SK, sk)
			s.Require().Equal(expect.GSIPK, gsiPk)
			s.Require().Equal(expect.GSISK, gsiSk)
		},
	)
	s.Run(
		"update-returning", func() {
			db := GetDB(s.T())
			row := db.QueryRowContext(
				context.Background(),
				`UPDATE "test_tables" SET gsi_pk = ? SET gsi_sk = ? WHERE pk = ? AND sk = ? RETURNING ALL OLD pk, gsi_sk, sk`,
				"TestQueryTestSuite3",
				"3.5",
				"TestQueryTestSuite",
				3,
			)

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

			s.Require().NoError(row.Scan(&pk, &gsiSk, &sk))
			s.Require().Equal(expect.PK, pk)
			s.Require().Equal(expect.SK, sk)
			s.Require().Equal(expect.GSISK, gsiSk)
		},
	)
	s.Run(
		"delete-returning", func() {
			db := GetDB(s.T())
			row := db.QueryRowContext(
				context.Background(),
				`DELETE FROM "test_tables" WHERE pk = ? AND sk = ? RETURNING ALL OLD pk, gsi_sk, sk`,
				"TestQueryTestSuite",
				3,
			)

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

			s.Require().NoError(row.Scan(&pk, &gsiSk, &sk))
			s.Require().Equal(expect.PK, pk)
			s.Require().Equal(expect.SK, sk)
			s.Require().Equal(expect.GSISK, gsiSk)
		},
	)
	s.Run(
		"with-double-quoted-columns", func() {
			db := GetDB(s.T())
			row := db.QueryRowContext(
				context.Background(),
				`SELECT "pk", "sk", "gsi_pk", "gsi_sk" FROM "test_tables" WHERE pk = ? AND sk = ?`,
				"TestQueryTestSuite",
				1.0,
			)

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

			s.Require().NoError(row.Scan(&pk, &sk, &gsiPk, &gsiSk))
			s.Require().Equal(expect.PK, pk)
			s.Require().Equal(expect.SK, sk)
			s.Require().Equal(expect.GSIPK, gsiPk)
			s.Require().Equal(expect.GSISK, gsiSk)
		},
	)
	s.Run(
		"with-mixed-quoted-columns", func() {
			db := GetDB(s.T())
			row := db.QueryRowContext(
				context.Background(),
				`SELECT "pk", sk, "gsi_pk", gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`,
				"TestQueryTestSuite",
				2.0,
			)

			expect := TestTables{
				PK:    "TestQueryTestSuite",
				SK:    2.0,
				GSIPK: "TestQueryTestSuite2",
				GSISK: "2",
			}

			var (
				pk    string
				sk    float64
				gsiPk string
				gsiSk string
			)

			s.Require().NoError(row.Scan(&pk, &sk, &gsiPk, &gsiSk))
			s.Require().Equal(expect.PK, pk)
			s.Require().Equal(expect.SK, sk)
			s.Require().Equal(expect.GSIPK, gsiPk)
			s.Require().Equal(expect.GSISK, gsiSk)
		},
	)
	s.Run(
		"update-returning-with-quoted-columns", func() {
			db := GetDB(s.T())
			row := db.QueryRowContext(
				context.Background(),
				`UPDATE "test_tables" SET gsi_pk = ? SET gsi_sk = ? WHERE pk = ? AND sk = ? RETURNING ALL OLD "pk", "gsi_sk", "sk"`,
				"TestQueryTestSuite4",
				"4.5",
				"TestQueryTestSuite",
				4,
			)

			expect := TestTables{
				PK:    "TestQueryTestSuite",
				SK:    4,
				GSIPK: "TestQueryTestSuite4",
				GSISK: "4",
			}

			var (
				pk    string
				gsiSk string
				sk    float64
			)

			s.Require().NoError(row.Scan(&pk, &gsiSk, &sk))
			s.Require().Equal(expect.PK, pk)
			s.Require().Equal(expect.SK, sk)
			s.Require().Equal(expect.GSISK, gsiSk)
		},
	)
	s.Run(
		"describe-table", func() {
			db := GetDB(s.T())
			row := db.QueryRowContext(
				context.Background(),
				`SELECT * FROM "!pqxd_describe_table" WHERE table_name = ?`,
				"test_tables",
			)

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
			err := row.Scan(
				&archivalSummary,
				&attributeDefinitions,
				&billingModeSummary,
				&creationDateTime,
				&deletionProtectionEnabled,
				&keySchema,
				&globalSecondaryIndexes,
				&globalTableVersion,
				&itemCount,
				&localSecondaryIndexes,
				&onDemandThroughput,
				&provisionedThroughput,
				&replicas,
				&restoreSummary,
				&sseDescription,
				&streamSpecification,
				&tableClassSummary,
				&tableStatus,
			)
			s.Require().NoError(err)

			s.Require().False(archivalSummary.Valid)

			s.Require().Len(attributeDefinitions, 4)

			s.Require().Equal(aws.String("pk"), attributeDefinitions[0].AttributeName)
			s.Require().Equal(types.ScalarAttributeTypeS, attributeDefinitions[0].AttributeType)
			s.Require().Equal(aws.String("sk"), attributeDefinitions[1].AttributeName)
			s.Require().Equal(types.ScalarAttributeTypeN, attributeDefinitions[1].AttributeType)
			s.Require().Equal(aws.String("gsi_pk"), attributeDefinitions[2].AttributeName)
			s.Require().Equal(types.ScalarAttributeTypeS, attributeDefinitions[2].AttributeType)
			s.Require().Equal(aws.String("gsi_sk"), attributeDefinitions[3].AttributeName)
			s.Require().Equal(types.ScalarAttributeTypeS, attributeDefinitions[3].AttributeType)

			s.Require().False(billingModeSummary.Valid)
			s.Require().True(creationDateTime.Valid)

			s.Require().True(deletionProtectionEnabled.Valid)
			s.Require().False(deletionProtectionEnabled.Bool)

			s.Require().Len(keySchema, 2)
			s.Require().Equal(aws.String("pk"), keySchema[0].AttributeName)
			s.Require().Equal(types.KeyTypeHash, keySchema[0].KeyType)
			s.Require().Equal(aws.String("sk"), keySchema[1].AttributeName)
			s.Require().Equal(types.KeyTypeRange, keySchema[1].KeyType)

			s.Require().Len(globalSecondaryIndexes, 1)
			s.Require().Equal(aws.String("gsi_pk-gsi_sk-index"), globalSecondaryIndexes[0].IndexName)
			s.Require().Len(globalSecondaryIndexes[0].KeySchema, 2)
			s.Require().Equal(aws.String("gsi_pk"), globalSecondaryIndexes[0].KeySchema[0].AttributeName)
			s.Require().Equal(types.KeyTypeHash, globalSecondaryIndexes[0].KeySchema[0].KeyType)
			s.Require().Equal(aws.String("gsi_sk"), globalSecondaryIndexes[0].KeySchema[1].AttributeName)
			s.Require().Equal(types.KeyTypeRange, globalSecondaryIndexes[0].KeySchema[1].KeyType)
			s.Require().Equal(types.ProjectionTypeAll, globalSecondaryIndexes[0].Projection.ProjectionType)
			s.Require().Equal(aws.Int64(1), globalSecondaryIndexes[0].ProvisionedThroughput.ReadCapacityUnits)
			s.Require().Equal(aws.Int64(1), globalSecondaryIndexes[0].ProvisionedThroughput.WriteCapacityUnits)
			s.Require().Equal(types.IndexStatusActive, globalSecondaryIndexes[0].IndexStatus)

			s.Require().False(globalTableVersion.Valid)

			s.Require().True(itemCount.Valid)
			s.Require().Equal(int64(5), itemCount.Int64)

			s.Require().Len(localSecondaryIndexes, 0)
			s.Require().False(onDemandThroughput.Valid)

			s.Require().True(provisionedThroughput.Valid)
			s.Require().Equal(aws.Int64(1), provisionedThroughput.V.ReadCapacityUnits)
			s.Require().Equal(aws.Int64(1), provisionedThroughput.V.WriteCapacityUnits)

			s.Require().False(replicas.Valid)
			s.Require().False(restoreSummary.Valid)
			s.Require().False(sseDescription.Valid)
			s.Require().False(streamSpecification.Valid)
			s.Require().False(tableClassSummary.Valid)
			s.Require().Equal("ACTIVE", tableStatus.String())
		},
	)
}

func (s *QueryTestSuite) Test_QueryContext() {
	s.Run(
		"full-scan", func() {
			db := GetDB(s.T())
			rows, err := db.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables"`)
			s.Require().NoError(err)

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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(5, i)
		},
	)
	s.Run(
		"with-pk", func() {
			db := GetDB(s.T())
			rows, err := db.QueryContext(
				context.Background(),
				`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ?`,
				"TestQueryTestSuite",
			)
			s.Require().NoError(err)
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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(5, i)
		},
	)
	s.Run(
		"with-pk-and-sk", func() {
			db := GetDB(s.T())
			rows, err := db.QueryContext(
				context.Background(),
				`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`,
				"TestQueryTestSuite",
				3,
			)
			s.Require().NoError(err)
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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(1, i)
		},
	)
	s.Run(
		"with-sk", func() {
			db := GetDB(s.T())
			rows, err := db.QueryContext(
				context.Background(),
				`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE sk = ?`,
				3,
			)
			s.Require().NoError(err)
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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(1, i)
		},
	)
	s.Run(
		"with-gsi-pk", func() {
			db := GetDB(s.T())
			rows, err := db.QueryContext(
				context.Background(),
				`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE gsi_pk = ?`,
				"TestQueryTestSuite3",
			)
			s.Require().NoError(err)
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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(1, i)
		},
	)
	s.Run(
		"with-gsi-pk-and-sk", func() {
			db := GetDB(s.T())
			rows, err := db.QueryContext(
				context.Background(),
				`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE gsi_pk = ? AND gsi_sk = ?`,
				"TestQueryTestSuite3",
				"3",
			)
			s.Require().NoError(err)
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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(1, i)
		},
	)
	s.Run(
		"with-scanner", func() {
			db := GetDB(s.T())
			rows, err := db.QueryContext(
				context.Background(),
				`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE gsi_pk = ?`,
				"TestQueryTestSuite3",
			)
			s.Require().NoError(err)
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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, string(gsiSk))
					i++
				}
			}
			s.Require().Equal(1, i)
		},
	)
	s.Run(
		"update-returning", func() {
			db := GetDB(s.T())
			rows, err := db.QueryContext(
				context.Background(),
				`UPDATE "test_tables" SET gsi_pk = ? SET gsi_sk = ? WHERE pk = ? AND sk = ? RETURNING ALL OLD pk, gsi_sk, sk`,
				"TestQueryTestSuite3",
				"3.5",
				"TestQueryTestSuite",
				3,
			)
			s.Require().NoError(err)

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

					s.Require().NoError(rows.Scan(&pk, &gsiSk, &sk))
					s.Require().Equal(expect.PK, pk)
					s.Require().Equal(expect.GSISK, gsiSk)
					s.Require().Equal(expect.SK, sk)
					i++
				}
			}
			s.Require().Equal(1, i)
		},
	)
	s.Run(
		"delete-returning", func() {
			db := GetDB(s.T())
			rows, err := db.QueryContext(
				context.Background(),
				`DELETE FROM "test_tables" WHERE pk = ? AND sk = ? RETURNING ALL OLD pk, gsi_sk, sk`,
				"TestQueryTestSuite",
				3,
			)
			s.Require().NoError(err)

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

					s.Require().NoError(rows.Scan(&pk, &gsiSk, &sk))
					s.Require().Equal(expect.PK, pk)
					s.Require().Equal(expect.GSISK, gsiSk)
					s.Require().Equal(expect.SK, sk)
					i++
				}
			}
			s.Require().Equal(1, i)
		},
	)
	s.Run(
		"list-table", func() {
			db := GetDB(s.T())
			rows, err := db.QueryContext(context.Background(), `SELECT * FROM "!pqxd_list_tables"`)
			s.Require().NoError(err)

			var i int
			for rows.NextResultSet() {
				for rows.Next() {
					var tableName string
					s.Require().NoError(rows.Scan(&tableName))
					s.Require().Equal("test_tables", tableName)
					i++
				}
			}
			s.Require().Equal(1, i)
		},
	)
	s.Run(
		"with-double-quoted-columns", func() {
			db := GetDB(s.T())
			rows, err := db.QueryContext(
				context.Background(),
				`SELECT "pk", "sk", "gsi_pk", "gsi_sk" FROM "test_tables" WHERE pk = ?`,
				"TestQueryTestSuite",
			)
			s.Require().NoError(err)
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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(5, i)
		},
	)
	s.Run(
		"with-mixed-quoted-columns", func() {
			db := GetDB(s.T())
			rows, err := db.QueryContext(
				context.Background(),
				`SELECT "pk", sk, "gsi_pk", gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`,
				"TestQueryTestSuite",
				1.0,
			)
			s.Require().NoError(err)
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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(1, i)
		},
	)
}

func (s *QueryTestSuite) Test_Query() {
	s.Run(
		"full-scan", func() {
			db := GetDB(s.T())
			rows, err := db.Query(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables"`)
			s.Require().NoError(err)

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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(5, i)
		},
	)
}

func (s *QueryTestSuite) Test_PrepareContext() {
	s.Run(
		"Query", func() {
			db := GetDB(s.T())
			query, err := db.PrepareContext(
				context.Background(),
				`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`,
			)
			s.Require().NoError(err)
			rows, err := query.Query("TestQueryTestSuite", 1.0)
			s.Require().NoError(err)

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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(1, i)
		},
	)
	s.Run(
		"QueryContext", func() {
			db := GetDB(s.T())
			query, err := db.PrepareContext(
				context.Background(),
				`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`,
			)
			s.Require().NoError(err)
			rows, err := query.Query("TestQueryTestSuite", 1.0)
			s.Require().NoError(err)

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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(i, 1)
		},
	)
	s.Run(
		"update-returning", func() {
			db := GetDB(s.T())
			query, err := db.PrepareContext(
				context.Background(),
				`UPDATE "test_tables" SET gsi_pk = ? SET gsi_sk = ? WHERE pk = ? AND sk = ? RETURNING ALL OLD pk, gsi_sk, sk`,
			)
			s.Require().NoError(err)

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

			s.Require().NoError(row.Scan(&pk, &gsiSk, &sk))
			s.Require().Equal(expect.PK, pk)
			s.Require().Equal(expect.SK, sk)
			s.Require().Equal(expect.GSISK, gsiSk)
		},
	)
	s.Run(
		"delete-returning", func() {
			db := GetDB(s.T())
			query, err := db.PrepareContext(
				context.Background(),
				`DELETE FROM "test_tables" WHERE pk = ? AND sk = ? RETURNING ALL OLD pk, gsi_sk, sk`,
			)
			s.Require().NoError(err)

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

			s.Require().NoError(row.Scan(&pk, &gsiSk, &sk))
			s.Require().Equal(expect.PK, pk)
			s.Require().Equal(expect.SK, sk)
			s.Require().Equal(expect.GSISK, gsiSk)
		},
	)
	s.Run(
		"describe-table", func() {
			db := GetDB(s.T())
			stmt, err := db.PrepareContext(
				context.Background(),
				`SELECT * FROM "!pqxd_describe_table" WHERE table_name = ?`,
			)
			s.Require().NoError(err)
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
			err = row.Scan(
				&archivalSummary,
				&attributeDefinitions,
				&billingModeSummary,
				&creationDateTime,
				&deletionProtectionEnabled,
				&keySchema,
				&globalSecondaryIndexes,
				&globalTableVersion,
				&itemCount,
				&localSecondaryIndexes,
				&onDemandThroughput,
				&provisionedThroughput,
				&replicas,
				&restoreSummary,
				&sseDescription,
				&streamSpecification,
				&tableClassSummary,
				&tableStatus,
			)
			s.Require().NoError(err)

			s.Require().False(archivalSummary.Valid)

			s.Require().Len(attributeDefinitions, 4)

			s.Require().Equal(aws.String("pk"), attributeDefinitions[0].AttributeName)
			s.Require().Equal(types.ScalarAttributeTypeS, attributeDefinitions[0].AttributeType)
			s.Require().Equal(aws.String("sk"), attributeDefinitions[1].AttributeName)
			s.Require().Equal(types.ScalarAttributeTypeN, attributeDefinitions[1].AttributeType)
			s.Require().Equal(aws.String("gsi_pk"), attributeDefinitions[2].AttributeName)
			s.Require().Equal(types.ScalarAttributeTypeS, attributeDefinitions[2].AttributeType)
			s.Require().Equal(aws.String("gsi_sk"), attributeDefinitions[3].AttributeName)
			s.Require().Equal(types.ScalarAttributeTypeS, attributeDefinitions[3].AttributeType)

			s.Require().False(billingModeSummary.Valid)
			s.Require().True(creationDateTime.Valid)

			s.Require().True(deletionProtectionEnabled.Valid)
			s.Require().False(deletionProtectionEnabled.Bool)

			s.Require().Len(keySchema, 2)
			s.Require().Equal(aws.String("pk"), keySchema[0].AttributeName)
			s.Require().Equal(types.KeyTypeHash, keySchema[0].KeyType)
			s.Require().Equal(aws.String("sk"), keySchema[1].AttributeName)
			s.Require().Equal(types.KeyTypeRange, keySchema[1].KeyType)

			s.Require().Len(globalSecondaryIndexes, 1)
			s.Require().Equal(aws.String("gsi_pk-gsi_sk-index"), globalSecondaryIndexes[0].IndexName)
			s.Require().Len(globalSecondaryIndexes[0].KeySchema, 2)
			s.Require().Equal(aws.String("gsi_pk"), globalSecondaryIndexes[0].KeySchema[0].AttributeName)
			s.Require().Equal(types.KeyTypeHash, globalSecondaryIndexes[0].KeySchema[0].KeyType)
			s.Require().Equal(aws.String("gsi_sk"), globalSecondaryIndexes[0].KeySchema[1].AttributeName)
			s.Require().Equal(types.KeyTypeRange, globalSecondaryIndexes[0].KeySchema[1].KeyType)
			s.Require().Equal(types.ProjectionTypeAll, globalSecondaryIndexes[0].Projection.ProjectionType)
			s.Require().Equal(aws.Int64(1), globalSecondaryIndexes[0].ProvisionedThroughput.ReadCapacityUnits)
			s.Require().Equal(aws.Int64(1), globalSecondaryIndexes[0].ProvisionedThroughput.WriteCapacityUnits)
			s.Require().Equal(types.IndexStatusActive, globalSecondaryIndexes[0].IndexStatus)

			s.Require().False(globalTableVersion.Valid)

			s.Require().True(itemCount.Valid)
			s.Require().Equal(int64(5), itemCount.Int64)

			s.Require().Len(localSecondaryIndexes, 0)
			s.Require().False(onDemandThroughput.Valid)

			s.Require().True(provisionedThroughput.Valid)
			s.Require().Equal(aws.Int64(1), provisionedThroughput.V.ReadCapacityUnits)
			s.Require().Equal(aws.Int64(1), provisionedThroughput.V.WriteCapacityUnits)

			s.Require().False(replicas.Valid)
			s.Require().False(restoreSummary.Valid)
			s.Require().False(sseDescription.Valid)
			s.Require().False(streamSpecification.Valid)
			s.Require().False(tableClassSummary.Valid)
			s.Require().Equal("ACTIVE", tableStatus.String())
		},
	)
	s.Run(
		"list-tables", func() {
			db := GetDB(s.T())
			stmt, err := db.PrepareContext(context.Background(), `SELECT * FROM "!pqxd_list_tables"`)
			s.Require().NoError(err)
			rows, err := stmt.QueryContext(context.Background())
			s.Require().NoError(err)

			var i int
			for rows.NextResultSet() {
				for rows.Next() {
					var tableName string
					err = rows.Scan(&tableName)
					s.Require().NoError(err)
					s.Require().Equal("test_tables", tableName)
					i++
				}
			}
			s.Require().Equal(1, i)
		},
	)
	s.Run(
		"with-double-quoted-columns", func() {
			db := GetDB(s.T())
			stmt, err := db.PrepareContext(
				context.Background(),
				`SELECT "pk", "sk", "gsi_pk", "gsi_sk" FROM "test_tables" WHERE pk = ? AND sk = ?`,
			)
			s.Require().NoError(err)

			row := stmt.QueryRowContext(context.Background(), "TestQueryTestSuite", 1.0)

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

			s.Require().NoError(row.Scan(&pk, &sk, &gsiPk, &gsiSk))
			s.Require().Equal(expect.PK, pk)
			s.Require().Equal(expect.SK, sk)
			s.Require().Equal(expect.GSIPK, gsiPk)
			s.Require().Equal(expect.GSISK, gsiSk)
		},
	)
	s.Run(
		"with-mixed-quoted-columns", func() {
			db := GetDB(s.T())
			stmt, err := db.PrepareContext(
				context.Background(),
				`SELECT "pk", sk, "gsi_pk", gsi_sk FROM "test_tables" WHERE pk = ?`,
			)
			s.Require().NoError(err)

			rows, err := stmt.QueryContext(context.Background(), "TestQueryTestSuite")
			s.Require().NoError(err)

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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(5, i)
		},
	)
	s.Run(
		"update-returning-with-quoted-columns", func() {
			db := GetDB(s.T())
			stmt, err := db.PrepareContext(
				context.Background(),
				`UPDATE "test_tables" SET gsi_pk = ? SET gsi_sk = ? WHERE pk = ? AND sk = ? RETURNING ALL OLD "pk", "gsi_sk", "sk"`,
			)
			s.Require().NoError(err)

			row := stmt.QueryRowContext(context.Background(), "TestQueryTestSuite5", "5.5", "TestQueryTestSuite", 5)

			expect := TestTables{
				PK:    "TestQueryTestSuite",
				SK:    5,
				GSIPK: "TestQueryTestSuite5",
				GSISK: "5",
			}

			var (
				pk    string
				gsiSk string
				sk    float64
			)

			s.Require().NoError(row.Scan(&pk, &gsiSk, &sk))
			s.Require().Equal(expect.PK, pk)
			s.Require().Equal(expect.SK, sk)
			s.Require().Equal(expect.GSISK, gsiSk)
		},
	)
}

func (s *QueryTestSuite) Test_Prepare() {
	s.Run(
		"Query", func() {
			db := GetDB(s.T())
			query, err := db.Prepare(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ?`)
			s.Require().NoError(err)
			rows, err := query.Query("TestQueryTestSuite")
			s.Require().NoError(err)

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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(5, i)
		},
	)
	s.Run(
		"QueryContext", func() {
			db := GetDB(s.T())
			query, err := db.PrepareContext(
				context.Background(),
				`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ?`,
			)
			s.Require().NoError(err)
			rows, err := query.QueryContext(context.Background(), "TestQueryTestSuite")
			s.Require().NoError(err)

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

					s.Require().NoError(rows.Scan(&pk, &sk, &gsiPk, &gsiSk))
					s.Require().Equal(expect[i].PK, pk)
					s.Require().Equal(expect[i].SK, sk)
					s.Require().Equal(expect[i].GSIPK, gsiPk)
					s.Require().Equal(expect[i].GSISK, gsiSk)
					i++
				}
			}
			s.Require().Equal(5, i)
		},
	)
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
