package integration

import (
	"context"
	"github.com/avast/retry-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type QueryTestSuite struct {
	suite.Suite
	client *dynamodb.Client
}

func TestQueryTestSuite(t *testing.T) {
	suite.Run(t, &QueryTestSuite{client: GetClient(t)})
}

func (s *QueryTestSuite) SetupSuite() {
	err := retry.Do(
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			tb, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String("test_tables"),
			})
			if err != nil {
				return err
			}
			_ = tb
			return nil
		}, retry.Attempts(5))
	if err != nil {
		s.Failf("failed to describe table, %s", err.Error())
	}
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
