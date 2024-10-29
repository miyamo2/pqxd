package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
