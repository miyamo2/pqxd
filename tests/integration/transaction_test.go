package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/miyamo2/pqxd"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
)

func TestTransactionTestSuite(t *testing.T) {
	suite.Run(t, &QueryTransactionTestSuite{client: GetClient(t)})
	suite.Run(t, &ExecTransactionTestSuite{client: GetClient(t)})
}

type QueryTransactionTestSuite struct {
	suite.Suite
	client *dynamodb.Client
}

func (s *QueryTransactionTestSuite) testData() []map[string]types.AttributeValue {
	return []map[string]types.AttributeValue{
		{
			"pk": &types.AttributeValueMemberS{
				Value: "QueryTestTransactionTestSuite",
			},
			"sk": &types.AttributeValueMemberN{
				Value: "10.0",
			},
			"gsi_pk": &types.AttributeValueMemberS{
				Value: "QueryTestTransactionTestSuite1",
			},
			"gsi_sk": &types.AttributeValueMemberS{
				Value: "10",
			},
		},
		{
			"pk": &types.AttributeValueMemberS{
				Value: "QueryTestTransactionTestSuite",
			},
			"sk": &types.AttributeValueMemberN{
				Value: "20.0",
			},
			"gsi_pk": &types.AttributeValueMemberS{
				Value: "QueryTestTransactionTestSuite2",
			},
			"gsi_sk": &types.AttributeValueMemberS{
				Value: "20",
			},
		},
		{
			"pk": &types.AttributeValueMemberS{
				Value: "QueryTestTransactionTestSuite",
			},
			"sk": &types.AttributeValueMemberN{
				Value: "30",
			},
			"gsi_pk": &types.AttributeValueMemberS{
				Value: "QueryTestTransactionTestSuite3",
			},
			"gsi_sk": &types.AttributeValueMemberS{
				Value: "30",
			},
		},
		{
			"pk": &types.AttributeValueMemberS{
				Value: "QueryTestTransactionTestSuite",
			},
			"sk": &types.AttributeValueMemberN{
				Value: "40",
			},
			"gsi_pk": &types.AttributeValueMemberS{
				Value: "QueryTestTransactionTestSuite4",
			},
			"gsi_sk": &types.AttributeValueMemberS{
				Value: "40",
			},
		},
		{
			"pk": &types.AttributeValueMemberS{
				Value: "QueryTestTransactionTestSuite",
			},
			"sk": &types.AttributeValueMemberN{
				Value: "50",
			},
			"gsi_pk": &types.AttributeValueMemberS{
				Value: "QueryTestTransactionTestSuite5",
			},
			"gsi_sk": &types.AttributeValueMemberS{
				Value: "50",
			},
		},
	}
}

func (s *QueryTransactionTestSuite) SetupSubTest() {
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

func (s *QueryTransactionTestSuite) TearDownSubTest() {
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

func (s *QueryTransactionTestSuite) Test_BeginTx_QueryRowContext() {
	s.Run("query-once", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		row := tx.QueryRowContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)

		expect := TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    10.0,
			GSIPK: "QueryTestTransactionTestSuite1",
			GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})
	s.Run("query-twice", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		row := tx.QueryRowContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)

		expect := TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    10.0,
			GSIPK: "QueryTestTransactionTestSuite1",
			GSISK: "10",
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

		row2 := tx.QueryRowContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 30)

		expect = TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    30.0,
			GSIPK: "QueryTestTransactionTestSuite3",
			GSISK: "30",
		}
		pk, sk, gsiPk, gsiSk = "", 0, "", ""

		require.NoError(s.T(), row2.Scan(&pk, &sk, &gsiPk, &gsiSk))
		require.Equal(s.T(), expect.PK, pk)
		require.Equal(s.T(), expect.SK, sk)
		require.Equal(s.T(), expect.GSIPK, gsiPk)
		require.Equal(s.T(), expect.GSISK, gsiSk)

		require.NoError(s.T(), tx.Commit())
	})
	s.Run("with-query-context", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		row := tx.QueryRowContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)

		expect := TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    10.0,
			GSIPK: "QueryTestTransactionTestSuite1",
			GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})
}

func (s *QueryTransactionTestSuite) Test_BeginTx_QueryRow() {
	s.Run("query-once", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		row := tx.QueryRow(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)

		expect := TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    10.0,
			GSIPK: "QueryTestTransactionTestSuite1",
			GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})
	s.Run("query-twice", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		row := tx.QueryRow(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)

		expect := TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    10.0,
			GSIPK: "QueryTestTransactionTestSuite1",
			GSISK: "10",
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

		row2 := tx.QueryRow(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 30)

		expect = TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    30.0,
			GSIPK: "QueryTestTransactionTestSuite3",
			GSISK: "30",
		}
		pk, sk, gsiPk, gsiSk = "", 0, "", ""

		require.NoError(s.T(), row2.Scan(&pk, &sk, &gsiPk, &gsiSk))
		require.Equal(s.T(), expect.PK, pk)
		require.Equal(s.T(), expect.SK, sk)
		require.Equal(s.T(), expect.GSIPK, gsiPk)
		require.Equal(s.T(), expect.GSISK, gsiSk)

		require.NoError(s.T(), tx.Commit())
	})
	s.Run("with-query-context", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		row := tx.QueryRow(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)

		expect := TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    10.0,
			GSIPK: "QueryTestTransactionTestSuite1",
			GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})
}

func (s *QueryTransactionTestSuite) Test_BeginTx_QueryContext() {
	s.Run("query-once", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		rows, err := tx.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    10.0,
				GSIPK: "QueryTestTransactionTestSuite1",
				GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})
	s.Run("query-twice", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		rows, err := tx.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)
		require.NoError(s.T(), err)

		rows2, err := tx.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 30)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    10.0,
				GSIPK: "QueryTestTransactionTestSuite1",
				GSISK: "10",
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

		expect = []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    30,
				GSIPK: "QueryTestTransactionTestSuite3",
				GSISK: "30",
			},
		}

		i = 0
		for rows2.NextResultSet() {
			for rows2.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows2.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
		require.Equal(s.T(), 1, i)

		require.NoError(s.T(), tx.Commit())
	})
}

func (s *QueryTransactionTestSuite) Test_Begin_QueryRowContext() {
	s.Run("query-once", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		row := tx.QueryRowContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)

		expect := TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    10.0,
			GSIPK: "QueryTestTransactionTestSuite1",
			GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})
	s.Run("query-twice", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		row := tx.QueryRowContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)

		expect := TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    10.0,
			GSIPK: "QueryTestTransactionTestSuite1",
			GSISK: "10",
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

		row2 := tx.QueryRowContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 30)

		expect = TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    30.0,
			GSIPK: "QueryTestTransactionTestSuite3",
			GSISK: "30",
		}
		pk, sk, gsiPk, gsiSk = "", 0, "", ""

		require.NoError(s.T(), row2.Scan(&pk, &sk, &gsiPk, &gsiSk))
		require.Equal(s.T(), expect.PK, pk)
		require.Equal(s.T(), expect.SK, sk)
		require.Equal(s.T(), expect.GSIPK, gsiPk)
		require.Equal(s.T(), expect.GSISK, gsiSk)

		require.NoError(s.T(), tx.Commit())
	})
	s.Run("with-query-context", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		row := tx.QueryRowContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)

		expect := TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    10.0,
			GSIPK: "QueryTestTransactionTestSuite1",
			GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})
}

func (s *QueryTransactionTestSuite) Test_Begin_QueryRow() {
	s.Run("query-once", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		row := tx.QueryRow(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)

		expect := TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    10.0,
			GSIPK: "QueryTestTransactionTestSuite1",
			GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})
	s.Run("query-twice", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		row := tx.QueryRow(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)

		expect := TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    10.0,
			GSIPK: "QueryTestTransactionTestSuite1",
			GSISK: "10",
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

		row2 := tx.QueryRow(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 30)

		expect = TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    30.0,
			GSIPK: "QueryTestTransactionTestSuite3",
			GSISK: "30",
		}
		pk, sk, gsiPk, gsiSk = "", 0, "", ""

		require.NoError(s.T(), row2.Scan(&pk, &sk, &gsiPk, &gsiSk))
		require.Equal(s.T(), expect.PK, pk)
		require.Equal(s.T(), expect.SK, sk)
		require.Equal(s.T(), expect.GSIPK, gsiPk)
		require.Equal(s.T(), expect.GSISK, gsiSk)

		require.NoError(s.T(), tx.Commit())
	})
	s.Run("with-query-context", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		row := tx.QueryRow(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)

		expect := TestTables{
			PK:    "QueryTestTransactionTestSuite",
			SK:    10.0,
			GSIPK: "QueryTestTransactionTestSuite1",
			GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})
}

func (s *QueryTransactionTestSuite) Test_Begin_QueryContext() {
	s.Run("query-once", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		rows, err := tx.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    10.0,
				GSIPK: "QueryTestTransactionTestSuite1",
				GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})

	s.Run("query-twice", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		rows, err := tx.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)
		require.NoError(s.T(), err)

		rows2, err := tx.QueryContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 30)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    10.0,
				GSIPK: "QueryTestTransactionTestSuite1",
				GSISK: "10",
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

		expect = []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    30,
				GSIPK: "QueryTestTransactionTestSuite3",
				GSISK: "30",
			},
		}

		i = 0
		for rows2.NextResultSet() {
			for rows2.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows2.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
		require.Equal(s.T(), 1, i)

		require.NoError(s.T(), tx.Commit())
	})
}

func (s *QueryTransactionTestSuite) Test_BeginTx_Query() {
	s.Run("query-once", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		rows, err := tx.Query(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    10.0,
				GSIPK: "QueryTestTransactionTestSuite1",
				GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})
	s.Run("query-twice", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		rows, err := tx.Query(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)
		require.NoError(s.T(), err)

		rows2, err := tx.Query(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 30)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    10.0,
				GSIPK: "QueryTestTransactionTestSuite1",
				GSISK: "10",
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

		expect = []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    30,
				GSIPK: "QueryTestTransactionTestSuite3",
				GSISK: "30",
			},
		}

		i = 0
		for rows2.NextResultSet() {
			for rows2.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows2.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
		require.Equal(s.T(), 1, i)

		require.NoError(s.T(), tx.Commit())
	})
}

func (s *QueryTransactionTestSuite) Test_Begin_Query() {
	s.Run("query-once", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		rows, err := tx.Query(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    10.0,
				GSIPK: "QueryTestTransactionTestSuite1",
				GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})

	s.Run("query-twice", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		rows, err := tx.Query(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 10)
		require.NoError(s.T(), err)

		rows2, err := tx.Query(`SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`, "QueryTestTransactionTestSuite", 30)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    10.0,
				GSIPK: "QueryTestTransactionTestSuite1",
				GSISK: "10",
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

		expect = []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    30,
				GSIPK: "QueryTestTransactionTestSuite3",
				GSISK: "30",
			},
		}

		i = 0
		for rows2.NextResultSet() {
			for rows2.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows2.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
		require.Equal(s.T(), 1, i)

		require.NoError(s.T(), tx.Commit())
	})
}

func (s *QueryTransactionTestSuite) Test_BeginTx_PrepareContext() {
	s.Run("query-once", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		stmt, err := tx.PrepareContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`)
		require.NoError(s.T(), err)

		rows, err := stmt.QueryContext(context.Background(), "QueryTestTransactionTestSuite", 10)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    10.0,
				GSIPK: "QueryTestTransactionTestSuite1",
				GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})
	s.Run("query-twice", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		stmt, err := tx.PrepareContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`)
		require.NoError(s.T(), err)

		rows, err := stmt.QueryContext(context.Background(), "QueryTestTransactionTestSuite", 10)
		require.NoError(s.T(), err)

		rows2, err := stmt.QueryContext(context.Background(), "QueryTestTransactionTestSuite", 30)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    10.0,
				GSIPK: "QueryTestTransactionTestSuite1",
				GSISK: "10",
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

		expect = []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    30,
				GSIPK: "QueryTestTransactionTestSuite3",
				GSISK: "30",
			},
		}

		i = 0
		for rows2.NextResultSet() {
			for rows2.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows2.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
		require.Equal(s.T(), 1, i)

		require.NoError(s.T(), tx.Commit())
	})
}

func (s *QueryTransactionTestSuite) Test_Begin_PrepareContext() {
	s.Run("query-once", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		stmt, err := tx.PrepareContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`)
		require.NoError(s.T(), err)

		rows, err := stmt.QueryContext(context.Background(), "QueryTestTransactionTestSuite", 10)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    10.0,
				GSIPK: "QueryTestTransactionTestSuite1",
				GSISK: "10",
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

		require.NoError(s.T(), tx.Commit())
	})

	s.Run("query-twice", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		stmt, err := tx.PrepareContext(context.Background(), `SELECT pk, sk, gsi_pk, gsi_sk FROM "test_tables" WHERE pk = ? AND sk = ?`)
		require.NoError(s.T(), err)

		rows, err := stmt.QueryContext(context.Background(), "QueryTestTransactionTestSuite", 10)
		require.NoError(s.T(), err)

		rows2, err := stmt.QueryContext(context.Background(), "QueryTestTransactionTestSuite", 30)
		require.NoError(s.T(), err)

		expect := []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    10.0,
				GSIPK: "QueryTestTransactionTestSuite1",
				GSISK: "10",
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

		expect = []TestTables{
			{
				PK:    "QueryTestTransactionTestSuite",
				SK:    30,
				GSIPK: "QueryTestTransactionTestSuite3",
				GSISK: "30",
			},
		}

		i = 0
		for rows2.NextResultSet() {
			for rows2.Next() {
				var (
					pk    string
					sk    float64
					gsiPk string
					gsiSk string
				)

				require.NoError(s.T(), rows2.Scan(&pk, &sk, &gsiPk, &gsiSk))
				require.Equal(s.T(), expect[i].PK, pk)
				require.Equal(s.T(), expect[i].SK, sk)
				require.Equal(s.T(), expect[i].GSIPK, gsiPk)
				require.Equal(s.T(), expect[i].GSISK, gsiSk)
				i++
			}
		}
		require.Equal(s.T(), 1, i)

		require.NoError(s.T(), tx.Commit())
	})
}

type ExecTransactionTestSuite struct {
	suite.Suite
	client *dynamodb.Client
}

func (s *ExecTransactionTestSuite) TearDownSubTest() {
	registeredData := make([]map[string]types.AttributeValue, 0)
	var lastEvaluatedKey map[string]types.AttributeValue
	for {
		queryOutput, err := s.client.Query(context.Background(), &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
			},
			ExclusiveStartKey: lastEvaluatedKey,
		})
		require.NoError(s.T(), err)
		registeredData = append(registeredData, queryOutput.Items...)
		lastEvaluatedKey = queryOutput.LastEvaluatedKey
		if len(lastEvaluatedKey) == 0 {
			break
		}
	}
	for _, item := range registeredData {
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

func (s *ExecTransactionTestSuite) Test_Begin_ExecContext() {
	s.Run("insert/common", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		result, err := tx.ExecContext(context.Background(), `INSERT INTO "test_tables" VALUE {'pk': ?, 'sk': ?, 'gsi_pk': ?, 'gsi_sk': ?}`, "ExecTransactionTestSuite", 1.0, "ExecTransactionTestSuite1", "1")
		require.NoError(s.T(), err)

		rowAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(0), rowAffected)

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
				"#sk": "sk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
				":sk": &types.AttributeValueMemberN{Value: "1.0"},
			},
		}

		queryOutput, err := s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 0)

		lastInsertedID, err := result.LastInsertId()
		require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
		require.Equal(s.T(), int64(0), lastInsertedID)

		require.NoError(s.T(), tx.Commit())
		rowAffected, err = result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(1), rowAffected)

		expect := []TestTables{
			{
				PK:    "ExecTransactionTestSuite",
				SK:    1.0,
				GSIPK: "ExecTransactionTestSuite1",
				GSISK: "1",
			},
		}

		queryOutput, err = s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		var actual []TestTables
		attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
		require.Exactly(s.T(), expect, actual)
	})

	s.Run("update/common", func() {
		PutTestTable(s.T(), TestTables{
			PK:    "ExecTransactionTestSuite",
			SK:    1.0,
			GSIPK: "ExecTransactionTestSuite1",
			GSISK: "1",
		})

		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		result, err := tx.ExecContext(context.Background(), `UPDATE "test_tables" SET gsi_pk=? SET gsi_sk=? WHERE pk=? AND sk=?`, "ExecTransactionTestSuite2", "2", "ExecTransactionTestSuite", 1)
		require.NoError(s.T(), err)

		rowAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(0), rowAffected)

		expect := []TestTables{
			{
				PK:    "ExecTransactionTestSuite",
				SK:    1,
				GSIPK: "ExecTransactionTestSuite1",
				GSISK: "1",
			},
		}

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
				"#sk": "sk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
				":sk": &types.AttributeValueMemberN{Value: "1.0"},
			},
		}

		queryOutput, err := s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		var actual []TestTables
		attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
		require.Exactly(s.T(), expect, actual)

		lastInsertedID, err := result.LastInsertId()
		require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
		require.Equal(s.T(), int64(0), lastInsertedID)

		require.NoError(s.T(), tx.Commit())
		rowAffected, err = result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(1), rowAffected)

		expect = []TestTables{
			{
				PK:    "ExecTransactionTestSuite",
				SK:    1,
				GSIPK: "ExecTransactionTestSuite2",
				GSISK: "2",
			},
		}

		queryOutput, err = s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		actual = make([]TestTables, 0)
		attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
		require.Exactly(s.T(), expect, actual)
	})

	s.Run("delete/common", func() {
		PutTestTable(s.T(), TestTables{
			PK:    "ExecTransactionTestSuite",
			SK:    10.0,
			GSIPK: "ExecTransactionTestSuite1",
			GSISK: "1",
		})

		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		result, err := tx.ExecContext(context.Background(), `DELETE FROM "test_tables" WHERE pk=? AND sk=?`, "ExecTransactionTestSuite", 1.0)
		require.NoError(s.T(), err)

		rowAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(0), rowAffected)

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
				"#sk": "sk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
				":sk": &types.AttributeValueMemberN{Value: "1.0"},
			},
		}

		queryOutput, err := s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 0)

		lastInsertedID, err := result.LastInsertId()
		require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
		require.Equal(s.T(), int64(0), lastInsertedID)

		require.NoError(s.T(), tx.Commit())
		rowAffected, err = result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(1), rowAffected)

		queryOutput, err = s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 0)
	})
}

func (s *ExecTransactionTestSuite) Test_Begin_Exec() {
	s.Run("insert/common", func() {
		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		result, err := tx.Exec(`INSERT INTO "test_tables" VALUE {'pk': ?, 'sk': ?, 'gsi_pk': ?, 'gsi_sk': ?}`, "ExecTransactionTestSuite", 1.0, "ExecTransactionTestSuite1", "1")
		require.NoError(s.T(), err)

		rowAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(0), rowAffected)

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
				"#sk": "sk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
				":sk": &types.AttributeValueMemberN{Value: "1.0"},
			},
		}

		queryOutput, err := s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 0)

		lastInsertedID, err := result.LastInsertId()
		require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
		require.Equal(s.T(), int64(0), lastInsertedID)

		require.NoError(s.T(), tx.Commit())
		rowAffected, err = result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(1), rowAffected)

		expect := []TestTables{
			{
				PK:    "ExecTransactionTestSuite",
				SK:    1,
				GSIPK: "ExecTransactionTestSuite1",
				GSISK: "1",
			},
		}

		queryOutput, err = s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		var actual []TestTables
		attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
		require.Exactly(s.T(), expect, actual)
	})

	s.Run("update/common", func() {
		PutTestTable(s.T(), TestTables{
			PK:    "ExecTransactionTestSuite",
			SK:    1,
			GSIPK: "ExecTransactionTestSuite1",
			GSISK: "1",
		})

		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		result, err := tx.Exec(`UPDATE "test_tables" SET gsi_pk=? SET gsi_sk=? WHERE pk=? AND sk=?`, "ExecTransactionTestSuite2", "2", "ExecTransactionTestSuite", 1)
		require.NoError(s.T(), err)

		rowAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(0), rowAffected)

		expect := []TestTables{
			{
				PK:    "ExecTransactionTestSuite",
				SK:    1,
				GSIPK: "ExecTransactionTestSuite1",
				GSISK: "1",
			},
		}

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
				"#sk": "sk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
				":sk": &types.AttributeValueMemberN{Value: "1.0"},
			},
		}

		queryOutput, err := s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		var actual []TestTables
		attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
		require.Exactly(s.T(), expect, actual)

		lastInsertedID, err := result.LastInsertId()
		require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
		require.Equal(s.T(), int64(0), lastInsertedID)

		require.NoError(s.T(), tx.Commit())
		rowAffected, err = result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(1), rowAffected)

		expect = []TestTables{
			{
				PK:    "ExecTransactionTestSuite",
				SK:    1,
				GSIPK: "ExecTransactionTestSuite2",
				GSISK: "2",
			},
		}

		queryOutput, err = s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		actual = make([]TestTables, 0)
		attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
		require.Exactly(s.T(), expect, actual)
	})

	s.Run("delete/common", func() {
		PutTestTable(s.T(), TestTables{
			PK:    "ExecTransactionTestSuite",
			SK:    1.0,
			GSIPK: "ExecTransactionTestSuite1",
			GSISK: "1",
		})

		db := GetDB(s.T())
		tx, err := db.Begin()
		require.NoError(s.T(), err)

		result, err := tx.Exec(`DELETE FROM "test_tables" WHERE pk=? AND sk=?`, "ExecTransactionTestSuite", 1.0)
		require.NoError(s.T(), err)

		rowAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(0), rowAffected)

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
				"#sk": "sk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
				":sk": &types.AttributeValueMemberN{Value: "1.0"},
			},
		}

		queryOutput, err := s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		lastInsertedID, err := result.LastInsertId()
		require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
		require.Equal(s.T(), int64(0), lastInsertedID)

		require.NoError(s.T(), tx.Commit())
		rowAffected, err = result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(1), rowAffected)

		queryOutput, err = s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 0)
	})
}

func (s *ExecTransactionTestSuite) Test_BeginTx_ExecContext() {
	s.Run("insert/common", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		result, err := tx.ExecContext(context.Background(), `INSERT INTO "test_tables" VALUE {'pk': ?, 'sk': ?, 'gsi_pk': ?, 'gsi_sk': ?}`, "ExecTransactionTestSuite", 1.0, "ExecTransactionTestSuite1", "1")
		require.NoError(s.T(), err)

		rowAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(0), rowAffected)

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
				"#sk": "sk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
				":sk": &types.AttributeValueMemberN{Value: "1.0"},
			},
		}

		queryOutput, err := s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 0)

		lastInsertedID, err := result.LastInsertId()
		require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
		require.Equal(s.T(), int64(0), lastInsertedID)

		require.NoError(s.T(), tx.Commit())
		rowAffected, err = result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(1), rowAffected)

		expect := []TestTables{
			{
				PK:    "ExecTransactionTestSuite",
				SK:    1.0,
				GSIPK: "ExecTransactionTestSuite1",
				GSISK: "1",
			},
		}

		queryOutput, err = s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		var actual []TestTables
		attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
		require.Exactly(s.T(), expect, actual)
	})

	s.Run("update/common", func() {
		PutTestTable(s.T(), TestTables{
			PK:    "ExecTransactionTestSuite",
			SK:    1.0,
			GSIPK: "ExecTransactionTestSuite1",
			GSISK: "1",
		})

		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		result, err := tx.ExecContext(context.Background(), `UPDATE "test_tables" SET gsi_pk=? SET gsi_sk=? WHERE pk=? AND sk=?`, "ExecTransactionTestSuite2", "2", "ExecTransactionTestSuite", 1)
		require.NoError(s.T(), err)

		rowAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(0), rowAffected)

		expect := []TestTables{
			{
				PK:    "ExecTransactionTestSuite",
				SK:    1,
				GSIPK: "ExecTransactionTestSuite1",
				GSISK: "1",
			},
		}

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
				"#sk": "sk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
				":sk": &types.AttributeValueMemberN{Value: "1.0"},
			},
		}

		queryOutput, err := s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		var actual []TestTables
		attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
		require.Exactly(s.T(), expect, actual)

		lastInsertedID, err := result.LastInsertId()
		require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
		require.Equal(s.T(), int64(0), lastInsertedID)

		require.NoError(s.T(), tx.Commit())
		rowAffected, err = result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(1), rowAffected)

		expect = []TestTables{
			{
				PK:    "ExecTransactionTestSuite",
				SK:    1,
				GSIPK: "ExecTransactionTestSuite2",
				GSISK: "2",
			},
		}

		queryOutput, err = s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		actual = make([]TestTables, 0)
		attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
		require.Exactly(s.T(), expect, actual)
	})

	s.Run("delete/common", func() {
		PutTestTable(s.T(), TestTables{
			PK:    "ExecTransactionTestSuite",
			SK:    10.0,
			GSIPK: "ExecTransactionTestSuite1",
			GSISK: "1",
		})

		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		result, err := tx.ExecContext(context.Background(), `DELETE FROM "test_tables" WHERE pk=? AND sk=?`, "ExecTransactionTestSuite", 1.0)
		require.NoError(s.T(), err)

		rowAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(0), rowAffected)

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
				"#sk": "sk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
				":sk": &types.AttributeValueMemberN{Value: "1.0"},
			},
		}

		queryOutput, err := s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 0)

		lastInsertedID, err := result.LastInsertId()
		require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
		require.Equal(s.T(), int64(0), lastInsertedID)

		require.NoError(s.T(), tx.Commit())
		rowAffected, err = result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(1), rowAffected)

		queryOutput, err = s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 0)
	})
}

func (s *ExecTransactionTestSuite) Test_BeginTx_Exec() {
	s.Run("insert/common", func() {
		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		result, err := tx.Exec(`INSERT INTO "test_tables" VALUE {'pk': ?, 'sk': ?, 'gsi_pk': ?, 'gsi_sk': ?}`, "ExecTransactionTestSuite", 1.0, "ExecTransactionTestSuite1", "1")
		require.NoError(s.T(), err)

		rowAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(0), rowAffected)

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
				"#sk": "sk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
				":sk": &types.AttributeValueMemberN{Value: "1.0"},
			},
		}

		queryOutput, err := s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 0)

		lastInsertedID, err := result.LastInsertId()
		require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
		require.Equal(s.T(), int64(0), lastInsertedID)

		require.NoError(s.T(), tx.Commit())
		rowAffected, err = result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(1), rowAffected)

		expect := []TestTables{
			{
				PK:    "ExecTransactionTestSuite",
				SK:    1,
				GSIPK: "ExecTransactionTestSuite1",
				GSISK: "1",
			},
		}

		queryOutput, err = s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		var actual []TestTables
		attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
		require.Exactly(s.T(), expect, actual)
	})

	s.Run("update/common", func() {
		PutTestTable(s.T(), TestTables{
			PK:    "ExecTransactionTestSuite",
			SK:    1,
			GSIPK: "ExecTransactionTestSuite1",
			GSISK: "1",
		})

		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		result, err := tx.Exec(`UPDATE "test_tables" SET gsi_pk=? SET gsi_sk=? WHERE pk=? AND sk=?`, "ExecTransactionTestSuite2", "2", "ExecTransactionTestSuite", 1)
		require.NoError(s.T(), err)

		rowAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(0), rowAffected)

		expect := []TestTables{
			{
				PK:    "ExecTransactionTestSuite",
				SK:    1,
				GSIPK: "ExecTransactionTestSuite1",
				GSISK: "1",
			},
		}

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
				"#sk": "sk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
				":sk": &types.AttributeValueMemberN{Value: "1.0"},
			},
		}

		queryOutput, err := s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		var actual []TestTables
		attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
		require.Exactly(s.T(), expect, actual)

		lastInsertedID, err := result.LastInsertId()
		require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
		require.Equal(s.T(), int64(0), lastInsertedID)

		require.NoError(s.T(), tx.Commit())
		rowAffected, err = result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(1), rowAffected)

		expect = []TestTables{
			{
				PK:    "ExecTransactionTestSuite",
				SK:    1,
				GSIPK: "ExecTransactionTestSuite2",
				GSISK: "2",
			},
		}

		queryOutput, err = s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		actual = make([]TestTables, 0)
		attributevalue.UnmarshalListOfMaps(queryOutput.Items, &actual)
		require.Exactly(s.T(), expect, actual)
	})

	s.Run("delete/common", func() {
		PutTestTable(s.T(), TestTables{
			PK:    "ExecTransactionTestSuite",
			SK:    1.0,
			GSIPK: "ExecTransactionTestSuite1",
			GSISK: "1",
		})

		db := GetDB(s.T())
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(s.T(), err)

		result, err := tx.Exec(`DELETE FROM "test_tables" WHERE pk=? AND sk=?`, "ExecTransactionTestSuite", 1.0)
		require.NoError(s.T(), err)

		rowAffected, err := result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(0), rowAffected)

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("test_tables"),
			KeyConditionExpression: aws.String("#pk = :pk AND #sk = :sk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": "pk",
				"#sk": "sk",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "ExecTransactionTestSuite"},
				":sk": &types.AttributeValueMemberN{Value: "1.0"},
			},
		}

		queryOutput, err := s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 1)

		lastInsertedID, err := result.LastInsertId()
		require.ErrorIs(s.T(), err, pqxd.ErrNotSupported)
		require.Equal(s.T(), int64(0), lastInsertedID)

		require.NoError(s.T(), tx.Commit())
		rowAffected, err = result.RowsAffected()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(1), rowAffected)

		queryOutput, err = s.client.Query(context.Background(), queryInput)
		require.NoError(s.T(), err)
		require.Len(s.T(), queryOutput.Items, 0)
	})
}
