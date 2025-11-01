package integration

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/miyamo2/pqxd"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	t.Run(
		"with dsn", func(t *testing.T) {
			db, err := sql.Open(
				pqxd.DriverName,
				fmt.Sprintf(
					"AWS_REGION=%s;AWS_ACCESS_KEY_ID=%s;AWS_SECRET_ACCESS_KEY=%s;ENDPOINT=%s",
					dotEnv["AWS_REGION"],
					dotEnv["AWS_ACCESS_KEY_ID"],
					dotEnv["AWS_SECRET_ACCESS_KEY"],
					dotEnv["DYNAMODB_ENDPOINT"],
				),
			)
			require.NoError(t, err)
			err = db.Ping()
			require.NoError(t, err)
		},
	)
	t.Run(
		"with env vars", func(t *testing.T) {
			t.Setenv("AWS_REGION", dotEnv["AWS_REGION"])
			t.Setenv("AWS_ACCESS_KEY_ID", dotEnv["AWS_ACCESS_KEY_ID"])
			t.Setenv("AWS_SECRET_ACCESS_KEY", dotEnv["AWS_SECRET_ACCESS_KEY"])
			db, err := sql.Open(
				pqxd.DriverName,
				fmt.Sprintf("ENDPOINT=%s", dotEnv["DYNAMODB_ENDPOINT"]),
			)
			require.NoError(t, err)
			err = db.Ping()
			require.NoError(t, err)
		},
	)
}

func TestOpenDB(t *testing.T) {
	t.Run(
		"with dynamodb client", func(t *testing.T) {
			db := sql.OpenDB(
				pqxd.NewConnector(aws.Config{}, pqxd.WithDynamoDBClient(client)),
			)
			err := db.Ping()
			require.NoError(t, err)
		},
	)
}
