package integration

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/avast/retry-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/joho/godotenv"
	"github.com/miyamo2/pqxd"
)

var (
	client            *dynamodb.Client
	db                *sql.DB
	mu                sync.Mutex
	region            string
	accessKeyID       string
	secretAccessKeyID string
	endpoint          string
)

var dotEnv map[string]string

func init() {
	_ = godotenv.Load("./.env")

	var err error
	dotEnv, err = godotenv.Read("./.env")
	if err != nil {
		panic(err)
	}
	region = dotEnv["AWS_REGION"]
	accessKeyID = dotEnv["AWS_ACCESS_KEY_ID"]
	secretAccessKeyID = dotEnv["AWS_SECRET_ACCESS_KEY"]
	endpoint = dotEnv["DYNAMODB_ENDPOINT"]

	credential := credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKeyID, "")

	config := aws.Config{
		Region:      region,
		Credentials: credential,
	}
	if endpoint != "" {
		config.BaseEndpoint = aws.String(endpoint)
	}
	client = dynamodb.NewFromConfig(config)

	db = sql.OpenDB(pqxd.NewConnector(config))

	err = retry.Do(
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			tb, err := client.DescribeTable(
				ctx, &dynamodb.DescribeTableInput{
					TableName: aws.String("test_tables"),
				},
			)
			if err != nil {
				return err
			}
			_ = tb
			return nil
		}, retry.Attempts(10),
	)
	if err != nil {
		panic(err)
	}
}

func GetDB(t *testing.T) *sql.DB {
	t.Helper()
	return db
}

func GetClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	return client
}

func PutTestTable(t *testing.T, data ...TestTables) {
	t.Helper()
	for _, v := range data {
		av, err := attributevalue.MarshalMap(v)
		if err != nil {
			t.Fatalf("failed to marshal map: %s", err.Error())
		}
		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String("test_tables"),
		}
		if _, err = client.PutItem(context.Background(), input); err != nil {
			t.Fatalf("failed to put item: %s", err)
		}
	}
}
