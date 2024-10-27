package integration

import (
	"context"
	"database/sql"
	"github.com/avast/retry-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/joho/godotenv"
	"github.com/miyamo2/pqxd"
	"os"
	"testing"
	"time"
)

var (
	client *dynamodb.Client
	db     *sql.DB
)

func init() {
	_ = godotenv.Load("./.env")

	region := os.Getenv("AWS_REGION")
	ak := os.Getenv("AWS_ACCESS_KEY_ID")
	sk := os.Getenv("AWS_SECRET_ACCESS_KEY")
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")

	credential := credentials.NewStaticCredentialsProvider(ak, sk, "")

	config := aws.Config{
		Region:      region,
		Credentials: credential,
	}
	if endpoint != "" {
		config.BaseEndpoint = aws.String(endpoint)
	}
	client = dynamodb.NewFromConfig(config)

	db = sql.OpenDB(pqxd.NewConnector(config))

	err := retry.Do(
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			tb, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String("test_tables"),
			})
			if err != nil {
				return err
			}
			_ = tb
			return nil
		}, retry.Attempts(10))
	panic(err)
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
