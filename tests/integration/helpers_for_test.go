package integration

import (
	"database/sql"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/joho/godotenv"
	"github.com/miyamo2/pqxd"
	"os"
	"testing"
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
	// var err error
	// db, err = sql.Open(pqxd.DriverName, fmt.Sprintf("AWS_REGION=%s;AWS_ACCESS_KEY_ID=%s;AWS_SECRET_ACCESS_KEY=%s;ENDPOINT=%s", region, ak, sk, endpoint))
	//if err != nil {
	//	panic(fmt.Sprintf("failed to open database, got error %v", err))
	//}
}

func GetDB(t *testing.T) *sql.DB {
	t.Helper()
	return db
}

func GetClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	return client
}
