package pqxd

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"os"
	"slices"
	"strings"
	"sync"
)

func init() {
	sql.Register(DriverName, &pqxdDriver{})
}

// compatibility check
var (
	_ driver.Driver    = (*pqxdDriver)(nil)
	_ driver.Connector = (*pqxdDriver)(nil)
)

// pqxdDriver is an implementation of driver.Driver.
type pqxdDriver struct {
	// awsConfig is the aws.Config for the connection.
	awsConfig *aws.Config

	// connParam is the connection parameters.
	connParam connectionParam

	// connParamMu is the lock for connParam.
	connParamMu sync.RWMutex
}

// Open See: driver.Driver.
func (d *pqxdDriver) Open(connectionString string) (driver.Conn, error) {
	if d.awsConfig != nil {
		return newConnection(dynamodb.NewFromConfig(*d.awsConfig)), nil
	}

	return d.open(connectionString)
}

// Connect See: driver.Connector.
func (d *pqxdDriver) Connect(_ context.Context) (driver.Conn, error) {
	client := dynamodb.NewFromConfig(*d.awsConfig)
	return newConnection(client), nil
}

// Driver See: driver.Connector.
func (d *pqxdDriver) Driver() driver.Driver {
	return d
}

// ConnectorSetting is the setting for the connector.
type ConnectorSetting struct{}

// ConnectorOption is the option for the connector.
type ConnectorOption func(*ConnectorSetting)

// NewConnector creates a new connector with the given aws.Config and ConnectorOption.
func NewConnector(awsConfig aws.Config, options ...ConnectorOption) driver.Connector {
	setting := ConnectorSetting{}
	for _, option := range options {
		option(&setting)
	}
	return &pqxdDriver{
		awsConfig: &awsConfig,
	}
}

// connectionString keys
const (
	connectionStringKeyRegion    = "AWS_REGION"
	connectionStringKeyAccessKey = "AWS_ACCESS_KEY_ID"
	connectionStringKeySecret    = "AWS_SECRET_ACCESS_KEY"
	connectionStringEndpoint     = "ENDPOINT"
)

// environment variables key: Region
const (
	envVarKeyAWSRegion        = "AWS_REGION"
	envVarKeyAWSDefaultRegion = "AWS_DEFAULT_REGION"
)

// environment variables key: Access Key ID
const (
	envVarKeyAWSAccessKey   = "AWS_ACCESS_KEY"
	envVarKeyAWSAccessKeyID = "AWS_ACCESS_KEY_ID"
)

// environment variables key: Secret Access Key
const (
	envVarKeyAWSSecretKey       = "AWS_SECRET_KEY"
	envVarKeyAWSSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
)

// open establishes a connection with a DynamoDB with a connection string.
func (d *pqxdDriver) open(connStr string) (driver.Conn, error) {
	// intended to be called only once for a cold start, but with careful exclusivity control.
	d.connParamMu.Lock()
	defer d.connParamMu.Unlock()
	if d.connParam == nil {
		d.connParam = newConnectionParam(connStr)
	}
	opts := dynamoDBOptionsFromParams(d.connParam)
	client := dynamodb.New(opts)

	return newConnection(client), nil
}

// connectionParam is Key/Value pair for the connection string.
type connectionParam map[string]string

// newConnectionParam returns a new connectionParam from the connection string.
func newConnectionParam(connStr string) connectionParam {
	params := make(map[string]string)
	for _, paramStr := range strings.Split(connStr, ";") {
		kv := strings.SplitN(strings.TrimSpace(paramStr), "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		if key == "" {
			continue
		}
		value := strings.TrimSpace(kv[1])
		if value == "" {
			continue
		}
		params[strings.ToUpper(key)] = value
	}
	return params
}

// lookupOr returns the value of the key if it exists, otherwise returns the first non-empty value from the alternatives.
func (c connectionParam) lookupOr(key string, alt ...string) string {
	v, ok := c[key]
	if ok {
		return v
	}
	idx := slices.IndexFunc(alt, func(s string) bool {
		return s != ""
	})
	if idx == -1 {
		return ""
	}
	return alt[idx]
}

// lookup returns the value of the key if it exists, otherwise returns nil.
func (c connectionParam) lookup(key string) *string {
	v, ok := c[key]
	if !ok {
		return nil
	}
	return &v
}

// dynamoDBOptionsFromParams returns dynamodb.Options from the connectionParam.
func dynamoDBOptionsFromParams(connParam connectionParam) dynamodb.Options {
	region := connParam.lookupOr(connectionStringKeyRegion, os.Getenv(envVarKeyAWSRegion), os.Getenv(envVarKeyAWSDefaultRegion))

	ak := connParam.lookupOr(connectionStringKeyAccessKey, os.Getenv(envVarKeyAWSAccessKeyID), os.Getenv(envVarKeyAWSAccessKey))
	sk := connParam.lookupOr(connectionStringKeySecret, os.Getenv(envVarKeyAWSSecretKey), os.Getenv(envVarKeyAWSSecretAccessKey))

	// generally, this is a permanent credential and therefore does not use a session token.
	creds := credentials.NewStaticCredentialsProvider(ak, sk, "")

	endpoint := connParam.lookup(connectionStringEndpoint)

	var disableHttps bool
	if endpoint != nil {
		disableHttps = strings.HasPrefix(*endpoint, "http://")
	}

	return dynamodb.Options{
		Region:       region,
		Credentials:  creds,
		BaseEndpoint: endpoint,
		EndpointOptions: dynamodb.EndpointResolverOptions{
			DisableHTTPS: disableHttps,
		},
	}
}
