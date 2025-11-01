package pqxd

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func init() {
	sql.Register(DriverName, &pqxdDriver{})
}

// compatibility check
var (
	_ driver.Driver        = (*pqxdDriver)(nil)
	_ driver.DriverContext = (*pqxdDriver)(nil)
	_ driver.Connector     = (*pqxdDriver)(nil)
)

// ConnectorSetting is the setting for the connector.
type ConnectorSetting struct {
	client DynamoDBClient
}

// ConnectorOption is the option for the connector.
type ConnectorOption func(*ConnectorSetting)

// WithDynamoDBClient settings the DynamoDB client to the connector.
func WithDynamoDBClient(client DynamoDBClient) ConnectorOption {
	return func(s *ConnectorSetting) {
		s.client = client
	}
}

// NewConnector creates a new connector with the given aws.Config and ConnectorOption.
func NewConnector(awsConfig aws.Config, options ...ConnectorOption) driver.Connector {
	var setting ConnectorSetting
	for _, option := range options {
		option(&setting)
	}
	if setting.client == nil {
		setting.client = dynamodb.NewFromConfig(awsConfig)
	}
	var clientMap sync.Map
	clientMap.Store(clientKey{}, setting.client)
	return &pqxdDriver{
		clientMap: clientMap,
	}
}

type clientKey struct{}

type pqxdDriver struct {
	clientMap    sync.Map
	connectorMap sync.Map
}

// Open See: driver.Driver.
// Deprecated: Open is no longer called from [database/sql] since OpenConnector was implemented.
func (d *pqxdDriver) Open(_ string) (driver.Conn, error) {
	return nil, nil
}

// OpenConnector See: driver.DriverContext.
func (d *pqxdDriver) OpenConnector(name string) (driver.Connector, error) {
	if _connector, ok := d.connectorMap.Load(name); ok {
		return _connector.(driver.Connector), nil
	}
	opts := dynamoDBOptionsFromParams(newConnectionParam(name))
	_connector := NewConnector(aws.Config{}, WithDynamoDBClient(dynamodb.New(opts)))
	d.connectorMap.Store(name, _connector)
	return _connector, nil
}

// Connect See: driver.Connector.
func (d *pqxdDriver) Connect(_ context.Context) (driver.Conn, error) {
	// This key will definitely hit.
	client, _ := d.clientMap.Load(clientKey{})
	return newConnection(client.(DynamoDBClient)), nil
}

// Driver See: driver.Connector.
func (d *pqxdDriver) Driver() driver.Driver {
	return d
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
	idx := slices.IndexFunc(
		alt, func(s string) bool {
			return s != ""
		},
	)
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
	region := connParam.lookupOr(
		connectionStringKeyRegion,
		os.Getenv(envVarKeyAWSRegion),
		os.Getenv(envVarKeyAWSDefaultRegion),
	)

	ak := connParam.lookupOr(
		connectionStringKeyAccessKey,
		os.Getenv(envVarKeyAWSAccessKeyID),
		os.Getenv(envVarKeyAWSAccessKey),
	)
	sk := connParam.lookupOr(
		connectionStringKeySecret,
		os.Getenv(envVarKeyAWSSecretKey),
		os.Getenv(envVarKeyAWSSecretAccessKey),
	)

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
