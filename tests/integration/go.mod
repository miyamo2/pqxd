module github.com/miyamo2/pqxd/tests/integration

go 1.23

replace github.com/miyamo2/pqxd => ../../

require (
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/aws/aws-sdk-go-v2 v1.32.2
	github.com/aws/aws-sdk-go-v2/credentials v1.17.41
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.15.12
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.36.2
	github.com/joho/godotenv v1.5.1
	github.com/miyamo2/pqxd v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.24.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.10.2 // indirect
	github.com/aws/smithy-go v1.22.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/mock v0.5.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
