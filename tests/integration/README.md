# pqxd/tests/integration

## Tasks

We recommend that this section be run with [xc](https://github.com/joerdav/xc)

### setup:awslim

```sh
if [ -e awslim ]; then
  exit 0
fi
docker run -it -v $(pwd)/gen.yaml:/app/gen.yaml ghcr.io/fujiwara/awslim:builder
docker cp $(docker ps -lq):/app/awslim .
```

### setup:dynamodb

```sh
docker compose up -d 
```

### setup:table

requires: setup:awslim, setup:dynamodb  

Inputs: AWS_ENDPOINT_URL, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  

Environment: AWS_ENDPOINT_URL=http://localhost:8000, AWS_REGION=ap-northeast-1, AWS_ACCESS_KEY_ID=ABC1234567890, AWS_SECRET_ACCESS_KEY=ABC1234567890  

```sh
TABLES=$(./awslim dynamodb ListTables --query TableNames)

if [[ $TABLES != *"[]"* ]]; then
  STATUS=$(./awslim dynamodb DescribeTable '{"TableName": "test_tables"}' --query Table.TableStatus && true)
  if [[ $STATUS == *"ACTIVE"* ]]; then
    echo "Table already exists"
    exit 0
  fi
  if [[ $STATUS == *"CREATING"* ]]; then
    echo "Table is creating"
    exit 0
  fi
  if [[ $STATUS == *"UPDATING"* ]]; then
    echo "Table is updating"
    exit 0
  fi
fi

./awslim dynamodb CreateTable "`cat ./testdata/table-def.json`"
```

### test

requires: setup:table  

Inputs: DYNAMODB_ENDPOINT, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  

Environment: DYNAMODB_ENDPOINT=http://localhost:8000, AWS_REGION=ap-northeast-1, AWS_ACCESS_KEY_ID=ABC1234567890, AWS_SECRET_ACCESS_KEY=ABC1234567890

```sh
go mod tidy
go test -v -coverpkg='github.com/miyamo2/pqxd' -coverprofile=coverage.out
```