# pqxd/tests/integration

## Tasks

We recommend that this section be run with [xc](https://github.com/joerdav/xc)

### setup:localstack

```sh
docker compose up -d
```

### setup:table

requires: setup:localstack

```sh
TABLES=$(aws dynamodb list-tables --endpoint-url http://localhost:4566 --output json --query 'TableNames')

if [[ $TABLES != *"[]"* ]]; then
  STATUS=$(aws dynamodb describe-table --table-name test_tables --endpoint-url http://localhost:4566 --output json --query 'Table.TableStatus' && true)
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

aws dynamodb create-table --cli-input-json file://testdata/table-def.json --endpoint-url http://localhost:4566
```

### test

requires: setup:table

```sh
go mod tidy
go test -v -coverpkg='github.com/miyamo2/pqxd' -coverprofile=coverage.out
```