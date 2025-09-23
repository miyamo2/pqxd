# pqxd - [database/sql](https://golang.org/pkg/database/sql/)  driver for [PartiQL in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html)

[![Go Reference](https://pkg.go.dev/badge/github.com/miyamo2/pqxd.svg)](https://pkg.go.dev/github.com/miyamo2/pqxd)
[![CI](https://github.com/miyamo2/pqxd/actions/workflows/ci.yaml/badge.svg)](https://github.com/miyamo2/pqxd/actions/workflows/ci.yaml)
[![GitHub go.mod Go version (subdirectory of monorepo)](https://img.shields.io/github/go-mod/go-version/miyamo2/pqxd?logo=go)](https://img.shields.io/github/go-mod/go-version/miyamo2/pqxd?logo=go)
[![GitHub release (latest by date)](https://img.shields.io/github/v/release/miyamo2/pqxd)](https://img.shields.io/github/v/release/miyamo2/pqxd)
[![Go Report Card](https://goreportcard.com/badge/github.com/miyamo2/pqxd)](https://goreportcard.com/report/github.com/miyamo2/pqxd)
[![GitHub License](https://img.shields.io/github/license/miyamo2/pqxd?&color=blue)](https://img.shields.io/github/license/miyamo2/pqxd?&color=blue)

## Quick Start

### Install

```sh
go get github.com/miyamo2/pqxd
```

### Usage

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/miyamo2/pqxd"
)

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	db := sql.OpenDB(pqxd.NewConnector(cfg))
	if db == nil {
		log.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `SELECT id, name FROM "users"`)
	if err != nil {
		fmt.Printf("something happend. err: %s\n", err.Error())
		return
	}

	for rows.NextResultSet() { // page feed with next token
		for rows.Next() {
			var (
				id   string
				name string
			)
			if err := rows.Scan(&id, &name); err != nil {
				fmt.Printf("something happend. err: %s\n", err.Error())
				continue
			}
			fmt.Printf("id: %s, name: %s\n", id, name)
		}
	}
}
```

> [!Important]
> If `Ping` is to be performed, `dynamodb:ListTables` policy must be set for the IAM role.

#### `SELECT`

> [!TIP]
> If `*` is specified in the select column list,  
> the results of the rows are automatically sorted by column name(asc).
> 
> However, if specified with *, the number of attributes may differ from row to row. 
> 
> Therefore, it is recommended that the selection column list specify the attribute names.

##### Scan

```go
rows, err := db.QueryContext(context.Background(), `SELECT id, name FROM "users"`)
for rows.NextResultSet() { // page feed with next token
    for rows.Next() {
        var (
            id string
            name string
        )
        if err := rows.Scan(&id, &name); err != nil {
            fmt.Printf("something happend. err: %s\n", err.Error())
            continue
        }
        fmt.Printf("id: %s, name: %s\n", id, name)
    }
}
```

##### GetItem

```go
row := db.QueryRowContext(context.Background(), `SELECT id, name FROM "users" WHERE id = ?`, "1")
var (
    id string
    name string
)
if err := row.Scan(&id, &name); err != nil {
    fmt.Printf("something happend. err: %s\n", err.Error())
    return
}
fmt.Printf("id: %s, name: %s\n", id, name)
```

##### GetItem with Global Secondary Index

```go
row := db.QueryRowContext(context.Background(), `SELECT id, name FROM "users"."gsi_pk-gsi-sk_index" WHERE gsi_pk = ? AND gsi_sk = ?`, "foo", "bar")

var (
    id string
    name string
)
if err := row.Scan(&id, &name); err != nil {
    fmt.Printf("something happend. err: %s\n", err.Error())
    return
}
fmt.Printf("id: %s, name: %s\n", id, name)
```

##### With Prepared Statement

```go
ctx := context.Background()

stmt, err := db.PrepareContext(ctx, `SELECT id, name FROM "users" WHERE id = ?`)
if err != nil {
    fmt.Printf("something happend. err: %s\n", err.Error())
    return
}
defer stmt.Close()

rows, err := stmt.QueryRowContext(ctx, "1")
if err != nil {
    fmt.Printf("something happend. err: %s\n", err.Error())
    return
}

var (
    id string
    name string
)
if err := row.Scan(&id, &name); err != nil {
    fmt.Printf("something happend. err: %s\n", err.Error())
    return
}
fmt.Printf("id: %s, name: %s\n", id, name)
```

##### With Transaction

```go
tx, err := db.Begin()
if err != nil {
    return err
}

ctx := context.Background()

rows, err := tx.QueryContext(ctx, `SELECT id, name FROM "users" WHERE id = ?`, "1")
if err != nil {
    tx.Rollback()
    return err
}

row := tx.QueryRowContext(ctx, `SELECT id, name FROM "users" WHERE id = ?`, "2")

// WARNING: Do not use `tx.Commit()` when using `SELECT` statement.
//
// Each `sql.Rows` or `sql.Row` is resolved 
// the first time `rows.NextResultSet()`, `rows.Next()` or `row.Scan()` 
// is performed within that transaction.
// So, after the `rows.NextResultSet()`, `rows.Next()` or `row.Scan()` is performed,
// the transaction is automatically committed.
for rows.Next() {
    var (
        id string
        name string
    )
    if err := rows.Scan(&id, &name); err != nil {
        fmt.Printf("something happend. err: %s\n", err.Error())
        continue
    }
    fmt.Printf("id: %s, name: %s\n", id, name)
}

var (
    id string
    name string
)
if err := row.Scan(&id, &name); err != nil {
    fmt.Printf("something happend. err: %s\n", err.Error())
    return
}
fmt.Printf("id: %s, name: %s\n", id, name)
```

##### `RETURNING`

`pqxd` supports the `RETURNING` clause.

```go
row := db.QueryRowContext(context.Background(), `UPDATE "users" SET name = ? SET nickname = ? WHERE id = ? RETURNING MODIFIED OLD *`, "David", "Dave", "3")

var name, nickname sql.NullString
var disabled sql.NullBool
if err := row.Scan(&name, &nickname); err != nil {
    fmt.Printf("something happend. err: %s\n", err.Error())
    return
}
if name.Valid {
    fmt.Printf("name: %s\n", name.String)
}
if nickname.Valid {
    fmt.Printf("nickname: %s\n", nickname.String)
}
```

And provides individual syntax for specifying a column list instead of `*`.

```go
row := db.QueryRowContext(context.Background(), `UPDATE "users" SET name = ? SET nickname = ? WHERE id = ? RETURNING ALL OLD id`, "Robert", "Bob", "2")

var id string
if err := row.Scan(&id); err != nil {
    fmt.Printf("something happend. err: %s\n", err.Error())
    return
}
fmt.Printf("id: %s\n", id)
```

##### Describe Table

`pqxd` supports the [DescribeTable API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html) with `!pqxd_describe_table`, the meta-table.

```go
row := db.QueryRowContext(context.Background(), `SELECT TableStatus FROM "!pqxd_describe_table" WHERE table_name = ?`, "users")

var tableStatus pqxd.TableStatus
if err := row.Scan(&tableStatus); err != nil {
    fmt.Println(err.Error())
    return
}
fmt.Printf("TableStatus: %v\n", tableStatus)
```

##### List Tables

`pqxd` supports the [ListTables API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html) with `!pqxd_list_tables`, the meta-table.

```go
rows, err := db.QueryContext(context.Background(), `SELECT * FROM "!pqxd_list_tables"`)

for rows.NextResultSet() { // page feed with last-evaluated-key
    for rows.Next() {
        var tableName string
        if err := rows.Scan(&tableName); err != nil {
            fmt.Println(err.Error())
            continue
        }
        fmt.Printf("tableName: %s\n", tableName)
    }
}
```

#### `INSERT`/`UPDATE`/`DELETE`

```go
insertResult, err := db.Exec(`INSERT INTO "users" VALUE { 'id': ?, 'name': ? }`, "3", "Alice")
if err != nil {
    return err
}
affected, err := insertResult.RowsAffected()
if err != nil {
    return err
}
if affected != 1 {
    return fmt.Errorf("expected 1 row affected, got %d", affected)
}

updateResult, err := db.Exec(`UPDATE "users" SET name = ? WHERE id = ?`, "Bob", "2")
if err != nil {
    return err
}
affected, err = updateResult.RowsAffected()
if err != nil {
    return err
}
if affected != 1 {
    return fmt.Errorf("expected 1 row affected, got %d", affected)
}

deleteResult, err := db.Exec(`DELETE FROM "users" WHERE id = ?`, "1")
if err != nil {
    return err
}
affected, err = deleteResult.RowsAffected()
if err != nil {
    return err
}
if affected != 1 {
    return fmt.Errorf("expected 1 row affected, got %d", affected)
}
```

##### With Prepared Statement

```go
stmt, err := db.Prepare(`INSERT INTO "users" VALUE { 'id': ?, 'name': ? }`)
if err != nil {
    return err
}
defer stmt.Close()

insertResult, err := stmt.Exec("3", "Alice")
if err != nil {
    return err
}
affected, err := insertResult.RowsAffected()
if err != nil {
    return err
}
if affected != 1 {
    return fmt.Errorf("expected 1 row affected, got %d", affected)
}
```

##### With Transaction

```go
tx, err := db.Begin()
if err != nil {
    return err
}

insertResult, err := tx.Exec(`INSERT INTO "users" VALUE { 'id': ?, 'name': ? }`, "3", "Alice")
if err != nil {
    tx.Rollback()
    return err
}

updateResult, err := tx.Exec(`UPDATE "users" SET name = ? WHERE id = ?`, "Bob", "2")
if err != nil {
    tx.Rollback()
    return err
}

deleteResult, err := tx.Exec(`DELETE FROM "users" WHERE id = ?`, "1")
if err != nil {
    tx.Rollback()
    return err
}

// RowsAffected is available after commit
tx.Commit()

// RowsAffected might return 0 or 1. If 0, it means statement is not successful.
if affected, err := insertResult.RowsAffected(); err != nil || affected != 1 {
    return err
}

if affected, err := updateResult.RowsAffected(); err != nil || affected != 1 {
    return err
}

if affected, err := deleteResult.RowsAffected(); err != nil || affected != 1 {
    return err
}
```

#### DSN(Data Source Name) String

We recommend using `sql.OpenDB` with `pqxd.NewConnector` instead of `sql.Open`.
But if you want to use `sql.Open`, you can use the following DSN string.

```sh
AWS_REGION=<aws region>
;AWS_ACCESS_KEY_ID=<access key ID>
;AWS_SECRET_ACCESS_KEY=<secret access key>
[;ENDPOINT=<amazon dynamodb endpoint>]
```

| Key                     | description                                                                                                                                                                                                                                      |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `AWS_REGION`            | [AWS Region](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-regions). If not supplied, it is resolved from one of the following environment variables; `AWS_REGION` or `AWS_DEFAULT_REGION`. |
| `AWS_ACCESS_KEY_ID`     | [AWS Access Key ID](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html). If not supplied, it is resolved from one of the following environment variables; `AWS_ACCESS_KEY` or `AWS_ACCESS_KEY_ID`.                 |
| `AWS_SECRET_ACCESS_KEY` | [AWS Secret Access Key](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html). If not supplied, it is resolved from one of the following environment variables; `AWS_SECRET_KEY` or `AWS_SECRET_ACCESS_KEY`.         |
| `ENDPOINT`              | Endpoint of DynamoDB. Used to connect locally to an emulator or to a DynamoDB compatible interface.                                                                                                                                              |

```go
db, err := sql.Open(pqxd.DriverName, "AWS_REGION=ap-northeast-1;AWS_ACCESS_KEY_ID=AKIA...;AWS_SECRET_ACCESS_KEY=...;")
```

> [!TIP]
> If the application is run on AWS Lambda, connections can be obtained even if the DSN is an empty string.
> This is because the region, access key, and secret key are defined as [runtime environment variables](https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime).

#### O11y

##### New Relic

```go
package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/miyamo2/pqxd"
	nraws "github.com/newrelic/go-agent/v3/integrations/nrawssdk-v2"
)

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Instrumenting New Relic
	nraws.AppendMiddlewares(&cfg.APIOptions, nil)

	db := sql.OpenDB(pqxd.NewConnector(cfg))
	if db == nil {
		log.Fatal(err)
	}
	db.Ping()
}
```

##### Datadog

```go
package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/miyamo2/pqxd"
	awstrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/aws/aws-sdk-go-v2/aws"
)

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Instrumenting Datadog
	awstrace.AppendMiddleware(&cfg)

	db := sql.OpenDB(pqxd.NewConnector(cfg))
	if db == nil {
		log.Fatal(err)
	}
	db.Ping()
}
```

##### AWS X-Ray

```go
package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/miyamo2/pqxd"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-xray-sdk-go/instrumentation/awsv2"
	"github.com/aws/aws-xray-sdk-go/xray"
)

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	
	// Instrumenting X-Ray
	awsv2.AWSV2Instrumentor(&cfg.APIOptions)

	db := sql.OpenDB(pqxd.NewConnector(cfg))
	if db == nil {
		log.Fatal(err)
	}
	db.Ping()
}
```

## Contributing

Feel free to open a PR or an Issue.

However, you must promise to follow our [Code of Conduct](https://github.com/miyamo2/pqxd/blob/main/CODE_OF_CONDUCT.md).

### Tasks

We recommend that this section be run with [xc](https://github.com/joerdav/xc)

#### test:unit

```sh
go test -v -coverpkg='github.com/miyamo2/pqxd' -coverprofile=coverage.out
```

#### test:integration

```sh
cd tests/integration
xc test
```

## License

**pqxd** released under the [MIT License](https://github.com/miyamo2/pqxd/blob/main/LICENSE)

## Special Thanks

`pqxd` is inspired by the following projects.  
With the utmost respect, we would like to thank the authors and contributors of these projects.

- [btnguyen2k/godynamo](https://github.com/btnguyen2k/godynamo)
- [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)
- [lib/pq](https://github.com/lib/pq)
- [jackc/pgx](https://github.com/jackc/pgx)
