package pqxd_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/miyamo2/pqxd"
	"os"
)

func MustDB() *sql.DB {
	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}
	return sql.OpenDB(pqxd.NewConnector(awsConfig))
}

func Example() {
	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	db := sql.OpenDB(pqxd.NewConnector(awsConfig))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if err := db.Ping(); err != nil {
		fmt.Println(err.Error())
	}
}

func Example_withOpen() {
	region := os.Getenv("AWS_REGION")
	ak := os.Getenv("AWS_ACCESS_KEY_ID")
	sk := os.Getenv("AWS_SECRET_ACCESS_KEY")

	db, err := sql.Open(pqxd.DriverName, fmt.Sprintf("AWS_REGION=%s;AWS_ACCESS_KEY_ID=%s;AWS_SECRET_ACCESS_KEY=%s", region, ak, sk))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if err := db.Ping(); err != nil {
		fmt.Println(err.Error())
	}
}

func Example_queryContext() {
	db := MustDB()
	rows, err := db.QueryContext(context.Background(), `SELECT id, name FROM "users"`)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	for rows.NextResultSet() {
		for rows.Next() {
			var id, name string
			err := rows.Scan(&id, &name)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("id: %s, name: %s\n", id, name)
		}
	}
	rows, err = db.QueryContext(context.Background(), `SELECT name FROM "users"`)
	if err != nil {
		fmt.Println(err.Error())
	}
	for rows.NextResultSet() {
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("name: %s\n", name)
		}
	}
}

func Example_queryRowContext() {
	db := MustDB()

	var id, name string
	err := db.QueryRowContext(context.Background(), `SELECT id, name FROM "users" WHERE id = ?`, 1).Scan(&id, &name)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("id: %s, name: %s\n", id, name)
}

func Example_execContext() {
	db := MustDB()

	insertResult, err := db.Exec(`INSERT INTO "users" VALUE { 'id': ?, 'name': ? }`, "3", "Alice")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	affected, err := insertResult.RowsAffected()
	if err != nil {
		fmt.Println(err.Error())
	}
	if affected != 1 {
		fmt.Println(fmt.Errorf("expected 1 row affected, got %d", affected))
		return
	}

	updateResult, err := db.Exec(`UPDATE "users" SET "name" = ? WHERE id = ?`, "Bob", "2")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	affected, err = updateResult.RowsAffected()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if affected != 1 {
		fmt.Println(fmt.Errorf("expected 1 row affected, got %d", affected))
		return
	}

	deleteResult, err := db.Exec(`DELETE FROM "users" WHERE id = ?`, "1")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	affected, err = deleteResult.RowsAffected()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if affected != 1 {
		fmt.Println(fmt.Errorf("expected 1 row affected, got %d", affected))
		return
	}
}

func Example_prepareContext() {
	db := MustDB()

	stmt, err := db.PrepareContext(context.Background(), `SELECT id, name FROM "users" WHERE id = ?`)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer stmt.Close()

	row := stmt.QueryRowContext(context.Background(), 1)
	var id, name string
	if err := row.Scan(&id, &name); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("id: %s, name: %s\n", id, name)

	stmt, err = db.PrepareContext(context.Background(), `INSERT INTO "users" VALUE { 'id': ?, 'name': ? }`)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer stmt.Close()

	insertResult, err := stmt.Exec("3", "Alice")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	affected, err := insertResult.RowsAffected()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if affected != 1 {
		fmt.Println(fmt.Errorf("expected 1 row affected, got %d", affected))
		return
	}
}

func Example_queryInTransaction() {
	db := MustDB()

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	row := tx.QueryRowContext(context.Background(), `SELECT id, name FROM "users" WHERE id = ?`, 1)
	rows, err := tx.QueryContext(context.Background(), `SELECT id, name FROM "users" WHERE id = ?`, 2)
	if err != nil {
		fmt.Println(err.Error())
		tx.Rollback()
		return
	}

	// WARNING: Do not use `tx.Commit()` when using `SELECT` statement.
	//
	// Each `sql.Rows` or `sql.Row` is resolved
	// the first time `rows.NextResultSet()`, `rows.Next()` or `row.Scan()`
	// is performed within that transaction.
	// So, after the `rows.NextResultSet()`, `rows.Next()` or `row.Scan()` is performed,
	// the transaction is automatically committed.
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			fmt.Println(err.Error())
			tx.Rollback()
			return
		}
		fmt.Printf("id: %s, name: %s\n", id, name)
	}

	var id, name string
	if err := row.Scan(id, name); err != nil {
		return
	}
	fmt.Printf("id: %s, name: %s\n", id, name)
}

func Example_execInTransaction() {
	db := MustDB()

	tx, err := db.Begin()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	insertResult, err := tx.Exec(`INSERT INTO "users" VALUE { 'id': ?, 'name': ? }`, "3", "Alice")
	if err != nil {
		fmt.Println(err.Error())
		tx.Rollback()
		return
	}

	updateResult, err := tx.Exec(`UPDATE "users" SET "name" = ? WHERE id = ?`, "Bob", "2")
	if err != nil {
		fmt.Println(err.Error())
		tx.Rollback()
		return
	}

	deleteResult, err := tx.Exec(`DELETE FROM "users" WHERE id = ?`, "1")
	if err != nil {
		fmt.Println(err.Error())
		tx.Rollback()
		return
	}

	// RowsAffected is available after commit
	tx.Commit()

	// RowsAffected might return 0 or 1. If 0, it means statement is not successful.
	if affected, err := insertResult.RowsAffected(); err != nil || affected != 1 {
		fmt.Println(err.Error())
		return
	}

	if affected, err := updateResult.RowsAffected(); err != nil || affected != 1 {
		fmt.Println(err.Error())
		return
	}

	if affected, err := deleteResult.RowsAffected(); err != nil || affected != 1 {
		fmt.Println(err.Error())
		return
	}
}

func ExampleNewConnector() {
	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	db := sql.OpenDB(pqxd.NewConnector(awsConfig))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if err := db.Ping(); err != nil {
		fmt.Println(err.Error())
	}
}

func Example_returning() {
	db := MustDB()

	row := db.QueryRowContext(context.Background(), `UPDATE "users" SET name = ? SET nickname = ? WHERE id = ? RETURNING MODIFIED OLD *`, "David", "Dave", "3")

	var name, nickname sql.NullString
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

	row = db.QueryRowContext(context.Background(), `UPDATE "users" SET name = ? SET nickname = ? WHERE id = ? RETURNING ALL OLD id`, "Bob", "2")
	var id string
	if err := row.Scan(&id); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("id: %s\n", id)
}

func Example_describeTable() {
	db := MustDB()

	row := db.QueryRowContext(context.Background(), `SELECT * FROM "!pqxd_describe_table" WHERE table_name = ?`, "test_tables")
	var (
		archivalSummary           pqxd.ArchivalSummary
		attributeDefinitions      pqxd.AttributeDefinitions
		billingModeSummary        pqxd.BillingModeSummary
		creationDateTime          pqxd.CreationDateTime
		deletionProtectionEnabled pqxd.DeletionProtectionEnabled
		keySchema                 pqxd.KeySchema
		globalSecondaryIndexes    pqxd.GlobalSecondaryIndexes
		globalTableVersion        pqxd.GlobalTableVersion
		itemCount                 pqxd.ItemCount
		localSecondaryIndexes     pqxd.LocalSecondaryIndexes
		onDemandThroughput        pqxd.OnDemandThroughput
		provisionedThroughput     pqxd.ProvisionedThroughput
		replicas                  pqxd.Replicas
		restoreSummary            pqxd.RestoreSummary
		sseDescription            pqxd.SSEDescription
		streamSpecification       pqxd.StreamSpecification
		tableClassSummary         pqxd.TableClassSummary
		tableStatus               pqxd.TableStatus
	)
	err := row.Scan(&archivalSummary,
		&attributeDefinitions,
		&billingModeSummary,
		&creationDateTime,
		&deletionProtectionEnabled,
		&keySchema,
		&globalSecondaryIndexes,
		&globalTableVersion,
		&itemCount,
		&localSecondaryIndexes,
		&onDemandThroughput,
		&provisionedThroughput,
		&replicas,
		&restoreSummary,
		&sseDescription,
		&streamSpecification,
		&tableClassSummary,
		&tableStatus,
	)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("archivalSummary: %v\n", archivalSummary)
}

func Example_describeTableWithSpecifyColumn() {
	db := MustDB()

	row := db.QueryRowContext(context.Background(), `SELECT TableStatus FROM "!pqxd_describe_table" WHERE table_name = ?`, "test_tables")

	var tableStatus pqxd.TableStatus
	if err := row.Scan(&tableStatus); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("TableStatus: %v\n", tableStatus)
}

func Example_listTables() {
	db := MustDB()

	rows, err := db.QueryContext(context.Background(), `SELECT * FROM "!pqxd_list_tables"`)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	for rows.NextResultSet() {
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("tableName: %s\n", tableName)
		}
	}
}
