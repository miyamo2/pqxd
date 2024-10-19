package pqxd_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/joho/godotenv"
	"github.com/miyamo2/pqxd"
	"os"
)

func Example() {
	_ = godotenv.Load("./.env")

	region := os.Getenv("AWS_REGION")
	ak := os.Getenv("AWS_ACCESS_KEY_ID")
	sk := os.Getenv("AWS_SECRET_ACCESS_KEY")

	db, err := sql.Open(pqxd.DriverName, fmt.Sprintf("AWS_REGION=%s;AWS_ACCESS_KEY_ID=%s;AWS_SECRET_ACCESS_KEY=%s", region, ak, sk))
	if err != nil {
		fmt.Println(err)
	}
	if err := db.Ping(); err != nil {
		fmt.Println(err)
	}
}

func Example_WithOpenDB() {
	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		fmt.Println(err)
	}
	db := sql.OpenDB(pqxd.NewConnector(awsConfig))
	if err != nil {
		fmt.Println(err)
	}
	if err := db.Ping(); err != nil {
		fmt.Println(err)
	}
}

func Example_Select() {
	awsConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		fmt.Println(err)
	}
	db := sql.OpenDB(pqxd.NewConnector(awsConfig))
	if err != nil {
		fmt.Println(err)
	}
	if err := db.Ping(); err != nil {
		fmt.Println(err)
	}

	rows, err := db.QueryContext(context.Background(), `SELECT id, name FROM "users"`)
	if err != nil {
		fmt.Println(err)
	}

	for rows.NextResultSet() {
		for rows.Next() {
			var id, name string
			err := rows.Scan(&id, &name)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("id: %s, name: %s\n", id, name)
		}
	}
	rows, err = db.QueryContext(context.Background(), `SELECT name FROM "users"`)
	if err != nil {
		fmt.Println(err)
	}
	for rows.NextResultSet() {
		for rows.Next() {
			var name string
			err := rows.Scan(&name)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("name: %s\n", name)
		}
	}
}
