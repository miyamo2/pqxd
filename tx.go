package pqxd

import (
	"database/sql/driver"
)

// transaction extends driver.Tx
type transaction interface {
	driver.Tx
}

// compatibility check
var _ transaction = (*queryTx)(nil)

// queryTx is an implementation of transaction for `SELECT` statements.
type queryTx struct{}

// Commit See: driver.Tx
func (t queryTx) Commit() error {
	//TODO implement me
	panic("implement me")
}

// Rollback See: driver.Tx
func (t queryTx) Rollback() error {
	//TODO implement me
	panic("implement me")
}

// compatibility check
var _ transaction = (*execTx)(nil)

// execTx is an implementation of transaction for `INSERT`, `UPDATE`, and `DELETE` statements.
type execTx struct{}

// Commit See: driver.Tx
func (t execTx) Commit() error {
	//TODO implement me
	panic("implement me")
}

// Rollback See: driver.Tx
func (t execTx) Rollback() error {
	//TODO implement me
	panic("implement me")
}
