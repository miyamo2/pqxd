package pqxd

import "database/sql/driver"

// compatibility checks
var (
	_ driver.Result = (*pqxdResult)(nil)
)

// pqxdResult is an implementation of driver.Result
type pqxdResult struct {
	// affected is the number of rows affected
	affected int64
}

// LastInsertId See: driver.Result
func (r pqxdResult) LastInsertId() (int64, error) {
	return 0, ErrNotSupported
}

// RowsAffected See: driver.Result
func (r pqxdResult) RowsAffected() (int64, error) {
	return r.affected, nil
}

// newPqxdResult returns new pqxdResult
func newPqxdResult(affected int64) *pqxdResult {
	return &pqxdResult{affected: affected}
}
