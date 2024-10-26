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

// lazyResult is resolve the affected rows lazily
type lazyResult struct {
	getAffected func() (int64, error)
}

// LastInsertId See: driver.Result
func (r lazyResult) LastInsertId() (int64, error) {
	return 0, ErrNotSupported
}

// RowsAffected See: driver.Result
func (r lazyResult) RowsAffected() (int64, error) {
	return r.getAffected()
}

// newLazyResult returns new lazyResult
func newLazyResult(getAffected func() (int64, error)) driver.Result {
	return lazyResult{getAffected: getAffected}
}
