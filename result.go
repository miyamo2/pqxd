package pqxd

import "database/sql/driver"

// compatibility checks
var (
	_ driver.Result = (*result)(nil)
)

type result struct{}

func (r result) LastInsertId() (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (r result) RowsAffected() (int64, error) {
	//TODO implement me
	panic("implement me")
}
