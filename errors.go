package pqxd

import "errors"

var (
	// ErrNotSupported occurs when performed operation that is not supported in pqxd
	ErrNotSupported = errors.New("pqxd: not supported this operation")

	// ErrInvalidPreparedStatement occurs when the prepared statement is invalid
	ErrInvalidPreparedStatement = errors.New("pqxd: invalid prepared statement")

	// ErrStatementClosed occurs when the statement is closed
	ErrStatementClosed = errors.New("pqxd: statement is closed")

	// ErrTxDualBoot occurs when running more than one transaction at a time in a single connection
	ErrTxDualBoot = errors.New("pqxd: cannot run more than transaction at a time in a single connection")
)
