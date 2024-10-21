package pqxd

import "errors"

var (
	// ErrNotSupported occurs when performed operation that is not supported in pqxd
	ErrNotSupported = errors.New("pqxd not supported this operation")

	// ErrInvalidPreparedStatement occurs when the prepared statement is invalid
	ErrInvalidPreparedStatement = errors.New("invalid prepared statement")

	// ErrStatementClosed occurs when the statement is closed
	ErrStatementClosed = errors.New("statement is closed")
)
