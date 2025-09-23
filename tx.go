package pqxd

import (
	"context"
	"database/sql/driver"
	"log/slog"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// compatibility check
var _ driver.Tx = (*connection)(nil)

// Commit See: driver.Tx
func (c *connection) Commit() error {
	if c.closed.Load() {
		return driver.ErrBadConn
	}
	if !c.txOngoing.Load() {
		slog.Warn("pqxd: commit was performed, but transaction is not ongoing")
		return nil
	}
	c.txCommit.Load().function()
	return nil
}

func (c *connection) Rollback() error {
	if c.closed.Load() {
		return driver.ErrBadConn
	}
	if !c.txOngoing.Load() {
		slog.Warn("pqxd: rollback was performed, but transaction is not ongoing")
		return nil
	}
	c.txRollback.Load().function()
	return nil
}

// transactionInOut is the input/output of a transaction.
type transactionInOut struct {
	input  types.ParameterizedStatement
	output map[string]types.AttributeValue
	err    error
}

// transationStatementPublisher publishes statements in a transaction.
type transactionStatementPublisher struct {
	ch        chan *transactionInOut
	closeOnce sync.Once
}

// publish publishes a statement.
func (p *transactionStatementPublisher) publish(inout *transactionInOut) {
	p.ch <- inout
}

// close closes the channel.
func (p *transactionStatementPublisher) close() {
	p.closeOnce.Do(
		func() {
			close(p.ch)
		},
	)
}

// txCommit represents a commit operation in a transaction.
type txCommit struct {
	ctx           context.Context
	function      context.CancelFunc
	receiveResult context.Context
}

// txRollback represents a rollback operation in a transaction.
type txRollback struct {
	ctx      context.Context
	function context.CancelFunc
}
