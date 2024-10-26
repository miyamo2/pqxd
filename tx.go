package pqxd

import (
	"database/sql/driver"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log/slog"
	"sync"
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
	c.txCommiter.Load().commit()
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
	c.txRollbacker.Load().rollback()
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
	ch chan *transactionInOut
}

// publish publishes a statement.
func (p *transactionStatementPublisher) publish(inout *transactionInOut) {
	p.ch <- inout
}

// close closes the channel.
func (p *transactionStatementPublisher) close() {
	close(p.ch)
}

// transactionCommitter commits a transaction.
type transactionCommitter struct {
	ch         chan<- struct{}
	done       <-chan struct{}
	commitOnce sync.Once
	closeOnce  sync.Once
}

// commit commits the transaction.
func (c *transactionCommitter) commit() {
	c.commitOnce.Do(func() {
		c.ch <- struct{}{}
		<-c.done
	})
}

// close closes the channel.
func (c *transactionCommitter) close() {
	close(c.ch)
}

// transactionRollbacker rolls back a transaction.
type transactionRollbacker struct {
	ch           chan<- struct{}
	done         <-chan struct{}
	rollbackOnce sync.Once
	closeOnce    sync.Once
}

// rollback rolls back the transaction.
func (r *transactionRollbacker) rollback() {
	r.rollbackOnce.Do(func() {
		r.ch <- struct{}{}
		select {
		case _, ok := <-r.done:
			if ok {
				return
			}
		}
	})
}

// close closes the channel.
func (r *transactionRollbacker) close() {
	r.closeOnce.Do(func() {
		close(r.ch)
	})
}
