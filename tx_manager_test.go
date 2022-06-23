package mvcc

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestNextTxid(t *testing.T) {
	tm := newTxManager()

	var wg sync.WaitGroup
	for i := 0; i < 9999; i++ {
		wg.Add(1)
		go func() {
			tm.nextTxid()
			wg.Done()
		}()
	}
	wg.Wait()

	assert.Equal(t, txid(10000), tm.nextTxid())
}

func TestVisibility(t *testing.T) {
	tm := newTxManager()
	tm.setStatus(tm.nextTxid(), StatusActive)    // 1
	tm.setStatus(tm.nextTxid(), StatusAborted)   // 2
	tm.setStatus(tm.nextTxid(), StatusCommitted) // 3
	tm.setStatus(tm.nextTxid(), StatusCommitted) // 4
	tm.setStatus(tm.nextTxid(), StatusActive)    // 5
	tm.setStatus(tm.nextTxid(), StatusActive)    // 6
	tm.setStatus(tm.nextTxid(), StatusCommitted) // 7
	tm.setStatus(tm.nextTxid(), StatusActive)    // 8

	assert.Equal(t, StatusActive, tm.getStatus(txid(1)))
	assert.Equal(t, StatusActive, tm.getStatus(txid(5)))
	assert.Equal(t, StatusAborted, tm.getStatus(txid(2)))
	assert.Equal(t, StatusCommitted, tm.getStatus(txid(7)))

	snapshot := tm.snapshot(txid(6))
	assert.True(t, snapshot.visible(txid(2)))
	assert.True(t, snapshot.visible(txid(3)))
	assert.True(t, snapshot.visible(txid(4)))
	assert.True(t, snapshot.visible(txid(6)))

	assert.False(t, snapshot.visible(txid(1)))
	assert.False(t, snapshot.visible(txid(5)))
	assert.False(t, snapshot.visible(txid(7)))
	assert.False(t, snapshot.visible(txid(8)))
}
