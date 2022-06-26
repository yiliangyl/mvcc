package mvcc

import (
	"sync"
	"sync/atomic"
)

type txManager struct {
	txid uint64

	mu        sync.RWMutex
	status    map[txid]TxStatus
	activeSet map[txid]struct{}
}

func newTxManager() *txManager {
	return &txManager{
		status:    make(map[txid]TxStatus),
		activeSet: make(map[txid]struct{}),
	}
}

func (tm *txManager) nextTxid() txid {
	return txid(atomic.AddUint64(&tm.txid, 1))
}

func (tm *txManager) setStatus(id txid, status TxStatus) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.status[id] = status
	if status == StatusActive {
		tm.activeSet[id] = struct{}{}
	} else {
		delete(tm.activeSet, id)
	}
}

func (tm *txManager) getStatus(id txid) TxStatus {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return tm.status[id]
}

func (tm *txManager) active(id txid) bool {
	return tm.getStatus(id) == StatusActive
}

func (tm *txManager) committed(id txid) bool {
	return tm.getStatus(id) == StatusCommitted
}

func (tm *txManager) snapshot(id txid) *snapshot {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	set := make(map[txid]struct{})
	clone(tm.activeSet, &set)

	return &snapshot{
		id:        id,
		activeSet: set,
	}
}

type snapshot struct {
	id        txid
	activeSet map[txid]struct{}
}

func (s *snapshot) visible(id txid) bool {
	_, ok := s.activeSet[id]
	return id == s.id || (id < s.id && !ok)
}
