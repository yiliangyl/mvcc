package mvcc

import "sync"

type db struct {
	mu    sync.RWMutex
	tm    *txManager
	table map[int]*version
}

func NewDB() *db {
	return &db{
		tm:    newTxManager(),
		table: make(map[int]*version),
	}
}

func (db *db) write(tx *Tx, key, value int) error {
	db.mu.Lock()
	v, ok := db.table[key]
	if !ok {
		v = &version{}
		db.table[key] = v
	}
	db.mu.Unlock()

	record := &record{
		value: value,
		tmin:  tx.id,
		tmax:  inf,
	}
	return v.write(record, tx.db.tm)
}

func (db *db) read(tx *Tx, key int) (int, bool, error) {
	db.mu.RLock()
	v, ok := db.table[key]
	if !ok {
		db.mu.RUnlock()
		return 0, false, nil
	}
	db.mu.RUnlock()

	return v.read(tx.snapshot)
}

type record struct {
	value int
	tmin  txid
	tmax  txid
}

type version struct {
	mu      sync.RWMutex
	records []*record
}

func (v *version) write(record *record, tm *txManager) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if len(v.records) > 0 {
		back := v.records[len(v.records)-1]
		if back.tmin >= record.tmin {
			return ErrAborted
		} else if tm.getStatus(back.tmin) == StatusCommitted {
			back.tmax = record.tmin
		} else {
			// the last tx is still active
			// solution 1: stall until commit
			// solution 2: abort
			// TODO: use the better way to wait for committing
			for tm.getStatus(back.tmin) == StatusActive {
			}
			back.tmax = record.tmin
		}
	}

	v.records = append(v.records, record)
	return nil
}

func (v *version) read(snapshot *snapshot) (int, bool, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	for i := len(v.records) - 1; i >= 0; i-- {
		if snapshot.visible(v.records[i].tmin) {
			return v.records[i].value, true, nil
		}
	}
	return 0, false, nil
}
