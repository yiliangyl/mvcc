package mvcc

import "log"

type Tx struct {
	id       txid
	status   TxStatus
	snapshot *snapshot
	db       *db
}

func NewTx(db *db) *Tx {
	return &Tx{
		status: StatusBegin,
		db:     db,
	}
}

func (tx *Tx) Begin() {
	tx.id = tx.db.tm.nextTxid()
	tx.status = StatusActive
	tx.snapshot = tx.db.tm.snapshot(tx.id)

	tx.db.tm.setStatus(tx.id, tx.status)
	log.Printf("tx (%d): begin", tx.id)
}

func (tx *Tx) Commit() {
	_assert(tx.db.tm.getStatus(tx.id) == StatusActive, "current tx is not active")

	tx.status = StatusCommitted
	tx.db.tm.setStatus(tx.id, tx.status)
	log.Printf("tx (%d): commit", tx.id)
}

func (tx *Tx) Abort() {
	_assert(tx.db.tm.getStatus(tx.id) == StatusActive, "current tx is not active")

	tx.status = StatusAborted
	tx.db.tm.setStatus(tx.id, tx.status)
	log.Printf("tx (%d): abort", tx.id)
}

func (tx *Tx) Put(key, value int) error {
	_assert(tx.db.tm.getStatus(tx.id) == StatusActive, "current tx is not active")

	err := tx.db.write(tx, key, value)
	if err == ErrAborted {
		tx.Abort()
	}
	return err
}

func (tx *Tx) Get(key int) (int, bool, error) {
	_assert(tx.db.tm.getStatus(tx.id) == StatusActive, "current tx is not active")

	value, found, err := tx.db.read(tx, key)
	if err == ErrAborted {
		tx.Abort()
	}
	return value, found, err
}

func (tx *Tx) Status() TxStatus {
	return tx.db.tm.getStatus(tx.id)
}
