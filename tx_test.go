package mvcc

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBasic(t *testing.T) {
	tx := NewTx(NewDB())
	tx.Begin()
	assert.Nil(t, tx.Put(1, 1))
	tx.Commit()
}

func TestNoDirtyRead(t *testing.T) {
	db := NewDB()
	tx := NewTx(db)
	tx.Begin()
	assert.Nil(t, tx.Put(0, 1))
	tx.Commit()

	tx1 := NewTx(db)
	tx2 := NewTx(db)
	tx1.Begin()
	tx2.Begin()

	v1, found, err := tx1.Get(0)
	assert.True(t, found)
	assert.Nil(t, err)

	assert.Nil(t, tx2.Put(0, 2))

	v2, found, err := tx1.Get(0)
	assert.True(t, found)
	assert.Nil(t, err)

	assert.Equal(t, v1, v2)

	tx1.Commit()
	tx2.Commit()

	tx3 := NewTx(db)
	tx3.Begin()
	v3, found, err := tx3.Get(0)
	assert.True(t, found)
	assert.Nil(t, err)
	assert.Equal(t, 2, v3)
	tx3.Commit()
}

func TestRepeatableRead(t *testing.T) {
	db := NewDB()
	tx := NewTx(db)
	tx.Begin()
	assert.Nil(t, tx.Put(0, 1))
	tx.Commit()

	tx1 := NewTx(db)
	tx2 := NewTx(db)
	tx1.Begin()
	tx2.Begin()

	v1, found, err := tx1.Get(0)
	assert.True(t, found)
	assert.Nil(t, err)

	assert.Nil(t, tx2.Put(0, 2))
	tx2.Commit()

	v2, found, err := tx1.Get(0)
	assert.True(t, found)
	assert.Nil(t, err)

	assert.Equal(t, v1, v2)
	tx1.Commit()

	tx3 := NewTx(db)
	tx3.Begin()
	v3, found, err := tx3.Get(0)
	assert.True(t, found)
	assert.Nil(t, err)
	assert.Equal(t, 2, v3)
	tx3.Commit()
}

func TestWriteConflictShouldAbort(t *testing.T) {
	db := NewDB()
	tx1 := NewTx(db)
	tx2 := NewTx(db)
	tx1.Begin()
	tx2.Begin()

	assert.Nil(t, tx2.Put(0, 1))
	assert.Equal(t, ErrAborted, tx1.Put(0, 2))
	assert.Equal(t, StatusAborted, tx1.Status())

	tx2.Commit()

	tx3 := NewTx(db)
	tx3.Begin()
	v3, found, err := tx3.Get(0)
	assert.True(t, found)
	assert.Nil(t, err)
	assert.Equal(t, 1, v3)
	tx3.Commit()
}

func TestWriteSkewIsAllowed(t *testing.T) {
	db := NewDB()
	tx := NewTx(db)
	tx.Begin()
	assert.Nil(t, tx.Put(1, 1))
	assert.Nil(t, tx.Put(2, 2))
	tx.Commit()

	tx1 := NewTx(db)
	tx2 := NewTx(db)
	tx1.Begin()
	tx2.Begin()

	v1, _, _ := tx1.Get(1)
	v2, _, _ := tx2.Get(2)

	assert.Nil(t, tx1.Put(2, v1))
	assert.Nil(t, tx2.Put(1, v2))

	tx1.Commit()
	tx2.Commit()

	tx = NewTx(db)
	tx.Begin()
	v3, _, err := tx.Get(1)
	assert.Nil(t, err)
	assert.Equal(t, 2, v3)

	v4, _, err := tx.Get(2)
	assert.Nil(t, err)
	assert.Equal(t, 1, v4)
	tx.Commit()
}
