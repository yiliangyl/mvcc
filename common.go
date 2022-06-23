package mvcc

import (
	"errors"
	"math"
)

var (
	ErrAborted = errors.New("transaction has been already aborted")
)

type txid uint64

var (
	inf txid = math.MaxUint64
)

type TxStatus uint8

const (
	StatusBegin TxStatus = iota
	StatusActive
	StatusAborted
	StatusCommitted
)
