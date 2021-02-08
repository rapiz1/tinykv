package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	it  engine_util.DBIterator
	txn *MvccTxn
	key []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	it := txn.Reader.IterCF(engine_util.CfWrite)
	return &Scanner{
		it:  it,
		txn: txn,
		key: startKey,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.it.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan == nil || !scan.it.Valid() {
		return nil, nil, nil
	}

	for scan.it.Valid() {
		scan.it.Seek(EncodeKey(scan.key, scan.txn.StartTS))
		if !scan.it.Valid() {
			break
		}

		wb, err := scan.it.Item().Value()
		if err != nil {
			panic(err)
		}
		w, err := ParseWrite(wb)
		if err != nil {
			panic(err)
		}

		if w.Kind != WriteKindDelete {
			userKey := DecodeUserKey(scan.it.Item().Key())
			if bytes.Equal(userKey, scan.key) {
				key := scan.key
				val, err := scan.txn.GetValue(key)

				// Advance the iterator
				scan.it.Seek(EncodeKey(scan.key, 0))
				if scan.it.Valid() {
					scan.key = DecodeUserKey(scan.it.Item().Key())
				}

				return key, val, err
			}
			scan.key = userKey
		} else {
			// Advance the iterator
			scan.it.Seek(EncodeKey(scan.key, 0))
			if scan.it.Valid() {
				scan.key = DecodeUserKey(scan.it.Item().Key())
			}
		}
	}
	return nil, nil, nil
}
