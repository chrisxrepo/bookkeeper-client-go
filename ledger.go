package bookkeeper

import (
	"crypto/sha1"
	"sync"
	"sync/atomic"
)

type Ledger interface {
	// GetLedgerID return ledger id
	GetLedgerID() int64

	// AddEntry add entry to ledger
	AddEntry([]byte) error
}

type normalLedger struct {
	bookkeeper       *BookKeeper
	metadata         *Metadata
	checksum         Checksum
	ledgerKey        []byte
	lastAddPushed    atomic.Int64
	lastAddConfirmed atomic.Int64
	length           atomic.Int64
	entryLock        sync.Mutex
}

func newNormalLedger(bookkeeper *BookKeeper, metadata *Metadata) (Ledger, error) {
	checksum, err := NewChecksum(metadata.ledgerID, metadata.password, metadata.digestType)
	if err != nil {
		return nil, err
	}

	h := sha1.New()
	if _, err := h.Write([]byte("ledger")); err != nil {
		return nil, err
	}
	if _, err := h.Write(metadata.password); err != nil {
		return nil, err
	}

	l := &normalLedger{
		bookkeeper: bookkeeper,
		metadata:   metadata,
		checksum:   checksum,
		ledgerKey:  h.Sum(nil),
	}
	l.lastAddPushed.Store(-1)
	l.lastAddConfirmed.Store(-1)

	return l, nil
}

func (l *normalLedger) GetLedgerID() int64 {
	return l.metadata.ledgerID
}

func (l *normalLedger) AddEntry(data []byte) error {
	l.entryLock.Lock()
	var entryID = l.lastAddPushed.Add(1)
	var length = l.length.Add(int64(len(data)))
	l.entryLock.Unlock()

	toSend, err := l.checksum.PackageForSending(entryID, l.lastAddConfirmed.Load(), length, data)
	if err != nil {
		return err
	}

	bookies := l.metadata.ensembles[0]
	for _, bookie := range bookies {
		client, err := l.bookkeeper.clientPool.GetClient(bookie, l.metadata.ledgerID)
		if err != nil {
			return err
		}

		if err := client.AddEntry(l.GetLedgerID(), entryID, l.ledgerKey, toSend); err != nil {
			return err
		}
	}
	return nil
}
