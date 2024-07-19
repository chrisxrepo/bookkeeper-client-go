package bookkeeper

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path"

	"github.com/chrisxrepo/bookkeeper-client-go/pb"
)

type BookKeeper struct {
	cfg        *Config
	zk         *Zookeeper
	clientPool *ClientPool
}

func NewBookeeper(cfg *Config) (*BookKeeper, error) {
	if err := cfg.ValidConfig(); err != nil {
		return nil, err
	}

	zk, err := NewZookeeper(cfg)
	if err != nil {
		return nil, err
	}

	return &BookKeeper{cfg: cfg, zk: zk, clientPool: NewClientPool(cfg)}, nil
}

func (b *BookKeeper) CreateLeadger(ensSize, writeQuorumSize, ackQuorumSize int, password []byte, digestType pb.LedgerMetadataFormat_DigestType) (Ledger, error) {
	if ensSize > len(b.zk.Bookies()) {
		return nil, errors.New("Not enough non-faulty bookies available")
	}

	ensemble, err := b.newEnsemble(ensSize, writeQuorumSize, ackQuorumSize)
	if err != nil {
		return nil, err
	}

	ledgerID, err := b.genLedgerID()
	if err != nil {
		return nil, err
	}

	metadata := &Metadata{
		ledgerID:        ledgerID,
		ensembleSize:    int32(ensSize),
		writeQuorumSize: int32(writeQuorumSize),
		ackQuorumSize:   int32(ackQuorumSize),
		state:           pb.LedgerMetadataFormat_OPEN,
		digestType:      digestType,
		password:        password,
		cToken:          rand.Int63(),
		ensembles:       map[int64][]string{0: ensemble},
	}
	data, err := metadata.Serialize()
	if err != nil {
		return nil, err
	}

	if err := b.zk.SetData(getLedgerPath(ledgerID), data); err != nil {
		return nil, err
	}

	return newNormalLedger(b, metadata)
}

func (b *BookKeeper) newEnsemble(ensSize, writeQuorumSize, ackQuorumSize int) ([]string, error) {
	bks := b.zk.Bookies()
	if ensSize > len(bks) {
		return nil, errors.New("Not enough bookie node")
	}

	return bks[0:ensSize], nil
}

func (b *BookKeeper) genLedgerID() (int64, error) {
	return b.zk.LedgerID()
}

func getLedgerPath(ledger int64) string {
	if ledger < math.MaxInt32 {
		ledgerStr := fmt.Sprintf("%010d", ledger)
		return path.Join(ledgerStr[:2], ledgerStr[2:6], "L"+ledgerStr[6:10])
	}

	ledgerStr := fmt.Sprintf("%019d", ledger)
	return path.Join(ledgerStr[:3], ledgerStr[3:7], ledgerStr[7:11], ledgerStr[11:15], "L"+ledgerStr[15:19])
}
