package bookkeeper

import (
	"math"
	"testing"
	"time"

	"github.com/chrisxrepo/bookkeeper-client-go/pb"
	"github.com/stretchr/testify/assert"
)

func TestGetLedgerPath(t *testing.T) {
	ledgerPath := getLedgerPath(12)
	assert.Equal(t, ledgerPath, "00/0000/L0012")

	ledgerPath = getLedgerPath(12 + math.MaxInt32)
	assert.Equal(t, ledgerPath, "000/0000/0021/4748/L3659")
}

func TestCreateLedger(t *testing.T) {
	bk, err := NewBookeeper(&Config{
		BKURI:     "zk://10.150.13.39:2181/bookkeeper/ledgers",
		ZKTimeout: time.Second * 5,
	})
	assert.NoError(t, err)

	_, err = bk.CreateLeadger(3, 2, 2, []byte(""), pb.LedgerMetadataFormat_DUMMY)
	assert.NoError(t, err)
}
