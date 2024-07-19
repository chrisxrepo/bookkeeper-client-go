package bookkeeper

import (
	"fmt"
	"testing"
	"time"

	"github.com/chrisxrepo/bookkeeper-client-go/pb"
	"github.com/stretchr/testify/assert"
)

func TestLedger_AddEntry(t *testing.T) {
	bk, err := NewBookeeper(&Config{
		BKURI:     "zk://10.150.13.39:2181/bookkeeper/ledgers",
		ZKTimeout: time.Second * 5,
	})
	assert.NoError(t, err)

	ledger, err := bk.CreateLeadger(3, 2, 2, []byte(""), pb.LedgerMetadataFormat_DUMMY)
	assert.NoError(t, err)

	fmt.Println("-------ledgerID:", ledger.GetLedgerID())
	err = ledger.AddEntry([]byte("hello bookkeeper"))
	assert.NoError(t, err)

	time.Sleep(time.Second * 5)
}
