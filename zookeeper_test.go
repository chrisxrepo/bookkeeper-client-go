package bookkeeper

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestZKBookies(t *testing.T) {
	zk, err := NewZookeeper(&Config{
		BKURI:     "zk://10.150.13.39:2181/bookkeeper/ledgers",
		ZKTimeout: time.Second * 5,
	})
	assert.NoError(t, err)

	bks := zk.Bookies()
	fmt.Println("bookies:", bks)
	fmt.Println("idgen:", zk.idgen)
}

func TestZkLedger(t *testing.T) {
	zk, err := NewZookeeper(&Config{
		BKURI:     "zk://10.150.13.39:2181/bookkeeper/ledgers",
		ZKTimeout: time.Second * 5,
	})
	assert.NoError(t, err)

	ledger, err := zk.LedgerID()
	assert.NoError(t, err)
	fmt.Println(ledger)
}
