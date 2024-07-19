package bookkeeper

import (
	"time"
)

const (
	VERSION_THREE = 3
)

type Config struct {
	// zookeeper path for bookeeper, zk://127.0.0.1:2181/ledgers
	BKURI string

	// zookeeper session timeout
	ZKTimeout time.Duration

	// number client per
	ClientNumPreBookie int
}

func (c Config) ValidConfig() error {
	return nil
}
