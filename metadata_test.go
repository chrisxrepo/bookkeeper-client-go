package bookkeeper

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/chrisxrepo/bookkeeper-client-go/pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
)

func TestMetadata(t *testing.T) {
	mt := &Metadata{
		ledgerID:        100,
		ensembleSize:    3,
		writeQuorumSize: 2,
		ackQuorumSize:   2,
		state:           pb.LedgerMetadataFormat_OPEN,
		digestType:      pb.LedgerMetadataFormat_CRC32,
		password:        []byte(""),
		cToken:          rand.Int63(),
		ensembles:       map[int64][]string{0: {"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}},
	}

	bs, err := mt.Serialize()
	assert.NoError(t, err)

	newMt := &Metadata{}
	err = newMt.Parse(bytes.NewBuffer(bs))
	assert.NoError(t, err)

	assert.Equal(t, mt.ensembleSize, newMt.ensembleSize)
	assert.Equal(t, mt.writeQuorumSize, newMt.writeQuorumSize)
	assert.Equal(t, mt.ackQuorumSize, newMt.ackQuorumSize)
	assert.Equal(t, mt.cToken, newMt.cToken)
}

func TestParseMetadata(t *testing.T) {
	zk, err := NewZookeeper(&Config{
		BKURI:     "zk://10.150.13.39:2181/bookkeeper/ledgers",
		ZKTimeout: time.Second * 5,
	})
	assert.NoError(t, err)

	bs, err := zk.GetData(getLedgerPath(20))
	assert.NoError(t, err)

	mt := &Metadata{ledgerID: 10}
	err = mt.Parse(bytes.NewBuffer(bs))
	assert.NoError(t, err)
	fmt.Println(mt)
}

func TestProtoMarshal(t *testing.T) {
	var num int32 = 99999999
	var num64 int64 = 10
	var state = pb.LedgerMetadataFormat_OPEN
	builder := &pb.LedgerMetadataFormat{
		QuorumSize:    &num,
		EnsembleSize:  &num,
		AckQuorumSize: &num,
		Length:        &num64,
		State:         &state,
	}

	var size = proto.Size(builder)
	var bs = bytes.NewBuffer(make([]byte, 0, size+10))
	n, err := protodelim.MarshalTo(bs, builder)
	fmt.Println("size:", size, n, bs.Len())
	assert.NoError(t, err)
	assert.Equal(t, bs.Len(), n)

	var ledger pb.LedgerMetadataFormat
	err = proto.Unmarshal(bs.Bytes(), &ledger)
	assert.NoError(t, err)
}
