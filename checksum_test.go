package bookkeeper

import (
	"bytes"
	"testing"

	"github.com/chrisxrepo/bookkeeper-client-go/pb"
	"github.com/stretchr/testify/assert"
)

func TestChecksum_CRC32(t *testing.T) {
	c, err := NewChecksum(0, []byte(""), pb.LedgerMetadataFormat_CRC32)
	assert.NoError(t, err)

	buffer := bytes.NewBuffer(make([]byte, 0, 64))
	c.writeChecksum(buffer, []byte("abce"), []byte("edfh"))
	assert.Equal(t, buffer.Len(), c.getChecksumLength())
}

func TestChecksum_CRC32C(t *testing.T) {
	c, err := NewChecksum(0, []byte(""), pb.LedgerMetadataFormat_CRC32C)
	assert.NoError(t, err)

	buffer := bytes.NewBuffer(make([]byte, 0, 64))
	c.writeChecksum(buffer, []byte("abce"), []byte("edfh"))
	assert.Equal(t, buffer.Len(), c.getChecksumLength())
}

func TestChecksum_HMAC(t *testing.T) {
	c, err := NewChecksum(0, []byte(""), pb.LedgerMetadataFormat_HMAC)
	assert.NoError(t, err)

	buffer := bytes.NewBuffer(make([]byte, 0, 64))
	c.writeChecksum(buffer, []byte("abce"), []byte("edfh"))
	assert.Equal(t, buffer.Len(), c.getChecksumLength())
}
