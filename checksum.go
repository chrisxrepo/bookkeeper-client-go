package bookkeeper

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/chrisxrepo/bookkeeper-client-go/pb"
)

const (
	_METADATA_LENGTH     = 32
	_LAC_METADATA_LENGTH = 16
)

type Checksum interface {
	// package sending data
	PackageForSending(entryID, lastAddConfirmed, length int64, data []byte) ([]byte, error)

	getChecksumLength() int

	writeChecksum(buffer *bytes.Buffer, bss ...[]byte)
}

func NewChecksum(ledgerID int64, password []byte, digestType pb.LedgerMetadataFormat_DigestType) (Checksum, error) {
	dfMgr := &defaultChecksum{
		ledgerID: ledgerID,
		password: password,
	}

	var mgr Checksum
	switch digestType {
	case pb.LedgerMetadataFormat_CRC32:
		mgr = &CRC32Checksum{defaultChecksum: dfMgr}

	case pb.LedgerMetadataFormat_HMAC:
		sha := sha1.New()
		sha.Write([]byte("mac"))
		sha.Write(password)
		mgr = &HMACChecksum{defaultChecksum: dfMgr, macKey: sha.Sum(nil)}

	case pb.LedgerMetadataFormat_CRC32C:
		mgr = &CRC32CChecksum{defaultChecksum: dfMgr}

	case pb.LedgerMetadataFormat_DUMMY:
		mgr = &DummyChecksum{defaultChecksum: dfMgr}

	default:
		return nil, fmt.Errorf("Unknown checksum type:%v", digestType)
	}

	dfMgr.mgr = mgr
	return mgr, nil
}

type defaultChecksum struct {
	mgr      Checksum
	ledgerID int64
	password []byte
}

func (d *defaultChecksum) PackageForSending(entryID, lastAddConfirmed, length int64, data []byte) ([]byte, error) {
	var buffer = bytes.NewBuffer(make([]byte, 0, _METADATA_LENGTH+d.mgr.getChecksumLength()+len(data)))
	binary.Write(buffer, binary.BigEndian, d.ledgerID)
	binary.Write(buffer, binary.BigEndian, entryID)
	binary.Write(buffer, binary.BigEndian, lastAddConfirmed)
	binary.Write(buffer, binary.BigEndian, length)

	d.mgr.writeChecksum(buffer, buffer.Bytes(), data)
	buffer.Write(data)

	return buffer.Bytes(), nil
}

// DummyChecksum digest manager for dummy
type DummyChecksum struct {
	*defaultChecksum
}

func (d *DummyChecksum) getChecksumLength() int { return 0 }

func (d *DummyChecksum) writeChecksum(buffer *bytes.Buffer, bss ...[]byte) {}

// CRC32Checksum digest manager for crc32
type CRC32Checksum struct {
	*defaultChecksum
}

func (d *CRC32Checksum) getChecksumLength() int { return 8 }

func (d *CRC32Checksum) writeChecksum(buffer *bytes.Buffer, bss ...[]byte) {
	crc := crc32.New(crc32.MakeTable(crc32.IEEE))
	for _, bs := range bss {
		crc.Write(bs)
	}

	binary.Write(buffer, binary.BigEndian, uint64(crc.Sum32()))
}

// CRC32CChecksum digest manager for crc32c
type CRC32CChecksum struct {
	*defaultChecksum
}

func (d *CRC32CChecksum) getChecksumLength() int { return 4 }

func (d *CRC32CChecksum) writeChecksum(buffer *bytes.Buffer, bss ...[]byte) {
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	for _, bs := range bss {
		crc.Write(bs)
	}
	binary.Write(buffer, binary.BigEndian, uint32(crc.Sum32()))
}

// HMacChecksum digest manager for hmac-1
type HMACChecksum struct {
	*defaultChecksum
	macKey []byte
}

func (d *HMACChecksum) getChecksumLength() int { return 20 }

func (d *HMACChecksum) writeChecksum(buffer *bytes.Buffer, bss ...[]byte) {
	h := hmac.New(sha1.New, d.macKey)
	for _, bs := range bss {
		h.Write(bs)
	}
	buffer.Write(h.Sum(nil))
}
