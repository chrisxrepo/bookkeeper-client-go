package bookkeeper

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/chrisxrepo/bookkeeper-client-go/pb"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
)

const (
	_MAX_VERSION_DIGITS = 10
)

var (
	_VERSION_KEY_BYTES   = []byte("BookieMetadataFormatVersion\t")
	_LINE_SPLITTER_BYTES = []byte("\n")
)

type Metadata struct {
	ledgerID        int64
	lastEntryID     int64
	ensembleSize    int32
	writeQuorumSize int32
	ackQuorumSize   int32
	length          int64
	state           pb.LedgerMetadataFormat_State
	digestType      pb.LedgerMetadataFormat_DigestType
	password        []byte
	cToken          int64
	ctime           int64
	ensembles       map[int64][]string
	customMetadata  map[string][]byte
}

func (m *Metadata) Serialize() ([]byte, error) {
	builder := &pb.LedgerMetadataFormat{
		QuorumSize:     &m.writeQuorumSize,
		EnsembleSize:   &m.ensembleSize,
		AckQuorumSize:  &m.ackQuorumSize,
		LastEntryId:    &m.lastEntryID,
		Length:         &m.length,
		State:          &m.state,
		DigestType:     &m.digestType,
		Password:       m.password,
		Ctime:          &m.ctime,
		CToken:         &m.cToken,
		CustomMetadata: make([]*pb.LedgerMetadataFormatCMetadataMapEntry, 0, len(m.customMetadata)),
		Segment:        make([]*pb.LedgerMetadataFormat_Segment, 0, len(m.ensembles)),
	}

	for key, value := range m.customMetadata {
		builder.CustomMetadata = append(builder.CustomMetadata, &pb.LedgerMetadataFormatCMetadataMapEntry{
			Key:   &key,
			Value: value,
		})
	}

	for entry, number := range m.ensembles {
		builder.Segment = append(builder.Segment, &pb.LedgerMetadataFormat_Segment{
			FirstEntryId: &entry, EnsembleMember: number,
		})
	}

	os := bytes.NewBuffer(make([]byte, 0, proto.Size(builder)+40))
	writeHeader(os, int(pb.ProtocolVersion_VERSION_THREE))

	if _, err := protodelim.MarshalTo(os, builder); err != nil {
		return nil, err
	}
	return os.Bytes(), nil

}

func (m *Metadata) Parse(is *bytes.Buffer) error {
	if _, err := readHeader(is); err != nil {
		return err
	}

	var builder pb.LedgerMetadataFormat
	if err := protodelim.UnmarshalFrom(is, &builder); err != nil {
		return err
	}

	if builder.LastEntryId != nil {
		m.lastEntryID = *builder.LastEntryId
	}
	if builder.LastEntryId != nil {
		m.ensembleSize = *builder.EnsembleSize
	}
	if builder.QuorumSize != nil {
		m.writeQuorumSize = *builder.QuorumSize
	}
	if builder.AckQuorumSize != nil {
		m.ackQuorumSize = *builder.AckQuorumSize
	}
	if builder.Length != nil {
		m.length = *builder.Length
	}
	if builder.State != nil {
		m.state = *builder.State
	}
	if builder.DigestType != nil {
		m.digestType = *builder.DigestType
	}
	if builder.CToken != nil {
		m.cToken = *builder.CToken
	}
	if builder.Ctime != nil {
		m.ctime = *builder.Ctime
	}
	m.password = builder.Password

	m.ensembles = make(map[int64][]string)
	for _, segement := range builder.Segment {
		m.ensembles[*segement.FirstEntryId] = segement.EnsembleMember
	}

	m.customMetadata = make(map[string][]byte)
	for _, mt := range builder.CustomMetadata {
		m.customMetadata[*mt.Key] = mt.Value
	}
	return nil
}

func readHeader(os *bytes.Buffer) (int, error) {
	bs := os.Next(len(_VERSION_KEY_BYTES))
	if !BytesEqual(bs, _VERSION_KEY_BYTES) {
		return 0, errors.New("Invalid ledger metadata header")
	}

	var vsStr = make([]byte, 0, _MAX_VERSION_DIGITS)
	for i := 0; i < _MAX_VERSION_DIGITS; i++ {
		if c, err := os.ReadByte(); err != nil {
			return 0, err
		} else if c == _LINE_SPLITTER_BYTES[0] {
			break
		} else {
			vsStr = append(vsStr, c)
		}
	}

	if version, _ := strconv.ParseInt(string(vsStr), 10, 0); pb.ProtocolVersion(version) != pb.ProtocolVersion_VERSION_THREE {
		return 0, fmt.Errorf("Not support version %s", string(vsStr))
	}
	return int(pb.ProtocolVersion_VERSION_THREE), nil
}

func writeHeader(os *bytes.Buffer, version int) []byte {
	os.Write(_VERSION_KEY_BYTES)
	os.Write([]byte(strconv.FormatInt(int64(version), 10)))
	os.Write(_LINE_SPLITTER_BYTES)
	return os.Bytes()
}
