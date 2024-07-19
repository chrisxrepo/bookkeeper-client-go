package bookkeeper

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chrisxrepo/bookkeeper-client-go/pb"
	"google.golang.org/protobuf/proto"
)

var (
	_              Client = &emptyClient{}
	_              Client = &bookieClient{}
	txnIdGenerator        = atomic.Uint64{}
)

type Client interface {
	Remote() string
	AddEntry(ledgerID, entryID int64, mastKey []byte, payload []byte) error
}

type emptyClient struct{}

func (c emptyClient) Remote() string {
	return ""
}

func (c emptyClient) AddEntry(ledgerID, entryID int64, mastKey []byte, payload []byte) error {
	return nil
}

type ClientPool struct {
	cfg        *Config
	clientNew  func(*Config, string) (Client, error)
	clientMap  sync.Map //map[string][]Client
	clientLock sync.Mutex
}

func NewClientPool(cfg *Config) *ClientPool {
	return &ClientPool{
		cfg:       cfg,
		clientNew: newClient,
	}
}

func (p *ClientPool) GetClient(addr string, ledgerID int64) (Client, error) {
	value, ok := p.clientMap.Load(addr)
	if !ok {
		p.clientLock.Lock()
		if value, ok = p.clientMap.Load(addr); !ok {
			clients := make([]Client, p.cfg.ClientNumPreBookie)
			for i := 0; i < p.cfg.ClientNumPreBookie; i++ {
				client, err := p.clientNew(p.cfg, addr)
				if err != nil {
					return nil, err
				}
				clients[i] = client
			}
			p.clientMap.Store(addr, clients)
			value = clients
		}
		p.clientLock.Unlock()
	}

	clients := value.([]Client)
	return clients[rand.Intn(p.cfg.ClientNumPreBookie)], nil
}

type bookieClient struct {
	cfg  *Config
	addr string
	conn net.Conn
	in   *bufio.Reader
	out  *bufio.Writer
}

func newClient(cfg *Config, addr string) (Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	c := &bookieClient{
		cfg:  cfg,
		addr: addr,
		conn: conn,
		in:   bufio.NewReaderSize(conn, 4096),
		out:  bufio.NewWriterSize(conn, 4096),
	}

	go c.connRead()
	return c, nil
}

func (c *bookieClient) Remote() string {
	return c.addr
}

func (c *bookieClient) AddEntry(ledgerID, entryID int64, mastKey []byte, payload []byte) error {
	var (
		version   = pb.ProtocolVersion_VERSION_THREE
		operation = pb.OperationType_ADD_ENTRY
		txnID     = txnIdGenerator.Add(1)
	)

	req := &pb.Request{
		Header: &pb.BKPacketHeader{
			Version:   &version,
			Operation: &operation,
			TxnId:     &txnID,
		},
		AddRequest: &pb.AddRequest{
			LedgerId:  &ledgerID,
			EntryId:   &entryID,
			MasterKey: mastKey,
			Body:      payload,
		},
	}

	buffer := make([]byte, 4, proto.Size(req)+4)
	out, err := proto.MarshalOptions{}.MarshalAppend(buffer, req)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint32(out[:4], uint32(len(out)-4))

	if _, err = c.out.Write(out); err != nil {
		return err
	}
	return c.out.Flush()
}

func (c *bookieClient) connRead() {
	for {
		lengthBuf, err := c.in.Peek(4)
		if err != nil {
			fmt.Println("peek error:", err)
			return
		}

		length := binary.BigEndian.Uint32(lengthBuf)
		fmt.Println("-----------length:", length)

		if c.in.Buffered() < int(length)+4 {
			time.Sleep(time.Millisecond) //wait incomming data
			continue
		}

		buffer := make([]byte, length+4)
		if _, err := io.ReadFull(c.in, buffer); err != nil {
			fmt.Println("read error:", err)
			return
		}

		var resp pb.Response
		if err := proto.Unmarshal(buffer[4:], &resp); err != nil {
			fmt.Println("proto unmarshal error:", err)
			return
		}

		out, err := json.Marshal(resp)
		fmt.Println(string(out), err)

	}
}
