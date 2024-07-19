package bookkeeper

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/go-zookeeper/zk"
)

func NewZookeeper(cfg *Config) (*Zookeeper, error) {
	addrs, basePath, err := parseZkUri(cfg.BKURI)
	if err != nil {
		return nil, err
	}

	conn, evCh, err := zk.Connect(addrs, cfg.ZKTimeout)
	if err != nil {
		return nil, err
	}

	zk := &Zookeeper{
		zkConn:   conn,
		bathPath: basePath,
		idgen:    path.Join(basePath, "idgen", "ID-"),
	}

	bks, _, bkEvent, err := conn.ChildrenW(path.Join(basePath, "/available"))
	if err != nil {
		return nil, err
	}
	zk.setBookies(bks)

	go zk.zkEventWatch(evCh, bkEvent)
	return zk, nil
}

type Zookeeper struct {
	zkConn   *zk.Conn
	bathPath string
	bookies  atomic.Value //[]string
	idgen    string
}

func (z *Zookeeper) Bookies() []string {
	if v := z.bookies.Load(); v != nil {
		return v.([]string)
	}
	return []string{}
}

func (z *Zookeeper) LedgerID() (int64, error) {
	idStr, err := z.zkConn.Create(z.idgen, []byte{}, 3, zk.WorldACL(zk.PermAll))
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(strings.TrimPrefix(idStr, z.idgen), 10, 0)
}

func (z *Zookeeper) GetData(p string) ([]byte, error) {
	bs, _, err := z.zkConn.Get(path.Join(z.bathPath, p))
	return bs, err
}

func (z *Zookeeper) SetData(p string, data []byte) error {
	_, err := z.zkConn.Create(path.Join(z.bathPath, p), data, 0, zk.WorldACL(zk.PermAll))
	return err
}

func (z *Zookeeper) setBookies(strs []string) {
	bks := make([]string, 0, len(strs))
	for _, str := range strs {
		if str != "readonly" {
			bks = append(bks, str)
		}
	}
	z.bookies.Store(bks)
}

func (z *Zookeeper) zkEventWatch(zkCh, bkCh <-chan zk.Event) {
	for {
		select {
		case ev, ok := <-zkCh:
			if !ok {
				return
			}
			fmt.Println("zookeeper event:", ev)

		case ev, ok := <-bkCh:
			if !ok {
				return
			}
			fmt.Println("bookie event:", ev)
		}
	}
}

func parseZkUri(uriStr string) (addrs []string, basePath string, err error) {
	var uri *url.URL

	if uri, err = url.Parse(uriStr); err != nil {
		return nil, "", err
	}

	if uri.Scheme != "zk" {
		err = errors.New("invalid zk uri")
		return
	}

	basePath = uri.Path
	if basePath == "/" {
		basePath = ""
	}

	addrs = strings.Split(uri.Host, ";")
	return
}
