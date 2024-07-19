package bookkeeper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockClient struct {
	emptyClient
	addr string
}

func (c *mockClient) Remote() string {
	return c.addr
}

func newMockClient(_ *Config, addr string) (Client, error) {
	return &mockClient{addr: addr}, nil
}

func TestPoolGetClient(t *testing.T) {
	pool := NewClientPool(&Config{ClientNumPreBookie: 3})
	pool.clientNew = newMockClient

	c, err := pool.GetClient("127.0.0.1:8000", 1)
	assert.NoError(t, err)
	assert.Equal(t, c.Remote(), "127.0.0.1:8000")

	value, ok := pool.clientMap.Load("127.0.0.1:8000")
	assert.True(t, ok)
	assert.Equal(t, len(value.([]Client)), pool.cfg.ClientNumPreBookie)
}
