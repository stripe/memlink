package net

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/hemal-shah/memlink/codec"
)

type MockTCPConnList struct {
	mock.Mock
}

func (m *MockTCPConnList) IsHealthy() bool {
	return true
}

func (m *MockTCPConnList) Append(link codec.Link) error {
	args := m.Called(link)
	return args.Error(0)
}

func (m *MockTCPConnList) Close() error {
	args := m.Called()
	return args.Error(0)
}

type LinkMock struct {
	mock.Mock
}

func (l *LinkMock) Encoder() codec.LinkEncoder {
	panic("Intentionally not implemented for mocked structs in unit tests")
}

func (l *LinkMock) Decoder() codec.LinkDecoder {
	panic("Intentionally not implemented for mocked structs in unit tests")
}

func (l *LinkMock) Done() <-chan struct{} {
	panic("Intentionally not implemented for mocked structs in unit tests")
}

func (l *LinkMock) Complete(err error) {
	panic("Intentionally not implemented for mocked structs in unit tests")
}

func (l *LinkMock) Err() error {
	panic("Intentionally not implemented for mocked structs in unit tests")
}

func TestNewConnPoolWithEmptyBackends(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	pool, err := NewConnPool([]*Backend{})
	assert.NoError(t, err)
	assert.NotNil(t, pool)
}

func TestRemoveNonExistentBackend(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()

	be := NewBackend(listener.Addr(), 1, nil)

	mockTcpConn := &MockTCPConnList{}
	mockTcpConn.On("Close").Return(nil)

	pool := &tcpConnPool{
		backends: []*Backend{},
		cm:       map[string]TCPConnList{},
		hashFn:   RandomHashFn,
		logger:   zap.NewNop(),
	}

	err := pool.Remove(be)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "backend not found")
}

func TestAppendToEmptyConnPool(t *testing.T) {
	pool := &tcpConnPool{
		backends: nil,
		cm:       nil,
		hashFn:   RandomHashFn,
	}

	link := &LinkMock{}
	link.On("Chain").Return(nil)

	err := pool.Append(link)
	assert.Error(t, err)
	assert.Equal(t, err, emptyConnPoolErr)
}

func TestAppendWithInvalidHasherIdx(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()

	be := NewBackend(listener.Addr(), 1, nil)

	pool := &tcpConnPool{
		backends: []*Backend{be},
		cm: map[string]TCPConnList{
			be.String(): &MockTCPConnList{},
		},
		hashFn: func(hashKey string, n int) int {
			return n
		},
		maxIdxForHash: 1,
	}

	link := &LinkMock{}
	link.On("Chain").Return(nil)

	err := pool.Append(link)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index outside the range")
}

func TestAppendingCorrectly(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()

	be := NewBackend(listener.Addr(), 1, nil)

	mockTcpConn := &MockTCPConnList{}
	pool := &tcpConnPool{
		backends: []*Backend{be},
		cm: map[string]TCPConnList{
			be.String(): mockTcpConn,
		},
		hashFn:        RandomHashFn,
		maxIdxForHash: 1,
	}

	link := &LinkMock{}
	link.On("Chain").Return(nil)
	mockTcpConn.On("Append", link).Return(nil)

	err := pool.Append(link)
	assert.NoError(t, err)
	mockTcpConn.AssertCalled(t, "Append", link)
}

func TestClosingPool(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()

	be := NewBackend(listener.Addr(), 1, nil)

	mockTcpConn := &MockTCPConnList{}
	mockTcpConn.On("Close").Return(nil)

	pool := &tcpConnPool{
		backends: []*Backend{be},
		cm: map[string]TCPConnList{
			be.String(): mockTcpConn,
		},
		hashFn: RandomHashFn,
		logger: zap.NewNop(),
	}

	pool.Close()
	mockTcpConn.AssertCalled(t, "Close")
}

func TestAddRemoveBackend(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()

	be := NewBackend(listener.Addr(), 1, nil)

	pool := &tcpConnPool{
		backends: []*Backend{},
		cm:       map[string]TCPConnList{},
		hashFn:   RandomHashFn,
		logger:   zap.NewNop(),
	}

	err := pool.Add(be)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(pool.backends))

	err = pool.Remove(be)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(pool.backends))
}
