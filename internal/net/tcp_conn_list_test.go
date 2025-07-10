package net

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stripe/memlink/codec"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

type MockTCPConn struct {
	mock.Mock
}

func (m *MockTCPConn) Append(link codec.Link) error {
	args := m.Called(link)
	return args.Error(0)
}

func (m *MockTCPConn) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockTCPConn) IsHealthy() bool {
	return true
}

func TestNewTCPConnections(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close() //nolint: errcheck

	be := NewBackend(listener.Addr(), 3, nil)

	connList, err := NewTCPConnectionList(be, zap.NewNop())
	assert.NoError(t, err)
	defer connList.Close() //nolint: errcheck
	assert.NotNil(t, connList)

	mockTCL, ok := connList.(*tcpConnList)
	assert.True(t, ok)
	assert.Equal(t, uint64(3), mockTCL.numConns)
}

func TestNewTCPConnectionsWithZeroConns(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close() //nolint: errcheck

	be := NewBackend(listener.Addr(), 0, nil)

	connList, err := NewTCPConnectionList(be, zap.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, connList)

	mockTCL, ok := connList.(*tcpConnList)
	assert.True(t, ok)
	assert.Equal(t, uint64(1), mockTCL.numConns)

	time.Sleep(1 * time.Millisecond)
	assert.NoError(t, connList.Close())
}

func TestAppendWithNoConnections(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mockTCL := &tcpConnList{
		conns: []TCPConn{},
	}

	link := &LinkMock{}
	link.On("Chain").Return(nil)

	err := mockTCL.Append(link)
	assert.Error(t, err)
	assert.ErrorIs(t, err, errBackendUnhealthy)
}

func TestAppendSuccessfully(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close() //nolint: errcheck

	be := NewBackend(listener.Addr(), 2, nil)

	mockConn1 := &MockTCPConn{}
	mockConn1.On("Append", mock.Anything).Return(nil)

	mockConn2 := &MockTCPConn{}
	mockConn2.On("Append", mock.Anything).Return(nil)

	fakeTCL := &tcpConnList{
		conns:    []TCPConn{mockConn1, mockConn2},
		numConns: 2,
		be:       be,
		iterIdx:  1,
	}

	link := &LinkMock{}
	link.On("Chain").Return(nil)

	err := fakeTCL.Append(link)
	assert.NoError(t, err)
	mockConn1.AssertCalled(t, "Append", link) // iterIdx % numConns = 1 % 2 = 1
}

func TestCloseConnectionsWithError(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mockConn1 := &MockTCPConn{}
	mockConn1.On("Close").Return(errors.New("close error"))

	mockConn2 := &MockTCPConn{}
	mockConn2.On("Close").Return(nil)

	fakeTCL := &tcpConnList{
		conns:  []TCPConn{mockConn1, mockConn2},
		logger: zap.NewNop(),
	}

	err := fakeTCL.Close()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "close error")
}

func TestCloseConnectionsSuccessfully(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	mockConn1 := &MockTCPConn{}
	mockConn1.On("Close").Return(nil)

	mockConn2 := &MockTCPConn{}
	mockConn2.On("Close").Return(nil)

	fakeTCL := &tcpConnList{
		conns:  []TCPConn{mockConn1, mockConn2},
		logger: zap.NewNop(),
	}

	err := fakeTCL.Close()
	assert.NoError(t, err)
	mockConn1.AssertCalled(t, "Close")
	mockConn2.AssertCalled(t, "Close")
}
