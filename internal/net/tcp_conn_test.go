package net

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/hemal-shah/memlink/codec"
)

type MockLink struct {
	mock.Mock
}

func (l *MockLink) Done() <-chan struct{} {
	panic("did not implement method for test")
}

func (l *MockLink) Err() error {
	panic("did not implement method for test")
}

func (l *MockLink) Decoder() codec.LinkDecoder {
	args := l.Called()
	return args.Get(0).(codec.LinkDecoder)
}

func (l *MockLink) Encoder() codec.LinkEncoder {
	args := l.Called()
	return args.Get(0).(codec.LinkEncoder)
}

func (l *MockLink) Complete(err error) {
	l.Called(err)
}

type MockLinkDecoder struct {
	mock.Mock
}

func (m *MockLinkDecoder) Reset() {
	panic("did not implement method for test")
}

func (m *MockLinkDecoder) Decode(r *bufio.Reader) error {
	m.Called(r)
	return nil
}

type MockLinkEncoder struct {
	mock.Mock
}

func (m *MockLinkEncoder) Reset() {
	panic("did not implement method for test")
}

func (m *MockLinkEncoder) Encode(w *bufio.Writer) error {
	args := m.Called(w)
	return args.Error(0)
}

type ErrorfulMockLinkEncoder struct {
	mock.Mock
}

func (e *ErrorfulMockLinkEncoder) Encode(w *bufio.Writer) error {
	return e.Called(w).Error(0)
}

func (e *ErrorfulMockLinkEncoder) Reset() {
	panic("did not implement method for test")
}

type DelayedMockLinkEncoder struct {
	mock.Mock
}

func (m *DelayedMockLinkEncoder) Reset() {
	panic("did not implement method for test")
}

func (m *DelayedMockLinkEncoder) Encode(w *bufio.Writer) error {
	m.Called(w)
	time.Sleep(3 * time.Millisecond)
	return nil
}

type ErrorfulMockLinkDecoder struct {
	mock.Mock
}

func (e *ErrorfulMockLinkDecoder) Decode(r *bufio.Reader) error {
	return e.Called(r).Error(0)
}

func (e *ErrorfulMockLinkDecoder) Reset() {
	panic("implement me")
}

func TestNewTCPConnSuccess(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()

	be := NewBackend(listener.Addr(), 1, nil)
	conn, err := NewTCPConn(be, zap.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	tcpConn, ok := conn.(*tcpConn)
	assert.True(t, ok)
	assert.True(t, tcpConn.isConnected())
	time.Sleep(10 * time.Millisecond)
	assert.NoError(t, conn.Close())
	assert.True(t, tcpConn.isTerminated())
}

func TestInvalidConnectionStateAppend(t *testing.T) {
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	be := NewBackend(listener.Addr(), 1, nil)
	conn := &tcpConn{be: be, state: Reconnecting}
	link := &MockLink{}
	err := conn.Append(link)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot append link, connection to 127.0.0.1:11211 is in reconnecting, not connected state")
}

func TestHandleInbound(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	fakeTC := &tcpConn{
		inbound: make(chan codec.Link, 1),
		rw: &bufio.ReadWriter{
			Reader: bufio.NewReader(&bytes.Buffer{}),
		},
		logger: zap.NewNop(),
	}

	link := &MockLink{}
	decoder := &MockLinkDecoder{}
	link.On("Decoder").Return(decoder)
	decoder.On("Decode", fakeTC.rw.Reader).Return(nil)
	link.On("Complete", mock.Anything).Return()

	fakeTC.inbound <- link
	close(fakeTC.inbound)
	err := fakeTC.HandleInbound(context.Background())

	assert.NoError(t, err)
	link.AssertCalled(t, "Complete", nil)
}

func TestHandleOutbound(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()
	fakeTC := &tcpConn{
		outbound: make(chan codec.Link, 1),
		inbound:  make(chan codec.Link, 1),
		rw: &bufio.ReadWriter{
			Writer: bufio.NewWriter(&bytes.Buffer{}),
		},
		logger: zap.NewNop(),
		conn:   conn1,
	}

	link := &MockLink{}
	encoder := &MockLinkEncoder{}
	link.On("Encoder").Return(encoder)
	encoder.On("Encode", fakeTC.rw.Writer).Return(nil)
	link.On("Complete", mock.Anything).Return()

	fakeTC.outbound <- link
	close(fakeTC.outbound)
	err := fakeTC.HandleOutbound(context.Background())

	assert.NoError(t, err)
	link.AssertNotCalled(t, "Complete")
}

func TestClose(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()

	be := NewBackend(listener.Addr(), 1, nil)
	conn, _ := NewTCPConn(be, zap.NewNop())

	fakeTC, _ := conn.(*tcpConn)
	err := conn.Close()

	assert.NoError(t, err)
	assert.Equal(t, Terminated, fakeTC.state)
}

func TestManagerTerminates(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()

	be := NewBackend(listener.Addr(), 1, nil)
	conn, _ := NewTCPConn(be, zap.NewNop())
	time.Sleep(1 * time.Millisecond)
	assert.NoError(t, conn.Close())
	fakeTC, _ := conn.(*tcpConn)
	// Kind of weird -- but the tests need to acquire an lock as well otherwise periodically the go test runtime would
	// complain about dirty read when the `state` was being updated to be `Terminated`. I think this is fine, as the
	// state should be eventually terminated. An helper method can be introduced that can do the same 3 steps here but
	// I am not a huge fan of adding a helper method in the original struct just for a unit test.
	fakeTC.mu.RLock()
	assert.Equal(t, Terminated, fakeTC.state)
	fakeTC.mu.RUnlock()
}

func TestConcurrentStateManagement(t *testing.T) {
	listener, _ := net.Listen("tcp", "localhost:0")
	defer listener.Close()

	be := &Backend{addr: listener.Addr(), numConns: 1}
	fakeTC, err := NewTCPConn(be, zap.NewNop())
	assert.NoError(t, err)
	defer fakeTC.Close()
	conn := fakeTC.(*tcpConn)

	var wg sync.WaitGroup
	numGoroutines := 10
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			link := &MockLink{}
			encoder := &MockLinkEncoder{}
			decoder := &MockLinkDecoder{}
			link.On("Encoder").Return(encoder)
			encoder.On("Encode", mock.Anything).Return(nil)
			link.On("Decoder").Return(decoder)
			decoder.On("Decode", mock.Anything).Return(nil)
			link.On("Complete", nil).Return()
			conn.Append(link)
		}()
	}
	wg.Wait()

	time.Sleep(100 * time.Millisecond)
	assert.True(t, conn.isConnected())
	conn.mu.RLock()
	assert.Equal(t, Connected, conn.state, "connection should be in connected state")
	// both the inbound and outbound queues should be empty
	assert.Equal(t, 0, len(conn.inbound))
	assert.Equal(t, 0, len(conn.outbound))
	conn.mu.RUnlock()
}

func TestHandleConcurrency(t *testing.T) {
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()

	be := &Backend{addr: listener.Addr(), numConns: 1}
	fakeTCP, err := NewTCPConn(be, zap.NewNop())
	assert.NoError(t, err)
	conn := fakeTCP.(*tcpConn)

	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	link1 := &MockLink{}
	encoder1 := &MockLinkEncoder{}
	decoder1 := &MockLinkDecoder{}
	link1.On("Decoder").Return(decoder1)
	link1.On("Encoder").Return(encoder1)
	decoder1.On("Decode", mock.Anything).Return(nil)
	encoder1.On("Encode", mock.Anything).Return(nil)
	link1.On("Complete", nil).Return()

	link2 := &MockLink{}
	encoder2 := &MockLinkEncoder{}
	decoder2 := &MockLinkDecoder{}
	link2.On("Encoder").Return(encoder2)
	link2.On("Decoder").Return(decoder2)
	decoder2.On("Decode", mock.Anything).Return(nil)
	encoder2.On("Encode", mock.Anything).Return(nil)
	link2.On("Complete", nil).Return()

	conn.inbound <- link1
	conn.inbound <- link2

	time.Sleep(10 * time.Millisecond)

	link1.AssertCalled(t, "Complete", nil)
	link2.AssertCalled(t, "Complete", nil)

	assert.True(t, conn.isConnected())
	err = conn.Close()
	assert.NoError(t, err)
}

func TestDataRaceDuringTermination(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()

	be := &Backend{addr: listener.Addr(), numConns: 1}
	fakeTCP, err := NewTCPConn(be, zap.NewNop())
	assert.NoError(t, err)
	conn := fakeTCP.(*tcpConn)
	link := &MockLink{}
	encoder := &DelayedMockLinkEncoder{}
	decoder := &MockLinkDecoder{}
	encoder.On("Encode", mock.Anything).Return()
	decoder.On("Decode", mock.Anything).Return()
	link.On("Encoder").Return(encoder)
	link.On("Decoder").Return(decoder)
	link.On("Complete", mock.Anything).Return()
	assert.NoError(t, fakeTCP.Append(link))
	assert.NoError(t, fakeTCP.Close())
	time.Sleep(10 * time.Millisecond)
	link.AssertCalled(t, "Complete", mock.Anything)
	assert.True(t, conn.isTerminated())
}

func TestIOErrorEncoderPostClose(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()

	be := &Backend{addr: listener.Addr(), numConns: 1}
	fakeTCP, err := NewTCPConn(be, zap.NewNop())
	assert.NoError(t, err)
	conn := fakeTCP.(*tcpConn)

	link1 := &MockLink{}
	encoder1 := &ErrorfulMockLinkEncoder{}
	decoder1 := &MockLinkDecoder{}
	encoder1.On("Encode", mock.Anything).Return(errors.New("error related to i/o simulated here."))
	decoder1.On("Decode", mock.Anything).Return(nil)
	link1.On("Encoder").Return(encoder1)
	link1.On("Decoder").Return(decoder1)
	link1.On("Complete", mock.Anything).Return()
	assert.NoError(t, fakeTCP.Append(link1))
	assert.NoError(t, fakeTCP.Close())
	time.Sleep(10 * time.Millisecond)
	link1.AssertCalled(t, "Complete", mock.Anything)
	assert.True(t, conn.isTerminated())
}

func TestIOErrorDecoderPostClose(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	listener, _ := net.Listen("tcp", "localhost:11211")
	defer listener.Close()

	be := &Backend{addr: listener.Addr(), numConns: 1}
	fakeTCP, err := NewTCPConn(be, zap.NewNop())
	assert.NoError(t, err)
	conn := fakeTCP.(*tcpConn)
	link1 := &MockLink{}
	encoder1 := &MockLinkEncoder{}
	decoder1 := &ErrorfulMockLinkDecoder{}
	encoder1.On("Encode", mock.Anything).Return(nil)
	decoder1.On("Decode", mock.Anything).Return(errors.New("error related to i/o simulated here."))
	link1.On("Encoder").Return(encoder1)
	link1.On("Decoder").Return(decoder1)
	link1.On("Complete", mock.Anything).Return()
	assert.NoError(t, fakeTCP.Append(link1))
	assert.NoError(t, fakeTCP.Close())
	time.Sleep(10 * time.Millisecond)
	link1.AssertCalled(t, "Complete", mock.Anything)
	assert.True(t, conn.isTerminated())
}

func TestSetDeadlineIfNeeded(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()

	fakeTC := &tcpConn{
		conn:      conn1,
		logger:    zap.NewNop(),
		logFields: []zap.Field{},
	}

	err := fakeTC.setDeadlineIfNeeded()
	assert.NoError(t, err)
	assert.False(t, fakeTC.currentDeadline.IsZero(), "currentDeadline should be set after first call")
	firstDeadline := fakeTC.currentDeadline

	err = fakeTC.setDeadlineIfNeeded()
	assert.NoError(t, err)
	assert.Equal(t, firstDeadline, fakeTC.currentDeadline, "currentDeadline should not change on immediate second call")

	pastDeadline := time.Now().Add(500 * time.Millisecond)
	fakeTC.currentDeadline = pastDeadline

	err = fakeTC.setDeadlineIfNeeded()
	assert.NoError(t, err)
	assert.True(t, fakeTC.currentDeadline.After(pastDeadline), "deadline should be updated when close to expiring")

	fakeTC.currentDeadline = time.Time{}
	err = fakeTC.setDeadlineIfNeeded()
	assert.NoError(t, err)
	assert.False(t, fakeTC.currentDeadline.IsZero(), "zero deadline should trigger a new deadline to be set")

	expectedDeadline := time.Now().Add(socketTimeout)
	timeDiff := fakeTC.currentDeadline.Sub(expectedDeadline)
	assert.True(t, timeDiff < time.Second && timeDiff > -time.Second,
		"deadline should be approximately socketTimeout from now, got diff: %v", timeDiff)
}
