package net

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/stripe/memlink/codec"
	"github.com/stripe/memlink/internal/utils"
)

const (
	// amount of time to spend trying to establish a single connection.
	dialTimeout = 5 * time.Second
	// monitor attempts to re-establish the connection to backend if the connection is either in `ConnectFailed` or
	// `Reconnecting` state this many times. A successful connection establishment should reset the counter.
	monitorRoutineCycles = 1000
	// amount of time to sleep before starting another monitor routine.
	monitorRoutineSleep = 5 * time.Millisecond
	// number of attempts to establish the connection. Main connection monitoring routine will try to establish
	// this connection several times, so if the backend is down, it's better to call `Close()` on this connection.
	connAttemptCount = 3
	// amount of time to wait before trying to establish a connection where it previously failed.
	reconnectDelay = 1 * time.Millisecond

	queueSize = 1000

	// socketTimeout regardless of a request deadline.
	socketTimeout = 5 * time.Second
)

// enum represents state of the connection.
/*
  +----------------+
  |  Unavailable   |
  +----------------+
           |
           |  Success
           v
  +--------+--------+
  |    Connected    |<-------------------+
  +--------+--------+                    |
           |                             |
           | Connection lost             |
           v                             | Retry
  +--------+--------+                    |
  |  Reconnecting   |                    |
  +--------+--------+                    |
           |                             |
           | Connection attempt failed   |
           v                             |
  +----------------+                     |
  | ConnectFailed  | +-------------------+
  +----------------+
*/

type connState string

const (
	Unavailable   connState = "unavailable"
	Connected     connState = "open"
	Terminated    connState = "terminated"
	Reconnecting  connState = "reconnecting"
	ConnectFailed connState = "connect_failed"
)

var (
	errConnChangingState   = errors.New("tcpConn: failed to acquire lock as the connection is changing state")
	errZombieLinkOnEncoder = errors.New("tcpConn: encoder: link was pending in the encoder channel but conn was closed before processing")
	errZombieLinkOnDecoder = errors.New("tcpConn: decoder: link was pending in the decoder channel but conn was closed before processing")
	errOutboundQueueFull   = errors.New("tcpConn: append: outbound channel is full and can't instantly add a new link")
)

// TCPConn represents a single connection to an address.
type TCPConn interface {
	codec.Chain

	Close() error
}

type tcpConn struct {
	be               *Backend
	monitorLoopCount int

	mu    sync.RWMutex
	conn  net.Conn          // protected by mu
	state connState         // protected by mu
	rw    *bufio.ReadWriter // protected by mu

	// outbound is a channel that handles outbound data processing using codec.Link.
	// For each piece of outbound data, a connection buffer is initially passed to the encoder.
	// Once encoded, the data is then published to the inbound channel, ensuring that the
	// processed data is prepared for further handling or transmission.
	outbound chan codec.Link

	// inbound is a channel responsible for processing incoming data sequentially. It passes
	// each reader from the connection to a codec.LinkDecoder, ensuring that the sequence
	// of messages received from the connection is preserved. The order in which messages
	// are published to the inbound channel is exactly the order in which they will be
	// processed, maintaining data consistency and integrity.
	inbound chan codec.Link

	// deadline optimization: track the current deadline to avoid unnecessary SetDeadline calls
	currentDeadline time.Time

	logger    *zap.Logger
	logFields []zap.Field
}

var _ TCPConn = (*tcpConn)(nil)

func NewTCPConn(be *Backend, logger *zap.Logger) (TCPConn, error) {
	c := &tcpConn{
		be:     be,
		state:  Unavailable,
		logger: logger,
		logFields: []zap.Field{
			zap.String("conn_id", uuid.NewString()),
			zap.String("backend", be.String()),
		},
	}

	err := c.setup()
	if err != nil {
		return nil, err
	}

	once := sync.Once{}
	chanStart := make(chan struct{})
	go c.manager(func() {
		once.Do(func() {
			close(chanStart)
		})
	})

	<-chanStart

	return c, nil
}

func (c *tcpConn) Append(link codec.Link) (err error) {
	if c.mu.TryRLock() {
		if c.state == Connected {
			select {
			case c.outbound <- link:
			default:
				err = errOutboundQueueFull
			}
		} else {
			err = fmt.Errorf("cannot append link, connection to %s is in %s, not connected state", c.be.String(), c.state)
		}
		c.mu.RUnlock()
	} else {
		err = errConnChangingState
	}
	return
}

func (c *tcpConn) HandleInbound(ctx context.Context) error {
	c.logger.Debug("HandleInbound is starting", c.logFields...)

	for {
		select {
		case <-ctx.Done():
			c.logger.Debug("HandleInbound is closing due to ctx.Done()", c.logFields...)
			return nil
		case link, ok := <-c.inbound:
			if !ok {
				c.logger.Debug("HandleInbound is closing due to inbound channel not being open", c.logFields...)
				return nil
			}

			err := link.Decoder().Decode(c.rw.Reader)
			if err != nil {
				link.Complete(fmt.Errorf("HandleInbound: error trying to read response from %s backend: %w", c.be.String(), err))
				return err
			}
			link.Complete(nil)
		}
	}
}

func (c *tcpConn) HandleOutbound(ctx context.Context) error {
	c.logger.Debug("HandleOutbound is starting", c.logFields...)

	for {
		select {
		case <-ctx.Done():
			c.logger.Debug("HandleOutbound is closing due to ctx.Done()", c.logFields...)
			return nil
		case link, ok := <-c.outbound:
			if !ok {
				c.logger.Debug("HandleOutbound is closing due to outbound channel not being open", c.logFields...)
				return nil
			}

			if err := c.setDeadlineIfNeeded(); err != nil {
				link.Complete(fmt.Errorf("HandleOutbound: error setting deadline for %s backend: %w", c.be.String(), err))
				return err
			}

			if err := link.Encoder().Encode(c.rw.Writer); err != nil {
				link.Complete(fmt.Errorf("HandleOutbound: error trying to serialize request to a Writer on the %s backend: %w", c.be.String(), err))
				return err
			}

			if flushErr := c.rw.Flush(); flushErr != nil {
				link.Complete(fmt.Errorf("HandleOutbound: error trying to flush request to %s backend: %w", c.be.String(), flushErr))
				return flushErr
			}

			// only add the decoder after the message is safely written through the encoder.
			// we don't need any synchronization primitives as there's just 1 goroutine writing first
			// to the outbound connection and then to the `c.inbound` channel.
			select {
			case c.inbound <- link:
			case <-ctx.Done():
				c.logger.Debug("HandleOutbound is closing due to ctx.Done() while attempting to write to inbound", c.logFields...)
				return nil
			}
		}
	}
}

func (c *tcpConn) Close() error {
	c.logger.Info("received signal to close connection", c.logFields...)
	c.transitionState(Terminated)
	close(c.outbound)
	return c.closeConn()
}

func (c *tcpConn) closeConn() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Close()
}
func (c *tcpConn) transitionState(state connState) {
	c.mu.Lock()
	c.logger.Info(fmt.Sprintf("transitioning the state to %s", state), c.logFields...)
	c.state = state
	c.mu.Unlock()
}

func (c *tcpConn) isTerminated() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state == Terminated
}

func (c *tcpConn) isConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state == Connected
}

// setDeadlineIfNeeded sets the connection deadline only if it's not already set
// to a reasonable future time, avoiding expensive syscalls on every request.
func (c *tcpConn) setDeadlineIfNeeded() error {
	now := time.Now()
	targetDeadline := now.Add(socketTimeout)
	// Only set the deadline if:
	// 1. No deadline is currently set (zero time), or
	// 2. Current deadline is too close (within 1 second)
	if c.currentDeadline.IsZero() ||
		c.currentDeadline.Before(now.Add(time.Second)) {

		if err := c.conn.SetDeadline(targetDeadline); err != nil {
			return err
		}
		c.currentDeadline = targetDeadline
	}

	return nil
}

// manager starts HandleInbound() and HandleOutbound() methods only if there's an active connection. If either
// of those routines return errors due to connection failures (or without) then manager would reset the connection
// and restart the routines unless the connection is Terminated().
func (c *tcpConn) manager(started func()) {
	for ; c.monitorLoopCount < monitorRoutineCycles; c.monitorLoopCount++ {
		if c.isTerminated() {
			c.logger.Debug("Manager routine will quit attempting as connection is closed", c.logFields...)
			return
		}

		if c.isConnected() {
			c.logger.Debug("Starting errgroup with HandleInbound and HandleOutbound routines", c.logFields...)
			eg, _ := utils.NewSyncErrGroup(context.Background())
			eg.Go(c.HandleInbound)
			eg.Go(c.HandleOutbound)
			started()
			_ = eg.Wait()
		}

		// Once a connection is terminated, the context would be done and we should still clear out the
		// zombie connection on the list. This results in more mutex accesses than I like, but there doesn't
		// seem to be a good way around this. Transitioning the state to Reconnecting helps prevent new request
		// to be enqueued to this connection.
		if !c.isTerminated() {
			c.transitionState(Reconnecting)
		}

		// drain zombie link before resetting the channels.
		c.mu.Lock()
		pendingOutboundLinks := len(c.outbound)
		for i := 0; i < pendingOutboundLinks; i++ {
			link := <-c.outbound
			link.Complete(errZombieLinkOnEncoder)
		}

		pendingInboundLinks := len(c.inbound)
		for i := 0; i < pendingInboundLinks; i++ {
			link := <-c.inbound
			link.Complete(errZombieLinkOnDecoder)
		}
		c.mu.Unlock()

		if c.isTerminated() {
			c.logger.Debug("Manager routine is exiting after cleaning up the zombie links in queue", c.logFields...)
			return
		}

		time.Sleep(monitorRoutineSleep)
		_ = c.setup()
	}

	c.logger.Error("Monitor loop giving up on trying to connect to backend.", c.logFields...)
}

func (c *tcpConn) setup() error {
	var lastConnErr error
	for i := 0; i < connAttemptCount; i++ {
		c.logger.Debug("Trying to establish connection to backend", append(c.logFields, zap.Int("attempt", i))...)
		conn, err := dial(context.Background(), c.be.addr, c.be.tlsConfig)
		if err != nil {
			lastConnErr = err
			time.Sleep(reconnectDelay)
			continue
		}

		rw := bufio.NewReadWriter(
			bufio.NewReader(conn),
			bufio.NewWriter(conn))

		c.logger.Debug("Successfully established a connection", c.logFields...)
		c.mu.Lock()
		c.inbound = make(chan codec.Link, queueSize)
		c.outbound = make(chan codec.Link, queueSize)
		c.conn = conn
		c.rw = rw
		c.currentDeadline = time.Time{}
		c.state = Connected
		c.monitorLoopCount = 0
		c.mu.Unlock()
		return nil
	}

	c.transitionState(ConnectFailed)
	return lastConnErr
}
