package net

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/stripe/memlink/codec"
)

var errBackendUnhealthy = errors.New("connection to backend is unhealthy")

// TCPConnList maintains a set of connections to the same underlying address internally and allows a
// write to go through.
type TCPConnList interface {
	codec.Chain

	Close() error
}

type tcpConnList struct {
	// num of physical connections to established to underlying backend address.
	numConns uint64
	be       *Backend

	// conns is the list of connections which allow to send outbound and receive inbound messages
	// In a given tcpConnList, the traffic is currently sent randomly, without alternate load-balancing policies.
	conns   []TCPConn
	iterIdx uint64

	logFields []zapcore.Field
	logger    *zap.Logger
}

func (t *tcpConnList) Close() error {
	t.logger.Debug("Closing connection list", t.logFields...)
	errs := make([]error, 0)
	for _, conn := range t.conns {
		err := conn.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (t *tcpConnList) Append(link codec.Link) error {
	for i := uint64(0); i < t.numConns; i++ {
		newIterIdx := atomic.AddUint64(&t.iterIdx, 1)
		target := newIterIdx % t.numConns

		if err := t.conns[target].Append(link); !errors.Is(err, errConnChangingState) {
			return err
		}
	}

	return fmt.Errorf("backend=%s attempts=%d error=%w", t.be.String(), t.numConns, errBackendUnhealthy)
}

var _ TCPConnList = (*tcpConnList)(nil)

// NewTCPConnectionList establishes connection to the given backend. Backend can contain the optional tlsConfig and the
// number of connections to create to that backend.
func NewTCPConnectionList(b *Backend, logger *zap.Logger) (TCPConnList, error) {
	// if less than 1 connection is requested, we default to 1
	numConns := int(math.Max(1, float64(b.numConns)))

	connList := make([]TCPConn, 0, numConns)

	for i := 0; i < numConns; i++ {
		conn, err := NewTCPConn(b, logger)
		if err != nil {
			return nil, err
		}

		connList = append(connList, conn)
	}

	l := &tcpConnList{
		numConns: uint64(numConns),
		conns:    connList,
		be:       b,
		logger:   logger,
		logFields: []zap.Field{
			zap.String("list_id", uuid.NewString()),
			zap.String("backend", b.String()),
		},
	}

	logger.Debug("Initialized connection list to backend", l.logFields...)

	return l, nil
}
