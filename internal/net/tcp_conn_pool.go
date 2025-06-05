package net

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/andrew-d/csmrand"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/hemal-shah/memlink/codec"
)

var emptyConnPoolErr = errors.New("tcpConnPool: empty connection pool")
var connPoolExhaustedErr = errors.New("tcpConnPool: exhausted entire connection pool trying to append link")

// TCPConnPool is the ultimate pool which can submit a request to any target address in the connection pool
type TCPConnPool interface {
	Add(be *Backend) error
	Remove(be *Backend) error

	codec.Chain
	Close()
}

type tcpConnPool struct {
	mu            sync.RWMutex
	backends      []*Backend             // protected by mu
	cm            map[string]TCPConnList // protected by mu
	maxIdxForHash int                    // protected by mu

	hashFn HasherFn

	logger    *zap.Logger
	logFields []zap.Field
}

func (t *tcpConnPool) beKey(idx int) string {
	return t.backends[idx].String()
}

func (t *tcpConnPool) Remove(be *Backend) error {
	t.logger.Info(fmt.Sprintf("Removing connection to %s backend", be.String()), t.logFields...)
	t.mu.Lock()
	idx := slices.Index(t.backends, be)

	if idx == -1 {
		t.mu.Unlock()
		return fmt.Errorf("%v backend not found in the list of connection", be)
	}

	cl := t.cm[t.beKey(idx)]
	t.backends = slices.Delete(t.backends, idx, idx+1)
	delete(t.cm, be.addr.String())
	t.maxIdxForHash--
	t.mu.Unlock()

	// cl.Close() call will wait for all the pending requests to complete before attempting to close
	// them, so before we close that we are making sure that there are no new requests issued to the backends.
	return cl.Close()
}

func (t *tcpConnPool) Add(be *Backend) error {
	t.logger.Info(fmt.Sprintf("Adding a new connection to %s backend", be.String()), t.logFields...)
	cl, err := NewTCPConnectionList(be, t.logger)
	if err != nil {
		return err
	}

	t.mu.Lock()
	t.backends = append(t.backends, be)
	t.cm[be.String()] = cl
	t.maxIdxForHash++
	t.mu.Unlock()
	return nil
}

var _ TCPConnPool = (*tcpConnPool)(nil)

// HasherFn takes in a single string and returns an integer, indicating where to map the request to.
// n is the max value of the response value.
type HasherFn func(hashKey string, n int) int

type ConnPoolOptions func(pool *tcpConnPool)

func WithConnPoolHashFn(fn HasherFn) ConnPoolOptions {
	return func(pool *tcpConnPool) {
		pool.hashFn = fn
	}
}

func WithConnPoolLogger(logger *zap.Logger) ConnPoolOptions {
	return func(pool *tcpConnPool) {
		pool.logger = logger
	}
}

func NewConnPool(backends []*Backend, opts ...ConnPoolOptions) (TCPConnPool, error) {
	pool := &tcpConnPool{
		backends:      backends,
		maxIdxForHash: len(backends),
		mu:            sync.RWMutex{},
		logFields: []zap.Field{
			zap.String("pool_id", uuid.NewString()),
		},
	}

	for _, opt := range opts {
		opt(pool)
	}

	if pool.hashFn == nil {
		// use a random hash
		pool.hashFn = RandomHashFn
	}

	if pool.logger == nil {
		logger, err := zap.NewProduction()
		if err != nil {
			return nil, fmt.Errorf("no logger was provided using WithConnPoolLogger ConnPoolOptions and could not create a default zap logger: %w", err)
		}

		pool.logger = logger
	}

	// once all the settings are done, set up actual connections.
	pool.cm = make(map[string]TCPConnList, len(backends))

	for _, be := range backends {
		cl, err := NewTCPConnectionList(be, pool.logger)
		if err != nil {
			return nil, err
		}
		pool.cm[be.String()] = cl
	}

	pool.logger.Info(fmt.Sprintf("Initialized connection pool to %v backends", backends), pool.logFields...)
	return pool, nil
}

func RandomHashFn(_ string, n int) int {
	return csmrand.Intn(n)
}

func (t *tcpConnPool) Append(link codec.Link) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.cm == nil || len(t.cm) == 0 {
		return emptyConnPoolErr
	}

	for i := 0; i < t.maxIdxForHash; i++ {
		// TODO(hemal): need to pass the hashKey somehow through the encoder?
		idx := t.hashFn("", t.maxIdxForHash)

		if idx < 0 || idx >= t.maxIdxForHash {
			return fmt.Errorf("hasherFn returned an index outside the range of [0, %d). Got: %d", t.maxIdxForHash, idx)
		}

		err := t.cm[t.beKey(idx)].Append(link)

		if !errors.Is(err, backendUnhealthyErr) {
			// If append is successfull but there's another form of errors, we should break early and return that.
			return err
		}
	}

	return connPoolExhaustedErr
}

func (t *tcpConnPool) Close() {
	t.logger.Warn("Closing connection pool", t.logFields...)
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, cl := range t.cm {
		_ = cl.Close()
	}
}
