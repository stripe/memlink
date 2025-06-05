package main

import (
	"context"
	"fmt"
	"net"

	"github.com/hemal-shah/memlink/codec"
	"github.com/hemal-shah/memlink/codec/memcache"
	netpkg "github.com/hemal-shah/memlink/internal/net"
	"go.uber.org/zap"
)

// MemcachedClient provides a high-level interface for interacting with memcached instances
type MemcachedClient interface {
	// MetaSet takes a MetaSetEncoder and MetaSetDecoder as pointers
	MetaSet(ctx context.Context, encoder *memcache.MetaSetEncoder, decoder *memcache.MetaSetDecoder) error

	// MetaGet takes a MetaGetEncoder and MetaGetDecoder as pointers
	MetaGet(ctx context.Context, encoder *memcache.MetaGetEncoder, decoder *memcache.MetaGetDecoder) error

	// MetaDelete takes a MetaDeleteEncoder and MetaDeleteDecoder as pointers
	MetaDelete(ctx context.Context, encoder *memcache.MetaDeleteEncoder, decoder *memcache.MetaDeleteDecoder) error

	// MetaIncrement takes a MetaArithmeticEncoder and MetaArithmeticDecoder as pointers
	MetaIncrement(ctx context.Context, encoder *memcache.MetaArithmeticEncoder, decoder *memcache.MetaArithmeticDecoder) error

	// MetaDecrement takes a MetaArithmeticEncoder and MetaArithmeticDecoder as pointers
	MetaDecrement(ctx context.Context, encoder *memcache.MetaArithmeticEncoder, decoder *memcache.MetaArithmeticDecoder) error

	// BulkGet takes a BulkEncoder and BulkDecoder as pointers
	BulkGet(ctx context.Context, encoder *memcache.BulkEncoder[*memcache.MetaGetEncoder], decoder *memcache.BulkDecoder[*memcache.MetaGetDecoder]) error

	// Close closes all connections
	Close() error
}

// memcachedClient implements MemcachedClient
type memcachedClient struct {
	pool   netpkg.TCPConnPool
	logger *zap.Logger
}

// NewClient creates a new memcached client connected to the specified addresses
func NewClient(addresses []string, numConnsPerBackend int, opts ...ClientOption) (MemcachedClient, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("at least one address must be provided")
	}

	// Parse addresses and create backends
	backends := make([]*netpkg.Backend, 0, len(addresses))
	for _, addr := range addresses {
		tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("invalid address %s: %w", addr, err)
		}
		backends = append(backends, netpkg.NewBackend(tcpAddr, numConnsPerBackend, nil))
	}

	// Create connection pool
	poolOpts := []netpkg.ConnPoolOptions{
		netpkg.WithConnPoolLogger(zap.NewNop()),
	}

	pool, err := netpkg.NewConnPool(backends, poolOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	client := &memcachedClient{
		pool:   pool,
		logger: zap.NewNop(),
	}

	// Apply client options
	for _, opt := range opts {
		opt(client)
	}

	return client, nil
}

// ClientOption configures a memcached client
type ClientOption func(*memcachedClient)

// WithLogger sets a custom logger for the client
func WithLogger(logger *zap.Logger) ClientOption {
	return func(c *memcachedClient) {
		c.logger = logger
	}
}

// append is a helper method that abstracts the common pattern of creating a link,
// appending it to the pool, and waiting for completion
func (c *memcachedClient) append(ctx context.Context, e codec.LinkEncoder, d codec.LinkDecoder) error {
	link := codec.NewGenericLink(e, d)
	if err := c.pool.Append(link); err != nil {
		return fmt.Errorf("failed to append request: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-link.Done():
		return link.Err()
	}
}

// MetaSet takes a MetaSetEncoder and MetaSetDecoder as pointers
func (c *memcachedClient) MetaSet(ctx context.Context, encoder *memcache.MetaSetEncoder, decoder *memcache.MetaSetDecoder) error {
	if err := c.append(ctx, encoder, decoder); err != nil {
		return fmt.Errorf("MetaSet operation failed: %w", err)
	}

	return nil
}

// MetaGet takes a MetaGetEncoder and MetaGetDecoder as pointers
func (c *memcachedClient) MetaGet(ctx context.Context, encoder *memcache.MetaGetEncoder, decoder *memcache.MetaGetDecoder) error {
	if err := c.append(ctx, encoder, decoder); err != nil {
		return fmt.Errorf("MetaGet operation failed: %w", err)
	}

	return nil
}

// MetaDelete takes a MetaDeleteEncoder and MetaDeleteDecoder as pointers
func (c *memcachedClient) MetaDelete(ctx context.Context, encoder *memcache.MetaDeleteEncoder, decoder *memcache.MetaDeleteDecoder) error {
	if err := c.append(ctx, encoder, decoder); err != nil {
		return fmt.Errorf("MetaDelete operation failed: %w", err)
	}

	return nil
}

// MetaIncrement takes a MetaArithmeticEncoder and MetaArithmeticDecoder as pointers
func (c *memcachedClient) MetaIncrement(ctx context.Context, encoder *memcache.MetaArithmeticEncoder, decoder *memcache.MetaArithmeticDecoder) error {
	if err := c.append(ctx, encoder, decoder); err != nil {
		return fmt.Errorf("MetaIncrement operation failed: %w", err)
	}

	return nil
}

// MetaDecrement takes a MetaArithmeticEncoder and MetaArithmeticDecoder as pointers
func (c *memcachedClient) MetaDecrement(ctx context.Context, encoder *memcache.MetaArithmeticEncoder, decoder *memcache.MetaArithmeticDecoder) error {
	if err := c.append(ctx, encoder, decoder); err != nil {
		return fmt.Errorf("MetaDecrement operation failed: %w", err)
	}

	return nil
}

// BulkGet takes a BulkEncoder and BulkDecoder as pointers
func (c *memcachedClient) BulkGet(ctx context.Context, encoder *memcache.BulkEncoder[*memcache.MetaGetEncoder], decoder *memcache.BulkDecoder[*memcache.MetaGetDecoder]) error {
	if err := c.append(ctx, encoder, decoder); err != nil {
		return fmt.Errorf("BulkGet operation failed: %w", err)
	}

	return nil
}

// Close closes all connections
func (c *memcachedClient) Close() error {
	c.pool.Close()
	return nil
}
