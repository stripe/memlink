package safepool

import (
	"bytes"
)

// BufferPool is a safe wrapper around sync.Pool for *bytes.Buffer instances.
type BufferPool struct {
	p Pool[*bytes.Buffer]
}

// NewBufferPool returns a safe wrapper around sync.Pool for *bytes.Buffer instances.
// In particular, it calls Reset() on buffers returned with Put so users don't have
// to remember to manage that themselves.  Like sync.Pool, it is safe for concurrent use.
func NewBufferPool(newFn func() *bytes.Buffer) *BufferPool {
	return &BufferPool{
		p: *NewPool(newFn),
	}
}

// Get returns a *bytes.Buffer.
func (p *BufferPool) Get() *bytes.Buffer {
	return p.p.Get()
}

// Put returns a *bytes.Buffer to the pool for reuse, calling Reset() on the buffer.
func (p *BufferPool) Put(item *bytes.Buffer) {
	item.Reset()
	p.p.Put(item)
}
