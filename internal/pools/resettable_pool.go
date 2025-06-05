package pools

import (
	"sync"

	"github.com/hemal-shah/memlink/internal"
)

// Like safepool.Pool but for Resettable structs.
type ResettablePool[T internal.Resettable] struct {
	p sync.Pool
}

func NewResettablePool[T internal.Resettable](newFn func() T) *ResettablePool[T] {
	return &ResettablePool[T]{
		p: sync.Pool{
			New: func() interface{} {
				return newFn()
			},
		},
	}
}

func (p *ResettablePool[T]) Get() T {
	i := p.p.Get().(T)
	i.Reset()
	return i
}

func (p *ResettablePool[T]) Put(item T) {
	p.p.Put(item)
}

func (p *ResettablePool[T]) PutAll(items []T) {
	for _, i := range items {
		p.p.Put(i)
	}
}
