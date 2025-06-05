package memcache

import (
	"fmt"
	"sync/atomic"
)

var Counter = &atomic.Uint64{}

func NextOpaque() uint64 {
	return Counter.Add(1)
}

// NextNOpaques increments the given counter by n + 1 in a single operation.
// It returns the counter's value before the addition.
func NextNOpaques(n uint64) uint64 {
	o := Counter.Add(n)
	return o - n + 1
}

type OpaqueMismatchErr struct {
	expectedOpaque uint64
	actualOpaque   uint64
	operation      string
}

func (e OpaqueMismatchErr) Error() string {
	return fmt.Sprintf("[ExpectedOpaque=%d] [ActualOpaque=%d] [Operation=%s]", e.expectedOpaque, e.actualOpaque, e.operation)
}

func NewOpaqueMismatchErr(expected, actual uint64, op string) error {
	return &OpaqueMismatchErr{
		expectedOpaque: expected,
		actualOpaque:   actual,
		operation:      op,
	}
}
