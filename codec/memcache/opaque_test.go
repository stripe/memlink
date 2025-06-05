package memcache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNextOpaque(t *testing.T) {
	Counter.Store(0)
	first := NextOpaque()
	assert.Equal(t, uint64(1), first)

	second := NextOpaque()
	assert.Equal(t, uint64(2), second)
}

func TestNextNOpaques(t *testing.T) {
	Counter.Store(0)
	val := NextNOpaques(3)
	assert.Equal(t, uint64(1), val)

	valAfter := Counter.Load()
	assert.Equal(t, uint64(3), valAfter)
}

func TestOpaqueMismatchErr_Error(t *testing.T) {
	err := NewOpaqueMismatchErr(1, 2, "md")

	expectedMessage := "[ExpectedOpaque=1] [ActualOpaque=2] [Operation=md]"
	assert.EqualError(t, err, expectedMessage)
}

func Test_MultipleRoutinesCollisionFreeOpaque(t *testing.T) {
	numRoutines := 10

	Counter.Store(0)
	rwLock := sync.RWMutex{}
	seenOpaques := map[uint64]interface{}{}

	wg := &sync.WaitGroup{}
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				token := NextOpaque()

				rwLock.RLock()
				if _, ok := seenOpaques[token]; ok {
					assert.Fail(t, "found a repeated counter value for opaque")
				}
				rwLock.RUnlock()

				rwLock.Lock()
				seenOpaques[token] = struct{}{}
				rwLock.Unlock()
			}
		}()
	}
	wg.Wait()
}
