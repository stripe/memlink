package pools

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// MockResettable is a mock implementation for testing Resettable interface
type MockResettable struct {
	resetCalled bool
}

func (m *MockResettable) Reset() {
	m.resetCalled = true
}

func Test_ResettablePool_Get(t *testing.T) {
	newFn := func() *MockResettable {
		return &MockResettable{}
	}

	pool := NewResettablePool(newFn)

	// Get an item from the pool and verify it was reset
	item := pool.Get()

	assert.True(t, item.resetCalled)
}

func Test_ResettablePool_PutAndGet(t *testing.T) {
	newFn := func() *MockResettable {
		return &MockResettable{}
	}

	pool := NewResettablePool(newFn)

	// Create a new item, reset status should be false initially
	item := new(MockResettable)

	assert.False(t, item.resetCalled)

	// Put the item back into the pool
	pool.Put(item)

	// Get the item from the pool, the reset should have been called
	reusedItem := pool.Get()

	assert.True(t, reusedItem.resetCalled)
}
