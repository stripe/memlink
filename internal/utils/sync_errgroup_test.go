package utils

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

// TestGroupBasic ensures that all goroutines are waited upon and error reporting works correctly
func TestGroupBasic(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	ctx := context.Background()
	group, _ := NewSyncErrGroup(ctx)

	group.Go(func(ctx context.Context) error {
		return nil
	})
	group.Go(func(ctx context.Context) error {
		return errors.New("an error occurred")
	})

	// Wait for all goroutines and capture the error
	err := group.Wait()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "an error occurred")
}

// TestGroupExternalContextCancel ensures that context cancellation works correctly when context is cancelled from
// outside the go-routines.
func TestGroupExternalContextCancel(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	ctx := context.Background()
	group, cancelCause := NewSyncErrGroup(ctx)

	f := func(ctx context.Context) error {
		select {
		case <-time.After(200 * time.Millisecond): // Does not finish if context is cancelled
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	group.Go(f)

	time.AfterFunc(1*time.Millisecond, func() {
		cancelCause(errors.New("custom cancellation message"))
	})

	err := group.Wait()
	assert.NotNil(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, context.Cause(group.ctx).Error(), "custom cancellation message")
}

// TestGroupInternalContextCancel ensures that context cancellation works correctly when an go-routine errors out.
func TestGroupInternalContextCancel(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	ctx := context.Background()
	group, _ := NewSyncErrGroup(ctx)

	f1 := func(ctx context.Context) error {
		select {
		case <-time.After(1 * time.Millisecond): // Does not finish if context is cancelled
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}

	f2 := func(ctx context.Context) error {
		select {
		case <-time.After(1 * time.Microsecond):
			return fmt.Errorf("internal go-routine error")
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	group.Go(f1)
	group.Go(f2)

	err := group.Wait()
	assert.NotNil(t, err)
	assert.Equal(t, "internal go-routine error", err.Error())
}

// TestGroupMultipleErrors ensures that only the first error is reported
func TestGroupMultipleErrors(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	ctx := context.Background()
	group, _ := NewSyncErrGroup(ctx)

	f1 := func(ctx context.Context) error {
		return errors.New("first error")
	}

	group.Go(f1)
	err := group.Wait()
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "first error")

	f2 := func(ctx context.Context) error {
		return errors.New("second error")
	}

	group.Go(f2)

	err2 := group.Wait()
	assert.NotNil(t, err2)
	assert.Equal(t, err2.Error(), "first error")
}
