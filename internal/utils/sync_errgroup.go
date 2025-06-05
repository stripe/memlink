package utils

import (
	"context"
	"sync"
)

// SyncErrGroup is similar to sync.x.ErrGroup but it differs by passing the context to the functions started in separated go-routines
// and removes the SetLimit functionality as it should provide the opportunity to do as much work as possible but quit
// all routines when the context is done. It's not safe to use the zero-value of SyncErrGroup and should rely on the helper method
// NewSyncErrGroup
type SyncErrGroup struct {
	ctx         context.Context
	cancelCause context.CancelCauseFunc
	wg          sync.WaitGroup

	errOnce sync.Once
	err     error
}

func NewSyncErrGroup(ctx context.Context) (*SyncErrGroup, context.CancelCauseFunc) {
	ctx, cancelCause := context.WithCancelCause(ctx)
	return &SyncErrGroup{ctx: ctx, cancelCause: cancelCause}, cancelCause
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the first non-nil error (if any) from them.
func (g *SyncErrGroup) Wait() error {
	g.wg.Wait()
	return g.err
}

func (g *SyncErrGroup) Go(f func(ctx context.Context) error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		err := f(g.ctx)
		g.cancelCause(err)
		if err != nil {
			g.errOnce.Do(func() {
				g.err = err
			})
		}
	}()
}
