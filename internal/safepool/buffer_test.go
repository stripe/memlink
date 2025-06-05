package safepool

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewBufferPool(t *testing.T) {
	p := NewBufferPool(func() *bytes.Buffer {
		return bytes.NewBuffer(nil)
	})
	require.NotNil(t, p)

	buf := p.Get()
	require.NotNil(t, buf)
	require.Zero(t, buf.Len())
	p.Put(buf)
	buf = nil
}
