package memcache

import (
	"bytes"

	"github.com/stripe/memlink/internal/safepool"
)

var bytePool = safepool.NewBufferPool(func() *bytes.Buffer {
	return &bytes.Buffer{}
})
