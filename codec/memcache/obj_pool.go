package memcache

import (
	"bytes"

	"github.com/hemal-shah/memlink/internal/safepool"
)

var bytePool = safepool.NewBufferPool(func() *bytes.Buffer {
	return &bytes.Buffer{}
})
