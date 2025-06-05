package memcache

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_BulkGetDecoder_ErrorPath(t *testing.T) {
	targs := []struct {
		name          string
		erroneousLine []byte
	}{
		{
			name:          "Incorrect value size format",
			erroneousLine: []byte("HD 132kk \r\n"),
		},
		{
			name:          "incorrect opaque",
			erroneousLine: []byte("HD 5 Ohello \r\n"),
		},
		{
			name:          "Incorrect ttl",
			erroneousLine: []byte("HD 1 t10f \r\n"),
		},
		{
			name:          "Incorrect cas",
			erroneousLine: []byte("HD c10d3 \r\n"),
		},
	}

	for _, tt := range targs {
		t.Run(tt.name, func(t *testing.T) {
			data := &bytes.Buffer{}
			writer := bufio.NewWriter(data)

			_, _ = writer.Write(tt.erroneousLine)
			_ = writer.Flush()
			decoder := &BulkDecoder[*MetaGetDecoder]{}
			decoder.Reset()
			mockReader := bufio.NewReader(data)
			err := decoder.Decode(mockReader)
			assert.Error(t, err)
		})
	}
}
