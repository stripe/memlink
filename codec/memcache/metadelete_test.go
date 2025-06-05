package memcache

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MetaDeleteDecoders_HappyPath(t *testing.T) {
	targs := []struct {
		name                   string
		memcachedResponse      []byte
		expectedMetadataStatus MetadataStatus
		expectedOpaque         uint64
	}{
		{
			name:                   "baseline md response",
			memcachedResponse:      []byte("HD\r\n"),
			expectedMetadataStatus: Deleted,
			expectedOpaque:         0,
		},
		{
			name:                   "md response with opaque",
			memcachedResponse:      []byte("HD O1231\r\n"),
			expectedMetadataStatus: Deleted,
			expectedOpaque:         1231,
		},
		{
			name:                   "not stored response with opaque and cas id",
			memcachedResponse:      []byte("NS O1231 \r\n"),
			expectedMetadataStatus: NotStored,
			expectedOpaque:         1231,
		},
		{
			name:                   "exists response with opaque",
			memcachedResponse:      []byte("EX O1231 \r\n"),
			expectedMetadataStatus: Exists,
			expectedOpaque:         1231,
		},
		{
			name:                   "not found with opaque ",
			memcachedResponse:      []byte("NF O1231 \r\n"),
			expectedMetadataStatus: NotFound,
			expectedOpaque:         1231,
		},
	}

	for _, tt := range targs {
		t.Run(tt.name, func(t *testing.T) {

			data := &bytes.Buffer{}
			writer := bufio.NewWriter(data)

			writer.Write(tt.memcachedResponse)
			writer.Flush()

			decoder := &MetaDeleteDecoder{}
			decoder.Reset()

			mockReader := bufio.NewReader(data)
			err := decoder.Decode(mockReader)
			assert.NoError(t, err)

			assert.Equal(t, tt.expectedOpaque, decoder.Opaque)
			assert.Equal(t, tt.expectedMetadataStatus, decoder.Status)
		})
	}
}

func Test_MetaDeleteDecoder_ErrorPath(t *testing.T) {
	targs := []struct {
		name          string
		erroneousLine []byte
	}{
		{
			name:          "incorrect opaque",
			erroneousLine: []byte("HD O123kk\r\n"),
		},
	}

	for _, tt := range targs {
		t.Run(tt.name, func(t *testing.T) {

			data := &bytes.Buffer{}
			writer := bufio.NewWriter(data)

			writer.Write(tt.erroneousLine)
			writer.Flush()
			decoder := &MetaDeleteDecoder{}
			decoder.Reset()

			mockReader := bufio.NewReader(data)
			err := decoder.Decode(mockReader)
			assert.Error(t, err)
		})
	}

}
