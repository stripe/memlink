package memcache

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MetaSetDecoders_HappyPath(t *testing.T) {
	targs := []struct {
		name                   string
		memcachedResponse      []byte
		expectedMetadataStatus MetadataStatus
		expectedOpaque         uint64
		expectedCasId          uint64
	}{
		{
			name:                   "baseline ms response",
			memcachedResponse:      []byte("HD\r\n"),
			expectedMetadataStatus: Stored,
			expectedOpaque:         0,
			expectedCasId:          0,
		},
		{
			name:                   "ms response with opaque",
			memcachedResponse:      []byte("HD O1231\r\n"),
			expectedMetadataStatus: Stored,
			expectedOpaque:         1231,
			expectedCasId:          0,
		},
		{
			name:                   "ms response with opaque and cas id",
			memcachedResponse:      []byte("HD O1231 c1111\r\n"),
			expectedMetadataStatus: Stored,
			expectedOpaque:         1231,
			expectedCasId:          1111,
		},
		{
			name:                   "not stored response with opaque and cas id",
			memcachedResponse:      []byte("NS O1231 c1111\r\n"),
			expectedMetadataStatus: NotStored,
			expectedOpaque:         1231,
			expectedCasId:          1111,
		},
		{
			name:                   "exists response with opaque and cas id",
			memcachedResponse:      []byte("EX O1231 c1111\r\n"),
			expectedMetadataStatus: Exists,
			expectedOpaque:         1231,
			expectedCasId:          1111,
		},
		{
			name:                   "not found with opaque ",
			memcachedResponse:      []byte("NF O1231 \r\n"),
			expectedMetadataStatus: NotFound,
			expectedOpaque:         1231,
			expectedCasId:          0,
		},
	}

	for _, tt := range targs {
		t.Run(tt.name, func(t *testing.T) {

			data := &bytes.Buffer{}
			writer := bufio.NewWriter(data)

			writer.Write(tt.memcachedResponse)
			writer.Flush()

			decoder := &MetaSetDecoder{}
			decoder.Reset()

			mockReader := bufio.NewReader(data)
			err := decoder.Decode(mockReader)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOpaque, decoder.Opaque)
			assert.Equal(t, tt.expectedCasId, decoder.CasId)
			assert.Equal(t, tt.expectedMetadataStatus, decoder.Status)
		})
	}
}

func Test_MetaSetDecoder_ErrorPath(t *testing.T) {
	targs := []struct {
		name          string
		erroneousLine []byte
	}{
		{
			name:          "incorrect opaque",
			erroneousLine: []byte("HD O123kk\r\n"),
		},
		{
			name:          "incorrect cas id",
			erroneousLine: []byte("HD c9877F\r\n"),
		},
	}

	for _, tt := range targs {
		t.Run(tt.name, func(t *testing.T) {

			data := &bytes.Buffer{}
			writer := bufio.NewWriter(data)

			writer.Write(tt.erroneousLine)
			writer.Flush()
			decoder := &MetaSetDecoder{}
			decoder.Reset()

			mockReader := bufio.NewReader(data)
			err := decoder.Decode(mockReader)
			assert.Error(t, err)
		})
	}

}
