package memcache

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MetaArithmeticDecoders_HappyPath(t *testing.T) {
	targs := []struct {
		name                   string
		memcachedResponse      []byte
		expectedMetadataStatus MetadataStatus
		expectedOpaque         uint64
		expectedUpdatedValue   uint64
	}{
		{
			name:                   "baseline ma response",
			memcachedResponse:      []byte("HD\r\n"),
			expectedMetadataStatus: Stored,
			expectedOpaque:         0,
		},
		{
			name:                   "ma response with opaque",
			memcachedResponse:      []byte("HD O1231\r\n"),
			expectedMetadataStatus: Stored,
			expectedOpaque:         1231,
		},
		{
			name:                   "not stored response with opaque",
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
		{
			name:                   "stored with values",
			memcachedResponse:      []byte("VA 2 O1231\r\n12\r\n"),
			expectedMetadataStatus: Stored,
			expectedOpaque:         1231,
			expectedUpdatedValue:   12,
		},
		{
			name:                   "stored with big values",
			memcachedResponse:      []byte("VA 20 O1231\r\n18446744073709551615\r\n"),
			expectedMetadataStatus: Stored,
			expectedOpaque:         1231,
			expectedUpdatedValue:   18446744073709551615,
		},
	}

	for _, tt := range targs {
		t.Run(tt.name, func(t *testing.T) {
			data := &bytes.Buffer{}
			writer := bufio.NewWriter(data)

			_, err := writer.Write(tt.memcachedResponse)
			assert.NoError(t, err)
			assert.NoError(t, writer.Flush())

			decoder := &MetaArithmeticDecoder{}
			decoder.Reset()

			mockReader := bufio.NewReader(data)
			err = decoder.Decode(mockReader)
			assert.NoError(t, err)

			assert.Equal(t, tt.expectedOpaque, decoder.Opaque)
			assert.Equal(t, tt.expectedMetadataStatus, decoder.Status)
			assert.Equal(t, tt.expectedUpdatedValue, decoder.ValueUInt64)
		})
	}
}

func Test_MetaArithmeticDecoder_ErrorPath(t *testing.T) {
	targs := []struct {
		name          string
		erroneousLine []byte
	}{
		{
			name:          "incorrect opaque",
			erroneousLine: []byte("VA 2 O123kk\r\n12\r\n"),
		},
	}

	for _, tt := range targs {
		t.Run(tt.name, func(t *testing.T) {
			data := &bytes.Buffer{}
			writer := bufio.NewWriter(data)

			_, err := writer.Write(tt.erroneousLine)
			assert.NoError(t, err)
			assert.NoError(t, writer.Flush())

			decoder := &MetaArithmeticDecoder{}
			decoder.Reset()

			mockReader := bufio.NewReader(data)
			err = decoder.Decode(mockReader)
			assert.Error(t, err)
		})
	}

}
