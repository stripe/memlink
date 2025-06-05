package memcache

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MetaGetDecoders_HappyPath(t *testing.T) {

	targs := []struct {
		name                   string
		memcachedResponse      []byte
		expectedMetadataStatus MetadataStatus
		expectedRecacheStatus  RecacheStatus
		expectedRemainingTTL   int32
		expectedCasId          uint64
		expectedValue          []byte
		expectedOpaque         uint64
		expectedStale          bool
	}{
		{
			name:                   "cache miss response",
			memcachedResponse:      []byte("EN\r\n"),
			expectedMetadataStatus: CacheMiss,
			expectedRecacheStatus:  RecacheNotSet,
			expectedRemainingTTL:   0,
			expectedCasId:          0,
			expectedValue:          nil,
			expectedOpaque:         0,
			expectedStale:          false,
		},
		{
			name:                   "cache miss response with opaque",
			memcachedResponse:      []byte("EN O123123\r\n"),
			expectedMetadataStatus: CacheMiss,
			expectedRecacheStatus:  RecacheNotSet,
			expectedRemainingTTL:   0,
			expectedCasId:          0,
			expectedValue:          nil,
			expectedOpaque:         123123,
			expectedStale:          false,
		},
		{
			name:                   "header only for cache hit",
			memcachedResponse:      []byte("HD\r\n"),
			expectedMetadataStatus: CacheHit,
			expectedRecacheStatus:  RecacheNotSet,
			expectedRemainingTTL:   0,
			expectedCasId:          0,
			expectedValue:          nil,
			expectedOpaque:         0,
			expectedStale:          false,
		},
		{
			name:                   "header only for cache hit with opaque",
			memcachedResponse:      []byte("HD O1231213\r\n"),
			expectedMetadataStatus: CacheHit,
			expectedRecacheStatus:  RecacheNotSet,
			expectedRemainingTTL:   0,
			expectedCasId:          0,
			expectedValue:          nil,
			expectedOpaque:         1231213,
			expectedStale:          false,
		},
		{
			name:                   "header only for cache hit with opaque and ttl",
			memcachedResponse:      []byte("HD O1231213 t989\r\n"),
			expectedMetadataStatus: CacheHit,
			expectedRecacheStatus:  RecacheNotSet,
			expectedRemainingTTL:   989,
			expectedCasId:          0,
			expectedValue:          nil,
			expectedOpaque:         1231213,
			expectedStale:          false,
		},
		{
			name:                   "header only for cache hit with opaque, recache won",
			memcachedResponse:      []byte("HD O1231213 W\r\n"),
			expectedMetadataStatus: CacheHit,
			expectedRecacheStatus:  RecacheWon,
			expectedRemainingTTL:   0,
			expectedCasId:          0,
			expectedValue:          nil,
			expectedOpaque:         1231213,
			expectedStale:          false,
		},
		{
			name:                   "header only for cache hit with opaque, recache sent",
			memcachedResponse:      []byte("HD O1231213 Z\r\n"),
			expectedMetadataStatus: CacheHit,
			expectedRecacheStatus:  RecacheAlreadySent,
			expectedRemainingTTL:   0,
			expectedCasId:          0,
			expectedValue:          nil,
			expectedOpaque:         1231213,
			expectedStale:          false,
		},
		{
			name:                   "header only for cache hit with opaque, recache sent, and stale",
			memcachedResponse:      []byte("HD O1231213 Z X\r\n"),
			expectedMetadataStatus: CacheHit,
			expectedRecacheStatus:  RecacheAlreadySent,
			expectedRemainingTTL:   0,
			expectedCasId:          0,
			expectedValue:          nil,
			expectedOpaque:         1231213,
			expectedStale:          true,
		},
		{
			name:                   "with data",
			memcachedResponse:      []byte("VA 10 O1231213 Z X\r\n1234567890\r\n"),
			expectedMetadataStatus: CacheHit,
			expectedRecacheStatus:  RecacheAlreadySent,
			expectedRemainingTTL:   0,
			expectedCasId:          0,
			expectedValue:          []byte("1234567890"),
			expectedOpaque:         1231213,
			expectedStale:          true,
		},
		{
			name:                   "with crlf in data",
			memcachedResponse:      []byte("VA 10 O1231213 Z X\r\n12345\r\n890\r\n"),
			expectedMetadataStatus: CacheHit,
			expectedRecacheStatus:  RecacheAlreadySent,
			expectedRemainingTTL:   0,
			expectedCasId:          0,
			expectedValue:          []byte("12345\r\n890"),
			expectedOpaque:         1231213,
			expectedStale:          true,
		},
		{
			name:                   "with data and recache won",
			memcachedResponse:      []byte("VA 10 O999999 W\r\n1234567890\r\n"),
			expectedMetadataStatus: CacheHit,
			expectedRecacheStatus:  RecacheWon,
			expectedRemainingTTL:   0,
			expectedCasId:          0,
			expectedValue:          []byte("1234567890"),
			expectedOpaque:         999999,
			expectedStale:          false,
		},
		{
			name:                   "zero length data",
			memcachedResponse:      []byte("VA 0 O999999 W\r\n\r\n"),
			expectedMetadataStatus: CacheHit,
			expectedRecacheStatus:  RecacheWon,
			expectedRemainingTTL:   0,
			expectedCasId:          0,
			expectedValue:          []byte(""),
			expectedOpaque:         999999,
			expectedStale:          false,
		},
		{
			name:                   "zero length data with cas",
			memcachedResponse:      []byte("VA 0 O999999 c1231\r\n\r\n"),
			expectedMetadataStatus: CacheHit,
			expectedRecacheStatus:  RecacheNotSet,
			expectedRemainingTTL:   0,
			expectedCasId:          1231,
			expectedValue:          []byte(""),
			expectedOpaque:         999999,
			expectedStale:          false,
		},
		{
			name:                   "data with cas",
			memcachedResponse:      []byte("VA 8 O999999 c1231\r\nmemcache\r\n"),
			expectedMetadataStatus: CacheHit,
			expectedRecacheStatus:  RecacheNotSet,
			expectedRemainingTTL:   0,
			expectedCasId:          1231,
			expectedValue:          []byte("memcache"),
			expectedOpaque:         999999,
			expectedStale:          false,
		},
	}

	for _, tt := range targs {
		t.Run(tt.name, func(t *testing.T) {
			data := &bytes.Buffer{}
			writer := bufio.NewWriter(data)

			writer.Write(tt.memcachedResponse)
			writer.Flush()
			decoder := &MetaGetDecoder{}
			decoder.Reset()
			mockReader := bufio.NewReader(data)
			err := decoder.Decode(mockReader)
			assert.NoError(t, err)

			assert.Equal(t, tt.expectedMetadataStatus, decoder.Status)
			assert.Equal(t, tt.expectedRecacheStatus, decoder.Recache)
			assert.Equal(t, tt.expectedRemainingTTL, decoder.RemainingTTLSeconds)
			assert.Equal(t, tt.expectedValue, decoder.Value)
			assert.Equal(t, tt.expectedCasId, decoder.CasId)
			assert.Equal(t, tt.expectedOpaque, decoder.Opaque)
			assert.Equal(t, tt.expectedStale, decoder.Stale)
		})
	}
}

func Test_MetaGetDecoder_ErrorPath(t *testing.T) {
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

			writer.Write(tt.erroneousLine)
			writer.Flush()
			decoder := &MetaGetDecoder{}
			decoder.Reset()

			mockReader := bufio.NewReader(data)
			err := decoder.Decode(mockReader)
			assert.Error(t, err)
		})
	}
}
