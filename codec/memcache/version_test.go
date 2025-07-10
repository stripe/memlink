package memcache

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionEncode(t *testing.T) {
	encoder := &VersionEncoder{}

	data := &bytes.Buffer{}
	writer := bufio.NewWriter(data)
	assert.NoError(t, encoder.Encode(writer))

	assert.NoError(t, writer.Flush())
	assert.Equal(t, "version\r\n", data.String())
}

func TestVersionDecode(t *testing.T) {
	decoder := &VersionDecoder{}

	data := &bytes.Buffer{}
	data.Write([]byte("VERSION 1.6.9\r\n"))
	mockReader := bufio.NewReader(data)

	assert.NoError(t, decoder.Decode(mockReader))

	data.Reset()
	data.Write([]byte("unexpected response\r\n"))
	mockReader = bufio.NewReader(data)
	assert.ErrorIs(t, decoder.Decode(mockReader), errNonVersionResp)
}
