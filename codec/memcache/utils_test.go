package memcache

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadCLRF(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectErr bool
	}{
		{"Valid CLRF", "\r\n", false},
		{"Invalid CLRF", "xx", true},
		{"Partial CLRF", "\r", true},
		{"Empty Input", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewBufferString(tc.input))
			err := ReadCLRF(reader)
			if tc.expectErr {
				assert.Error(t, err, tc.name)
			} else {
				assert.NoError(t, err, tc.name)
			}
		})
	}
}

func TestReadMNResp(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectErr bool
	}{
		{"Valid MNResp", "MN\r\n", false},
		{"Invalid MNResp: Wrong Start", "XM\r\n", true},
		{"Invalid MNResp: Missing CLRF", "MNxx", true},
		{"Empty Input", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewBufferString(tc.input))
			err := ReadMNResp(reader)
			if tc.expectErr {
				assert.Error(t, err, tc.name)
			} else {
				assert.NoError(t, err, tc.name)
			}
		})
	}
}

func TestIsLegalMemcacheKey(t *testing.T) {
	tests := []struct {
		key        string
		isLegalKey bool
	}{
		{"validKey", true},
		{string(make([]byte, 251)), false},
		{"contain space", false},
		{"contains\tspecialchar", false},
	}

	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			assert.Equal(t, tc.isLegalKey, isLegalMemcacheKey(tc.key))
		})
	}
}

func TestWriteKey(t *testing.T) {
	var buffer bytes.Buffer
	writeKey(&buffer, "testKey")
	expected := "testKey "
	assert.Equal(t, expected, buffer.String())
}

func TestWriteOpaque(t *testing.T) {
	var buffer bytes.Buffer
	writeOpaque(&buffer, 12345)
	expected := "O12345 "
	assert.Equal(t, expected, buffer.String())
}

func TestWriteCasId(t *testing.T) {
	var buffer bytes.Buffer
	writeCasId(&buffer, 67890)
	expected := "C67890 "
	assert.Equal(t, expected, buffer.String())
}

func TestWriteTTL(t *testing.T) {
	var buffer bytes.Buffer
	writeTTL(&buffer, 3600)
	expected := "T3600 "
	assert.Equal(t, expected, buffer.String())
}

func TestWriteBlockTTL(t *testing.T) {
	var buffer bytes.Buffer
	writeBlockTTL(&buffer, 7200)
	expected := "N7200 "
	assert.Equal(t, expected, buffer.String())
}

func TestWriteRecacheTTL(t *testing.T) {
	var buffer bytes.Buffer
	writeRecacheTTL(&buffer, 1800)
	expected := "R1800 "
	assert.Equal(t, expected, buffer.String())
}
