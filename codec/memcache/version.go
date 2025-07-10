package memcache

import (
	"bufio"
	"errors"
	"strings"

	"github.com/stripe/memlink/codec"
)

var errNonVersionResp = errors.New("expected VERSION prefix in response")

type VersionEncoder struct{}

func (e *VersionEncoder) Encode(writer *bufio.Writer) error {
	b := bytePool.Get()
	defer bytePool.Put(b)

	b.Write(Version)
	b.Write(CRLF)

	_, err := writer.Write(b.Bytes())
	return err
}

func (e *VersionEncoder) Reset() {
}

type VersionDecoder struct {
	HdrLine string
}

func (d *VersionDecoder) Decode(reader *bufio.Reader) error {
	hdrLine, err := reader.ReadString('\n')
	if err != nil {
		return err
	}

	d.HdrLine = hdrLine
	if !strings.HasPrefix(hdrLine, "VERSION") {
		// Ideally we don't return errors here, let the User of the API take care of any mismatch
		// or internal issue, but since VERSION command can't support opaque tokens, we have to break up the connection
		// if its not starting with VERSION
		return errNonVersionResp
	}

	return nil
}

func (d *VersionDecoder) Reset() {
	d.HdrLine = ""
}

var _ codec.LinkEncoder = (*VersionEncoder)(nil)
var _ codec.LinkDecoder = (*VersionDecoder)(nil)

func CreateVersionEncoder() *VersionEncoder {
	return &VersionEncoder{}
}

func CreateVersionDecoder() *VersionDecoder {
	return &VersionDecoder{}
}
