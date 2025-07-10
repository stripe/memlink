package memcache

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"

	"github.com/stripe/memlink/codec"
)

/*
MetaDelete command format: md <key> <flags>*\r\n

The flags used by the 'md' command are:

- b: interpret key as base64 encoded binary value (see metaget)
- C(token): compare CAS value
- E(token): use token as new CAS value (see metaget for detail)
- I: invalidate. mark as stale, bumps CAS.
- k: return key
- O(token): opaque to copy back.
- q: no-reply
- T(token): updates TTL, only when paired with the 'I' flag
- x: removes the item value, but leaves the item.
*/
type MetaDeleteEncoder struct {
	Key              string
	Base64EncodedKey bool
	CasId            uint64 // only non-zero value is valid
	CasOverride      uint64 // only non-zero value is valid.
	Invalidate       bool
	FetchKey         bool
	Opaque           uint64 // only non-zero value is valid
	TTL              int32  // negative values are ignored.
	ClientFlags      uint64 // only non-zero value is valid.
	RemoveValue      bool
}

func (e *MetaDeleteEncoder) Encode(writer *bufio.Writer) error {
	b := bytePool.Get()
	defer bytePool.Put(b)
	b.Write(MetaDelete)

	if keyErr := writeKey(b, e.Key); keyErr != nil {
		return keyErr
	}

	if e.Base64EncodedKey {
		b.Write(Base64EncodedKey)
	}

	if e.Invalidate {
		b.Write(Invalidate)
	}

	if e.FetchKey {
		b.Write(FetchKey)
	}

	if e.RemoveValue {
		b.Write(RemoveValue)
	}

	writeCasId(b, e.CasId)
	writeCasOverride(b, e.CasOverride)
	writeTTL(b, e.TTL)
	writeClientFlags(b, e.ClientFlags)
	writeOpaque(b, e.Opaque)

	b.Write(CRLF)

	_, err := writer.Write(b.Bytes())
	return err
}

func (e *MetaDeleteEncoder) Reset() {
	if e == nil {
		return
	}
	e.Key = ""
	e.Base64EncodedKey = false
	e.CasId = 0
	e.CasOverride = 0
	e.Invalidate = false
	e.FetchKey = false
	e.Opaque = 0
	e.TTL = -1
	e.ClientFlags = 0
	e.RemoveValue = false
}

type MetaDeleteDecoder struct {
	Status  MetadataStatus
	Opaque  uint64
	ItemKey string

	HdrLine string
}

func (d *MetaDeleteDecoder) Decode(reader *bufio.Reader) error {
	hdrLine, err := reader.ReadSlice('\n')
	if err != nil {
		return err
	}

	for idx, elem := range bytes.Fields(hdrLine) {
		if idx == 0 {
			d.Status = MetaDeleteStatusFromHeader(elem)
			if d.Status == MetadataStatusInvalid {
				// If we get an unknown response code, we can't further parse the header line.
				// store it for logging and move on.
				d.HdrLine = string(hdrLine)
				return nil
			}
			continue
		}

		switch elem[0] {
		case 'O':
			if o, pErr := strconv.ParseUint(string(elem[1:]), 10, 64); pErr != nil {
				return fmt.Errorf("meta_delete::decoder - unable to parse opaque token as an uint64 as the token is %s: %w", elem, pErr)
			} else {
				d.Opaque = o
			}
		case 'k':
			d.ItemKey = string(elem[1:])
		}
	}

	// dont read crlf at the end
	return nil
}

func (d *MetaDeleteDecoder) Reset() {
	if d == nil {
		return
	}
	d.Status = MetadataStatusInvalid
	d.Opaque = 0
	d.ItemKey = ""
	d.HdrLine = ""
}

var _ codec.LinkEncoder = (*MetaDeleteEncoder)(nil)
var _ codec.LinkDecoder = (*MetaDeleteDecoder)(nil)

type MetaDeleteTarget func(decoder *MetaDeleteDecoder, opaque uint64) error

func CreateMetaDeleteEncoder() *MetaDeleteEncoder {
	return &MetaDeleteEncoder{}
}

func CreateMetaDeleteDecoder() *MetaDeleteDecoder {
	return &MetaDeleteDecoder{}
}
