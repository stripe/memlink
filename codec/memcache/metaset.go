package memcache

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"

	"github.com/hemal-shah/memlink/codec"
)

// MetaSetMode represents the mode for a meta set operation
type MetaSetMode string

const (
	// Add mode - only set if key doesn't exist
	Add MetaSetMode = "add"
	// Replace mode - only set if key exists
	Replace MetaSetMode = "replace"
	// Append mode - append to existing value
	Append MetaSetMode = "append"
	// Prepend mode - prepend to existing value
	Prepend MetaSetMode = "prepend"
)

/*
MetaSet command format:

	ms <key> <datalen> <flags>*\r\n
	<data block>\r\n

The flags used by the 'ms' command are:

- b: interpret key as base64 encoded binary value (see metaget)
- c: return CAS value if successfully stored.
- C(token): compare CAS value when storing item
- E(token): use token as new CAS value (see metaget for detail)
- F(token): set client flags to token (32 bit unsigned numeric)
- I: invalidate. set-to-invalid if supplied CAS is older than item's CAS
- k: return key as a token
- O(token): opaque value, consumes a token and copies back with response
- q: use no-reply semantics for return codes
- s: return the size of the stored item on success (ie; new size on append)
- T(token): Time-To-Live for item, see "Expiration" above.
- M(token): mode switch to change behavior to add, replace, append, prepend
- N(token): if in append mode, auto vivify on miss with supplied TTL
*/
type MetaSetEncoder struct {
	Key              string
	Value            []byte
	Base64EncodedKey bool
	FetchCasId       bool
	CasId            uint64 // only non-zero value is valid.
	CasOverride      uint64 // only non-zero value is valid.
	ClientFlags      uint64 // only non-zero value is valid.
	Invalidate       bool
	FetchKey         bool
	FetchItemSize    bool
	TTL              int32  // negative values are ignored.
	Opaque           uint64 // only non-zero value is valid.
	Mode             MetaSetMode
	BlockTTL         int32 // negative values are ignored.
}

// todo(hemal): figure out a way to pre-calculate the request bytes so that the request is not generated
// when trying to write to a connection
func (e *MetaSetEncoder) Encode(writer *bufio.Writer) error {
	b := bytePool.Get()
	defer bytePool.Put(b)
	b.Write(MetaSet)

	if keyErr := writeKey(b, e.Key); keyErr != nil {
		return keyErr
	}

	b.Write(strconv.AppendInt(b.AvailableBuffer(), int64(len(e.Value)), 10))
	b.WriteByte(Space)

	if e.Base64EncodedKey {
		b.Write(Base64EncodedKey)
	}

	if e.FetchCasId {
		b.Write(FetchCasId)
	}

	if e.Invalidate {
		b.Write(Invalidate)
	}

	if e.FetchKey {
		b.Write(FetchKey)
	}

	if e.FetchItemSize {
		b.Write(FetchItemSize)
	}

	switch e.Mode {
	case Add:
		b.Write(PutIfAbsentMode)
	case Append:
		b.Write(AppendMode)
	case Prepend:
		b.Write(PrependMode)
	case Replace:
		b.Write(ReplaceMode)
	default:
		// do nothing - defaults to normal set mode
	}

	writeTTL(b, e.TTL)
	writeCasId(b, e.CasId)
	writeCasOverride(b, e.CasOverride)
	writeClientFlags(b, e.ClientFlags)
	writeBlockTTL(b, e.BlockTTL)
	writeOpaque(b, e.Opaque)

	b.Write(CRLF)
	b.Write(e.Value)
	b.Write(CRLF)

	_, err := writer.Write(b.Bytes())
	return err
}

func (e *MetaSetEncoder) Reset() {
	if e == nil {
		return
	}

	e.Key = ""
	e.Value = nil
	e.Base64EncodedKey = false
	e.FetchCasId = false
	e.CasId = 0
	e.CasOverride = 0
	e.ClientFlags = 0
	e.Invalidate = false
	e.FetchKey = false
	e.FetchItemSize = false
	e.TTL = -1
	e.Opaque = 0
	e.Mode = ""
	e.BlockTTL = -1
}

type MetaSetDecoder struct {
	Status  MetadataStatus
	Opaque  uint64
	CasId   uint64
	ItemKey string

	HdrLine string
}

func (d *MetaSetDecoder) Decode(reader *bufio.Reader) error {
	hdrLine, err := reader.ReadSlice('\n')
	if err != nil {
		return err
	}

	for idx, elem := range bytes.Fields(hdrLine) {
		if idx == 0 {
			d.Status = MetaSetStatusFromHeader(elem)
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
				return fmt.Errorf("meta_set::decoder - unable to parse opaque token as an uint64 as the token is %s: %w", elem, pErr)
			} else {
				d.Opaque = o
			}
		case 'c':
			if c, pErr := strconv.ParseUint(string(elem[1:]), 10, 64); pErr != nil {
				return fmt.Errorf("meta_set::decoder - unable to parse cas id as an uint64 as the token is %s: %w", elem, pErr)
			} else {
				d.CasId = c
			}
		case 'k':
			d.ItemKey = string(elem[1:])
		}
	}

	// dont read crlf at the end
	return nil
}

func (d *MetaSetDecoder) Reset() {
	if d == nil {
		return
	}

	d.Status = MetadataStatusInvalid
	d.Opaque = 0
	d.CasId = 0
	d.ItemKey = ""
	d.HdrLine = ""
}

var _ codec.LinkEncoder = (*MetaSetEncoder)(nil)
var _ codec.LinkDecoder = (*MetaSetDecoder)(nil)

type MetaSetTarget func(decoder *MetaSetDecoder, opaque uint64) error

func CreateMetaSetEncoder() *MetaSetEncoder {
	return &MetaSetEncoder{}
}

func CreateMetaSetDecoder() *MetaSetDecoder {
	return &MetaSetDecoder{}
}
