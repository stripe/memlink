package memcache

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"

	"github.com/stripe/memlink/codec"
)

/*
MetaGet command format: mg <key> <flags>*\r\n

The flags used by the 'mg' command are:

- b: interpret key as base64 encoded binary value
- c: return item cas token
- f: return client flags token
- h: return whether item has been hit before as a 0 or 1
- k: return key as a token
- l: return time since item was last accessed in seconds
- O(token): opaque value, consumes a token and copies back with response
- q: use noreply semantics for return codes.
- s: return item size token
- t: return item TTL remaining in seconds (-1 for unlimited)
- u: don't bump the item in the LRU
- v: return item value in <data block>

These flags can modify the item:
- E(token): use token as new CAS value if item is modified
- N(token): vivify on miss, takes TTL as a argument
- R(token): if remaining TTL is less than token, win for recache
- T(token): update remaining TTL

These extra flags can be added to the response:
- W: client has "won" the recache flag
- X: item is stale
- Z: item has already sent a winning flag
*/
type MetaGetEncoder struct {
	Key                   string
	Base64EncodedKey      bool
	FetchCasId            bool
	FetchClientFlags      bool
	FetchItemHitBefore    bool
	FetchKey              bool
	FetchLastAccessedTime bool
	Opaque                uint64 // only non-zero value is valid
	FetchItemSizeInBytes  bool
	FetchRemainingTTL     bool
	PreventLRUBump        bool
	FetchValue            bool
	CasOverride           uint64 // only non-zero value is valid
	BlockTTL              int32  // negative values are ignored
	RecacheTTL            int32  // negative values are ignored
	UpdateTTL             int32  // negative values are ignored
}

func (e *MetaGetEncoder) Reset() {
	if e == nil {
		return
	}

	e.Key = ""
	e.Base64EncodedKey = false
	e.FetchCasId = false
	e.FetchClientFlags = false
	e.FetchItemHitBefore = false
	e.FetchKey = false
	e.FetchLastAccessedTime = false
	e.Opaque = 0
	e.FetchItemSizeInBytes = false
	e.FetchRemainingTTL = false
	e.PreventLRUBump = false
	e.FetchValue = false
	e.CasOverride = 0
	e.BlockTTL = -1
	e.RecacheTTL = -1
	e.UpdateTTL = -1
}

func (e *MetaGetEncoder) Encode(writer *bufio.Writer) error {
	b := bytePool.Get()
	defer bytePool.Put(b)
	b.Write(MetaGet)

	if keyErr := writeKey(b, e.Key); keyErr != nil {
		return keyErr
	}

	if e.Base64EncodedKey {
		b.Write(Base64EncodedKey)
	}

	if e.FetchCasId {
		b.Write(FetchCasId)
	}

	if e.FetchClientFlags {
		b.Write(FetchClientFlags)
	}

	if e.FetchItemHitBefore {
		b.Write(FetchItemHitBefore)
	}

	if e.FetchKey {
		b.Write(FetchKey)
	}

	if e.FetchLastAccessedTime {
		b.Write(FetchLastAccessedTime)
	}

	if e.FetchItemSizeInBytes {
		b.Write(FetchItemSize)
	}

	// N flag MUST come before t flag
	// mg /sloprisec/dykeyspace///abc t N100
	// HD t-1 W
	// mg /sloprisec/dykeyspace///def N100 t
	// HD t100 W
	// Same for T flag
	// mg /sloprisec/dykeyspace///def t T150
	// HD t90
	// mg /sloprisec/dykeyspace///def t T150
	// HD t145
	writeCasOverride(b, e.CasOverride)
	writeRecacheTTL(b, e.RecacheTTL)
	writeBlockTTL(b, e.BlockTTL)
	writeTTL(b, e.UpdateTTL)

	if e.FetchRemainingTTL {
		b.Write(FetchRemainingTTL)
	}

	if e.PreventLRUBump {
		b.Write(PreventLRUBump)
	}

	if e.FetchValue {
		b.Write(FetchValue)
	}

	writeOpaque(b, e.Opaque)

	b.Write(CRLF)

	_, err := writer.Write(b.Bytes())
	return err
}

type MetaGetDecoder struct {
	Status                       MetadataStatus
	Recache                      RecacheStatus
	Value                        []byte // check for nil - always
	CasId                        uint64 // only non-zero value is valid.
	RemainingTTLSeconds          int32  // only non-zero value is valid.
	ClientFlags                  uint64 // only non-zero value is valid.
	Opaque                       uint64 // only non-zero value is valid.
	IsItemHitBefore              bool
	ItemKey                      string
	ItemSizeInBytes              uint64
	TimeSinceLastAccessedSeconds uint32
	Stale                        bool

	HdrLine string
}

func (d *MetaGetDecoder) Reset() {
	if d == nil {
		return
	}

	d.Status = MetadataStatusInvalid
	d.Recache = RecacheNotSet
	d.Value = nil
	d.CasId = 0
	d.RemainingTTLSeconds = 0
	d.ClientFlags = 0
	d.Opaque = 0
	d.IsItemHitBefore = false
	d.ItemKey = ""
	d.ItemSizeInBytes = 0
	d.TimeSinceLastAccessedSeconds = 0
	d.Stale = false
	d.HdrLine = ""
}

// Decode method will parse a metaget response output correctly and load the contents of the response in
// the fields of the object itself.
// the main concern is how to return the results from the backend to the decoder, without using channels and without using
// callback functions.
func (d *MetaGetDecoder) Decode(reader *bufio.Reader) error {
	hdrLine, err := reader.ReadSlice('\n')
	if err != nil {
		return err
	}

	valueSize := -1
	for idx, elem := range bytes.Fields(hdrLine) {
		if idx == 0 {
			d.Status = MetaGetStatusFromHeader(elem)
			if d.Status == MetadataStatusInvalid {
				// If we get an unknown response code, we can't further parse the header line.
				// store it for logging and move on.
				d.HdrLine = string(hdrLine)
				return nil
			}
			continue
		}

		// in memcache protocol, all fields would start with a letter except for the value size.
		if valueSize == -1 && elem[0] >= '0' && elem[0] <= '9' {
			if v, pErr := strconv.Atoi(string(elem)); pErr != nil {
				return pErr
			} else {
				valueSize = v
			}
			continue
		}

		if len(elem) == 1 {
			switch elem[0] {
			case 'W':
				d.Recache = RecacheWon
			case 'X':
				d.Stale = true
			case 'Z':
				d.Recache = RecacheAlreadySent
			}
			continue
		}

		// otherwise split the first character and the rest of the value to convert a field to a prefix and rest of the elements
		switch elem[0] {
		case 'O':
			if o, pErr := strconv.ParseUint(string(elem[1:]), 10, 64); pErr != nil {
				return fmt.Errorf("meta_get::decoder - unable to parse opaque token as an uint64 as the token is %s: %w", elem, pErr)
			} else {
				d.Opaque = o
			}
		case 't':
			if t, pErr := strconv.ParseInt(string(elem[1:]), 10, 32); pErr != nil {
				return fmt.Errorf("meta_get::decoder - unable to parse ttl as an int32 as the token is %s: %w", elem, pErr)
			} else {
				d.RemainingTTLSeconds = int32(t)
			}
		case 'c':
			if c, pErr := strconv.ParseUint(string(elem[1:]), 10, 64); pErr != nil {
				return fmt.Errorf("meta_get::decoder - unable to parse casid as an uint64 as the token is %s: %w", elem, pErr)
			} else {
				d.CasId = c
			}
		case 'f':
			if f, pErr := strconv.ParseUint(string(elem[1:]), 10, 64); pErr != nil {
				return fmt.Errorf("meta_get::decoder - unable to parse cft as an uint64 as the token is %s: %w", elem, pErr)
			} else {
				d.ClientFlags = f
			}
		case 'h':
			if bytes.Equal(elem[1:], []byte("1")) {
				d.IsItemHitBefore = true
			}
		case 'k':
			d.ItemKey = string(elem[1:])
		case 's':
			if s, pErr := strconv.ParseUint(string(elem[1:]), 10, 64); pErr != nil {
				return fmt.Errorf("meta_get::decoder - unable to parse item size as an uint64 as the token is %s: %w", elem, pErr)
			} else {
				d.ItemSizeInBytes = s
			}
		case 'l':
			if l, pErr := strconv.ParseUint(string(elem[1:]), 10, 32); pErr != nil {
				return fmt.Errorf("meta_get::decoder - unable to parse last access as an uint64 as the token is %s: %w", elem, pErr)
			} else {
				d.TimeSinceLastAccessedSeconds = uint32(l)
			}
		}
	}

	if valueSize >= 0 {
		d.Value = make([]byte, valueSize)
		bytesRead, rfErr := io.ReadFull(reader, d.Value)
		if rfErr != nil {
			return rfErr
		}

		if bytesRead != valueSize {
			return fmt.Errorf("io.ReadFull read less than desired number of bytes. Expected to read %d bytes, but only read %d bytes", valueSize, bytesRead)
		}

		return ReadCLRF(reader)
	}

	// don't read crlf if just a header line
	return nil
}

var _ codec.LinkEncoder = (*MetaGetEncoder)(nil)
var _ codec.LinkDecoder = (*MetaGetDecoder)(nil)

type MetaGetTarget func(decoder *MetaGetDecoder, opaque uint64) error

func CreateMetaGetEncoder() *MetaGetEncoder {
	return &MetaGetEncoder{}
}

func CreateMetaGetDecoder() *MetaGetDecoder {
	return &MetaGetDecoder{}
}
