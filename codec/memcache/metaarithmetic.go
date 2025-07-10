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
MetaArithmetic command format: ma <key> <flags>*\r\n

The flags used by the 'ma' command are:

- b: interpret key as base64 encoded binary value (see metaget)
- C(token): compare CAS value (see mset)
- E(token): use token as new CAS value (see metaget for detail)
- N(token): auto create item on miss with supplied TTL
- J(token): initial value to use if auto created after miss (default 0)
- D(token): delta to apply (decimal unsigned 64-bit number, default 1)
- T(token): update TTL on success
- M(token): mode switch to change between incr and decr modes.
- O(token): opaque value, consumes a token and copies back with response
- q: use no-reply semantics for return codes (see details under mset)
- t: return current TTL
- c: return current CAS value if successful.
- v: return new value
- k: return key as a token
*/
type MetaArithmeticEncoder struct {
	Key               string
	Base64EncodedKey  bool
	CasId             uint64 // only non-zero value is valid
	CasOverride       uint64 // only non-zero value is valid
	BlockTTL          int32  // negative values are ignored.
	InitialValue      uint64 // only non-zero value is valid
	Delta             uint64 // all range values are valid
	TTL               int32  // negative values are ignored.
	Decrement         bool   // increment is the default operation, set true for decrement
	Opaque            uint64 // only non-zero value is valid
	FetchRemainingTTL bool
	FetchCasId        bool
	FetchValue        bool
	FetchKey          bool
}

func (e *MetaArithmeticEncoder) Encode(writer *bufio.Writer) error {
	b := bytePool.Get()
	defer bytePool.Put(b)
	b.Write(MetaArithmetic)

	if keyErr := writeKey(b, e.Key); keyErr != nil {
		return keyErr
	}

	if e.Base64EncodedKey {
		b.Write(Base64EncodedKey)
	}

	if e.Decrement {
		b.Write(DecrementMode)
	}

	if e.FetchRemainingTTL {
		b.Write(FetchRemainingTTL)
	}

	if e.FetchCasId {
		b.Write(FetchCasId)
	}

	if e.FetchValue {
		b.Write(FetchValue)
	}

	if e.FetchKey {
		b.Write(FetchKey)
	}

	writeCasId(b, e.CasId)
	writeCasOverride(b, e.CasOverride)
	// TTL MUST come before BlockTTL. See this example:
	// ma /slo/dykeyspace///dytest3 T150 N100 J123 D1
	// HD
	// mg /slo/dykeyspace///dytest3 v t
	// VA 3 t97
	// 123
	// ma /slo/dykeyspace///dytest4 N100 T150 J123 D1
	// HD
	// mg /slo/dykeyspace///dytest4 v t
	// VA 3 t148
	// 123
	writeTTL(b, e.TTL)
	writeBlockTTL(b, e.BlockTTL)
	writeInitialValue(b, e.InitialValue)
	writeDelta(b, e.Delta)
	writeOpaque(b, e.Opaque)

	b.Write(CRLF)

	_, err := writer.Write(b.Bytes())
	return err
}

func (e *MetaArithmeticEncoder) Reset() {
	e.Key = ""
	e.Base64EncodedKey = false
	e.CasId = 0
	e.CasOverride = 0
	e.BlockTTL = -1
	e.InitialValue = 0
	e.Delta = 0
	e.TTL = -1
	e.Decrement = false
	e.Opaque = 0
	e.FetchRemainingTTL = false
	e.FetchValue = false
	e.FetchCasId = false
	e.FetchKey = false
}

type MetaArithmeticDecoder struct {
	Status              MetadataStatus
	Opaque              uint64
	RemainingTTLSeconds int32 // only non-zero value is valid.
	Value               []byte
	ValueUInt64         uint64 // just a parsed value from the Value above.
	CasId               uint64 // only non-zero value is valid.
	ItemKey             string

	HdrLine string
}

func (d *MetaArithmeticDecoder) Decode(reader *bufio.Reader) error {
	hdrLine, err := reader.ReadSlice('\n')
	if err != nil {
		return err
	}

	valueSize := -1
	for idx, elem := range bytes.Fields(hdrLine) {
		if idx == 0 {
			d.Status = ArithmeticStatusFromHeader(elem)
			if d.Status == MetadataStatusInvalid {
				// If we get an unknown response code, we can't further parse the header line.
				// store it for logging and move on.
				d.HdrLine = string(hdrLine)
				return nil
			}
			continue
		}

		if valueSize == -1 && elem[0] >= '0' && elem[0] <= '9' {
			if v, pErr := strconv.Atoi(string(elem)); pErr != nil {
				return pErr
			} else {
				valueSize = v
			}
			continue
		}

		// otherwise split the first character and the rest of the value to convert a field to a prefix and rest of the elements
		switch elem[0] {
		case 'O':
			if o, pErr := strconv.ParseUint(string(elem[1:]), 10, 64); pErr != nil {
				return fmt.Errorf("meta_arithmetic::decoder - unable to parse opaque token as an uint64 as the token is %s: %w", elem, pErr)
			} else {
				d.Opaque = o
			}
		case 't':
			if t, pErr := strconv.ParseInt(string(elem[1:]), 10, 32); pErr != nil {
				return fmt.Errorf("meta_arithmetic::decoder - unable to parse ttl as an int32 as the token is %s: %w", elem, pErr)
			} else {
				d.RemainingTTLSeconds = int32(t)
			}
		case 'c':
			if c, pErr := strconv.ParseUint(string(elem[1:]), 10, 64); pErr != nil {
				return fmt.Errorf("meta_arithmetic::decoder - unable to parse cas id as an uint64 as the token is %s: %w", elem, pErr)
			} else {
				d.CasId = c
			}
		case 'k':
			d.ItemKey = string(elem[1:])
		}
	}

	if valueSize >= 0 {
		d.Value = make([]byte, valueSize)
		bytesRead, fullReadErr := io.ReadFull(reader, d.Value)
		if fullReadErr != nil {
			return fullReadErr
		}

		if bytesRead != valueSize {
			return fmt.Errorf("io.ReadFull read less than desired number of bytes. Expected to read %d bytes, but only read %d bytes", valueSize, bytesRead)
		}

		// convert the bytes into counters
		value, convertErr := strconv.ParseUint(string(d.Value), 10, 64)
		if convertErr != nil {
			return convertErr
		}
		d.ValueUInt64 = value
		return ReadCLRF(reader)
	}

	// don't read crlf if just the header line
	return nil
}

func (d *MetaArithmeticDecoder) Reset() {
	if d == nil {
		return
	}

	d.Status = MetadataStatusInvalid
	d.Opaque = 0
	d.RemainingTTLSeconds = 0
	d.Value = nil
	d.ValueUInt64 = 0
	d.CasId = 0
	d.ItemKey = ""
	d.HdrLine = ""
}

var _ codec.LinkEncoder = (*MetaArithmeticEncoder)(nil)
var _ codec.LinkDecoder = (*MetaArithmeticDecoder)(nil)

type MetaArithmeticTarget func(decoder *MetaArithmeticDecoder, opaque uint64) error

func CreateArithmeticEncoder() *MetaArithmeticEncoder {
	return &MetaArithmeticEncoder{}
}

func CreateArithmeticDecoder() *MetaArithmeticDecoder {
	return &MetaArithmeticDecoder{}
}
