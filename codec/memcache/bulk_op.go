package memcache

import (
	"bufio"

	"github.com/hemal-shah/memlink/codec"
)

// BulkEncoder wraps multiple Encoders of type codec.LinkEncoder to encode multiple requests.
type BulkEncoder[T codec.LinkEncoder] struct {
	Encoders []T

	// Opaque works a bit differently in a bulk-get operation.
	// When the encoder is created by providing an ordered list of keys,
	// increment the global opaque counter by number of keys and assign them sequentially
	// Hence it becomes critical to instantiate the encoder and the decoders with the same list of keys
	// otherwise it wouldn't be possible to map the requests to a response.
	Opaque uint64
}

func (e *BulkEncoder[T]) Encode(writer *bufio.Writer) error {
	for _, encoder := range e.Encoders {
		if err := encoder.Encode(writer); err != nil {
			return err
		}
	}

	// add the traditional MN request
	_, err := writer.Write(NoOpRequest) // NoOpRequest contains the \r\n characters already
	return err
}

func (e *BulkEncoder[T]) Reset() {
	if e == nil {
		return
	}
	e.Encoders = e.Encoders[:0]
}

var _ codec.LinkEncoder = (*BulkEncoder[*MetaGetEncoder])(nil)

// BulkDecoder wraps multiple Decoders of type codec.LinkDecoder to decode multiple responses
type BulkDecoder[T codec.LinkDecoder] struct {
	Decoders []T

	// internal only
	OpaqueToKey map[uint64]string
}

func (d *BulkDecoder[T]) Decode(reader *bufio.Reader) error {
	for _, decoder := range d.Decoders {
		// TODO(hemal): based on a recent discovery we probably should read till the very end of the decoders
		// Though - this might end up being a no-op from the bulk operation method if the underlying single
		// key operation correctly reads the data and doesn't through unnecessary error that forces the connection to be
		// reset
		if err := decoder.Decode(reader); err != nil {
			return err
		}
	}
	return ReadMNResp(reader)
}

func (d *BulkDecoder[T]) Reset() {
	if d == nil {
		return
	}
	d.Decoders = d.Decoders[:0]
}

var _ codec.LinkDecoder = (*BulkDecoder[*MetaGetDecoder])(nil)

type BulkTarget[T codec.LinkDecoder] func(decoder *BulkDecoder[T]) error

func CreateBulkEncoder[T codec.LinkEncoder](size uint) *BulkEncoder[T] {
	return &BulkEncoder[T]{
		Encoders: make([]T, 0, size),
	}
}

func CreateBulkDecoder[T codec.LinkDecoder](size uint) *BulkDecoder[T] {
	return &BulkDecoder[T]{
		Decoders:    make([]T, 0, size),
		OpaqueToKey: make(map[uint64]string, size),
	}
}
