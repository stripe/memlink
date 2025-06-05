package codec

import (
	"bufio"

	"github.com/hemal-shah/memlink/internal"
)

// LinkEncoder allows you to convert a request to a network request.
// You can have multiple LinkEncoders chained together to perform a batch operation.
// You can use any approach to chain together the requests and write it to the buffer.
// For a bulk request, one can even create a temporary buffer and call into multiple requests
// and then call into the provided buffer.
type LinkEncoder interface {
	Encode(writer *bufio.Writer) error
	internal.Resettable
}

type LinkDecoder interface {
	Decode(reader *bufio.Reader) error
	internal.Resettable
}

type Link interface {
	Encoder() LinkEncoder
	Decoder() LinkDecoder

	// Done() closes the channel when the Link has executed completely.
	// i.e. the encoder has been used to submit a request to underlying connection and the decoder has processed the
	// response. Clients can select over this channel or other timers to provide a timeout.
	Done() <-chan struct{}

	// Complete should be called into by the underlying layer whenever the usage of the Link is completed.
	// i.e. the Encoder and the Decoder were both called into with appropriate connection and they both returned
	// control back to main thread. If an error happens, pass that in.
	Complete(err error)

	Err() error
}

// Chain allows scheduling an Link in a FIFO manner.
type Chain interface {
	Append(link Link) error
}

type GenericLink struct {
	e    LinkEncoder
	d    LinkDecoder
	err  error
	done chan struct{}
}

func (g *GenericLink) Err() error {
	return g.err
}

func (g *GenericLink) Encoder() LinkEncoder {
	return g.e
}

func (g *GenericLink) Decoder() LinkDecoder {
	return g.d
}

func (g *GenericLink) Done() <-chan struct{} {
	return g.done
}

func (g *GenericLink) Complete(err error) {
	g.err = err
	close(g.done)
}

var _ Link = (*GenericLink)(nil)

func NewGenericLink(e LinkEncoder, d LinkDecoder) Link {
	return &GenericLink{
		e:    e,
		d:    d,
		err:  nil,
		done: make(chan struct{}),
	}
}
