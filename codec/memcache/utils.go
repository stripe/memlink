package memcache

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
)

// Helper method whenever there's need to read and discard 2 bytes worth of data.
// raises an error if the next 2 bytes aren't \r\n
func ReadCLRF(reader *bufio.Reader) error {
	cr, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if cr != '\r' {
		return fmt.Errorf("expected to read CR but instead got %c", cr)
	}

	nl, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if nl != '\n' {
		return fmt.Errorf("expected to read NL but instead got %c", nl)
	}
	return nil
}

// Read the Meta No Op response including the \r\n response.
func ReadMNResp(reader *bufio.Reader) error {
	// according to the protocol, the meta no-op response should be always the "MN\r\n" bytes
	// and opaque tokens are not provided here. the next 4 bytes from the reader needs to be exactly the list above.
	m, err := reader.ReadByte()
	if err != nil {
		return err
	}

	if m != 'M' {
		return fmt.Errorf("expected to read M character but got %c", m)
	}

	n, err := reader.ReadByte()
	if err != nil {
		return err
	}

	if n != 'N' {
		return fmt.Errorf("expected to read N character but got %c", m)
	}

	return ReadCLRF(reader)
}

func isLegalMemcacheKey(key string) bool {
	if len(key) > 250 {
		return false
	}

	for _, c := range key {
		if c <= ' ' || c == 0x7f {
			return false
		}
	}

	return true
}

func writeKey(b *bytes.Buffer, key string) error {
	if !isLegalMemcacheKey(key) {
		return fmt.Errorf("%q is an invalid key in memcache", key)
	}
	b.WriteString(key)
	b.WriteByte(Space)
	return nil
}

func writeOpaque(b *bytes.Buffer, opaque uint64) {
	if opaque != 0 {
		b.WriteByte(Opaque)
		b.Write(strconv.AppendUint(b.AvailableBuffer(), opaque, 10))
		b.WriteByte(Space)
	}
}

func writeCasId(b *bytes.Buffer, casId uint64) {
	if casId != 0 {
		b.WriteByte(CasId)
		b.Write(strconv.AppendUint(b.AvailableBuffer(), casId, 10))
		b.WriteByte(Space)
	}
}

func writeCasOverride(b *bytes.Buffer, override uint64) {
	if override != 0 {
		b.WriteByte(CasOverride)
		b.Write(strconv.AppendUint(b.AvailableBuffer(), override, 10))
		b.WriteByte(Space)
	}
}

func writeClientFlags(b *bytes.Buffer, clientFlags uint64) {
	if clientFlags > 0 {
		b.WriteByte(ClientFlagsToken)
		b.Write(strconv.AppendUint(b.AvailableBuffer(), clientFlags, 10))
		b.WriteByte(Space)
	}
}

func writeTTL(b *bytes.Buffer, ttl int32) {
	if ttl >= 0 {
		b.WriteByte(TTL)
		b.Write(strconv.AppendInt(b.AvailableBuffer(), int64(ttl), 10))
		b.WriteByte(Space)
	}
}

func writeBlockTTL(b *bytes.Buffer, blockTTL int32) {
	if blockTTL >= 0 {
		b.WriteByte(BlockTTL)
		b.Write(strconv.AppendInt(b.AvailableBuffer(), int64(blockTTL), 10))
		b.WriteByte(Space)
	}
}

func writeRecacheTTL(b *bytes.Buffer, recacheTTL int32) {
	if recacheTTL >= 0 {
		b.WriteByte(RecacheTTL)
		b.Write(strconv.AppendInt(b.AvailableBuffer(), int64(recacheTTL), 10))
		b.WriteByte(Space)
	}
}

func writeInitialValue(b *bytes.Buffer, initialValue uint64) {
	if initialValue != 0 {
		b.WriteByte(InitialValue)
		b.Write(strconv.AppendUint(b.AvailableBuffer(), initialValue, 10))
		b.WriteByte(Space)
	}
}

func writeDelta(b *bytes.Buffer, delta uint64) {
	b.WriteByte(Delta)
	b.Write(strconv.AppendUint(b.AvailableBuffer(), delta, 10))
	b.WriteByte(Space)
}
