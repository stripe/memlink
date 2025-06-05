package net

import (
	"crypto/tls"
	"net"
)

type Backend struct {
	addr      net.Addr
	numConns  int
	tlsConfig *tls.Config
}

func NewBackend(addr net.Addr, numConns int, tlsConfig *tls.Config) *Backend {
	return &Backend{
		addr:      addr,
		numConns:  numConns,
		tlsConfig: tlsConfig,
	}
}

func (b *Backend) String() string {

	if b == nil {
		return "<Nil-Connection>"
	}

	return b.addr.String()
}
