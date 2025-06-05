package net

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
)

type TcpDialErr struct {
	Addr net.Addr
}

func (c *TcpDialErr) Error() string {
	return fmt.Sprintf("error dialing connection to address: %s", c.Addr.String())
}

type contextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

func dial(ctx context.Context, addr net.Addr, tlsConfig *tls.Config) (net.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	netDialer := &net.Dialer{
		Timeout: dialTimeout,
	}

	var dialer contextDialer = netDialer
	if tlsConfig != nil {
		dialer = &tls.Dialer{
			NetDialer: netDialer,
			Config:    tlsConfig,
		}
	}

	mcConn, err := dialer.DialContext(dialCtx, addr.Network(), addr.String())
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return nil, &TcpDialErr{addr}
	}
	if err != nil {
		return nil, err
	}
	return mcConn, nil
}
