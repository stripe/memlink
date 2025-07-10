# Memlink

Memlink is a high-performance TCP client library for sending and receiving ordered requests for memcached protocol, providing a robust and efficient way to interact with memcached servers. This can also be extended to support any tcp powered protocol.

## Features

- Generic TCP client with tls/mtls support as well
- Optimized for memcached protocol implementation
- Connection pooling and management
- High-performance message encoding/decoding
- Comprehensive test coverage

## Installation

```bash
go get github.com/stripe/memlink
```

## Quickstart

See the [example directory](./cmd/example/) for a comprehensive demonstration of how to use the memlink client with memcached instances.

## Protocol reference

For detailed information about the memcached protocol, refer to the [official documentation](https://github.com/memcached/memcached/blob/master/doc/protocol.txt).

## ⚠️ Important disclaimers

### Pending TODOs and known limitations

1. **Hash key passing**: The connection pool currently uses an empty key for key hashing - i.e. it randomly assigns the key to a backend. If this is not desirable, you should consider forwarding the request to the right backend from either the server side or wait for the change to be officially supported.

2. **Bulk operation fan-out**: Similar to above, all the pipelined bulk requests will land on the same backend. Work is in progress to spread that out over the appropriate backend once #1 is completed. 

3. **Extra Go routines**: Memlink creates 3 new goroutines per backend -- if you have `M` backends and connection pool size of `N`, then expect `M*N*3` new goroutines created by using memlink, 1 of them being mostly quiet. 

4. **Safe closing**: Memlink doesn't actively monitor health of backends and remove them from rotation, so its users responsibility to safely remove the backend from connection pool if its no longer healthy / going to be replaced.

### Memcached protocol considerations

This implementation follows the memcached meta protocol, but there are important considerations:

1. **Protocol compliance**: The implementation is based on memcached meta protocol specifications, but may not cover all edge cases or newer protocol features.

2. **Field ordering**: The protocol has specific requirements about field ordering in requests (e.g., TTL must come before BlockTTL in arithmetic operations).

3. **Protocol evolution**: Memcached protocol may evolve, and this implementation may need updates to support newer features or changes.

**Note**: Always test thoroughly with your specific memcached version and configuration before using in production.
