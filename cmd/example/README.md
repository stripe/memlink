# Memcached Client Example

This directory contains a comprehensive example demonstrating how to use the memlink client with memcached instances.

## Prerequisites

- Go 1.21 or later
- memcached installed and available in your PATH

## Starting Memcached Instances

Before running the example, you need to start memcached instances on different ports. The example is configured to connect to:

- `localhost:11211`
- `localhost:11212` 
- `localhost:11213`

### Option 1: Start memcached instances manually

Open separate terminal windows and run:

```bash
# Terminal 1 - Start memcached on port 11211
memcached -p 11211 -d

# Terminal 2 - Start memcached on port 11212  
memcached -p 11212 -d

# Terminal 3 - Start memcached on port 11213
memcached -p 11213 -d
```

### Option 2: Start all instances with a script

Create a script to start all instances:

```bash
#!/bin/bash
# start_memcached.sh

echo "Starting memcached instances..."

# Start memcached on port 11211
memcached -p 11211 -d
echo "Started memcached on port 11211"

# Start memcached on port 11212
memcached -p 11212 -d
echo "Started memcached on port 11212"

# Start memcached on port 11213
memcached -p 11213 -d
echo "Started memcached on port 11213"

echo "All memcached instances started!"
echo "You can now run: go run ./cmd/example/"
```

Make it executable and run:
```bash
chmod +x start_memcached.sh
./start_memcached.sh
```

## Running the Example

Once memcached instances are running, you can run the example:

```bash
# From the project root directory
go run ./cmd/example/
```

**Important**: Use `go run ./cmd/example/` (with trailing slash) to include both `main.go` and `client.go` files.

## TLS Support

The current example uses plain TCP connections. To add TLS support, you can modify the client implementation in several ways:

### Option 1: Modify the Backend Creation

Update the `NewClient` function in `client.go` to support TLS:

```go
// TLSConfig holds TLS configuration
type TLSConfig struct {
    CertFile   string
    KeyFile    string
    CAFile     string
    ServerName string
    Insecure   bool // Skip certificate verification
}

// NewClient creates a new memcached client with optional TLS support
func NewClient(addresses []string, numConnsPerBackend int, tlsConfig *TLSConfig, opts ...ClientOption) (MemcachedClient, error) {
    if len(addresses) == 0 {
        return nil, fmt.Errorf("at least one address must be provided")
    }

    // Parse addresses and create backends
    backends := make([]*netpkg.Backend, 0, len(addresses))
    for _, addr := range addresses {
        tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
        if err != nil {
            return nil, fmt.Errorf("invalid address %s: %w", addr, err)
        }
        
        // Create backend with TLS configuration
        backend := netpkg.NewBackend(tcpAddr, numConnsPerBackend, tlsConfig)
        backends = append(backends, backend)
    }

    // Create connection pool
    poolOpts := []netpkg.ConnPoolOptions{
        netpkg.WithConnPoolLogger(zap.NewNop()),
    }

    pool, err := netpkg.NewConnPool(backends, poolOpts...)
    if err != nil {
        return nil, fmt.Errorf("failed to create connection pool: %w", err)
    }

    client := &memcachedClient{
        pool:   pool,
        logger: zap.NewNop(),
    }

    // Apply client options
    for _, opt := range opts {
        opt(client)
    }

    return client, nil
}
```

### Option 2: Add TLS Client Option

Create a TLS client option:

```go
// WithTLS sets TLS configuration for the client
func WithTLS(tlsConfig *TLSConfig) ClientOption {
    return func(c *memcachedClient) {
        c.tlsConfig = tlsConfig
    }
}

// Update memcachedClient struct
type memcachedClient struct {
    pool      netpkg.TCPConnPool
    logger    *zap.Logger
    tlsConfig *TLSConfig
}
```

### Option 3: Modify Connection Dialing

Update the connection dialing logic in the internal network package to support TLS:

```go
// In internal/net/tcp_conn.go or similar
func (c *tcpConn) dialWithTLS(tlsConfig *TLSConfig) error {
    // Create TCP connection first
    conn, err := net.DialTimeout("tcp", c.addr.String(), c.dialTimeout)
    if err != nil {
        return err
    }

    // Upgrade to TLS if configured
    if tlsConfig != nil {
        tlsConn := tls.Client(conn, &tls.Config{
            ServerName:         tlsConfig.ServerName,
            InsecureSkipVerify: tlsConfig.Insecure,
            // Add other TLS configuration as needed
        })
        
        if err := tlsConn.Handshake(); err != nil {
            conn.Close()
            return fmt.Errorf("TLS handshake failed: %w", err)
        }
        
        c.conn = tlsConn
    } else {
        c.conn = conn
    }

    return nil
}
```

### Usage Example with TLS

```go
// Create TLS configuration
tlsConfig := &TLSConfig{
    ServerName: "memcached.example.com",
    Insecure:   false, // Set to true for self-signed certificates
    // CertFile: "/path/to/client.crt",
    // KeyFile:  "/path/to/client.key",
    // CAFile:   "/path/to/ca.crt",
}

// Create client with TLS
client, err := NewClient(
    []string{"memcached.example.com:11211"},
    3,
    tlsConfig,
    WithLogger(logger),
)
if err != nil {
    log.Fatalf("Failed to create TLS client: %v", err)
}
defer client.Close()
```

### Starting Memcached with TLS

To test TLS connections, you'll need to start memcached with TLS support:

```bash
# Generate certificates (for testing)
openssl req -x509 -newkey rsa:2048 -keyout memcached.key -out memcached.crt -days 365 -nodes

# Start memcached with TLS
memcached -p 11211 -Z -o ssl_chain_cert=memcached.crt -o ssl_key=memcached.key -d
```


## What the Example Demonstrates

The example includes several demonstrations:

### 1. Basic Operations
- **MetaSet**: Setting values with TTL
- **MetaGet**: Retrieving values with metadata
- **MetaIncrement/MetaDecrement**: Arithmetic operations
- **MetaDelete**: Deleting keys

### 2. Advanced Features
- **CAS (Compare-And-Swap)**: Optimistic concurrency control
- **Client Flags**: Custom metadata storage
- **Metadata Retrieval**: TTL, CAS ID, item size, last access time

### 3. Context and Timeouts
- **Context with timeout**: Demonstrates timeout handling
- **Cancellation**: Shows how to cancel operations

### 4. Bulk Operations
- **Multiple operations**: Setting, getting, and deleting multiple keys
- **Bulk Get**: Efficiently retrieving multiple keys in a single request
- **Opaque value management**: Proper request/response correlation

### 5. Object Pooling
- **Resettable pools**: Efficient memory management for encoders/decoders
- **Automatic cleanup**: Objects returned to pools after use

## Example Output

When running successfully, you should see output similar to:

```
2025/06/30 15:12:48 Running Memcached Client Examples...
2025/06/30 15:12:48 
=== Running Basic Example ===
=== Setting a value ===
✓ Value set successfully (Status: Stored)

=== Getting a value ===
✓ Value retrieved: hello world
  Status: CacheHit
  CAS ID: 16
  TTL: 60 seconds
  Is cache miss: false

=== Incrementing a counter ===
✓ Counter incremented to: 2
  Status: Stored

=== Decrementing a counter ===
✓ Counter decremented to: 1
  Status: Stored

=== Deleting a key ===
✓ Key deleted successfully (Status: Deleted)

=== Getting a deleted key ===
✓ Get result for deleted key:
  Status: CacheHit
  Is cache miss: false

=== Running Advanced Features Example ===
=== Setting with CAS and client flags ===
✓ Advanced set completed (Status: NotStored)

=== Getting with all metadata ===
✓ Advanced get completed:
  Value: advanced value
  Status: CacheHit
  CAS ID: 2
  Client Flags: 42
  Item Size: 14 bytes
  TTL: -1 seconds
  Last Access: 0 seconds ago

=== Running Context Example ===
Operation completed within timeout

=== Running Bulk Operations Example ===
=== Setting multiple values ===
✓ Set key1 = value1 (Status: Stored)
✓ Set key2 = value2 (Status: Stored)
✓ Set key3 = value3 (Status: Stored)
✓ Set key4 = value4 (Status: Stored)
✓ Set key5 = value5 (Status: Stored)

=== Getting multiple values ===
✓ key1 =  (Status: CacheMiss)
✓ key2 = value2 (Status: CacheHit)
✓ key3 = value3 (Status: CacheHit)
✓ key4 = value3 (Status: CacheMiss)
✓ key5 = value3 (Status: CacheMiss)

=== Cleaning up ===
✓ Deleted key1 (Status: NotFound)
✓ Deleted key2 (Status: NotFound)
✓ Deleted key3 (Status: NotFound)
✓ Deleted key4 (Status: Deleted)
✓ Deleted key5 (Status: NotFound)

=== Running Bulk Get Example ===
=== Setting values for bulk retrieval ===
✓ Set bulk_key1 = bulk_value1 (Status: Stored)
✓ Set bulk_key2 = bulk_value2 (Status: Stored)
✓ Set bulk_key3 = bulk_value3 (Status: Stored)
✓ Set bulk_key4 = bulk_value4 (Status: Stored)
✓ Set bulk_key5 = bulk_value5 (Status: Stored)

=== Bulk Get Results ===
bulk_key1 =  (Status: CacheMiss, Opaque: 1000)
bulk_key2 =  (Status: CacheMiss, Opaque: 1001)
bulk_key3 = bulk_value3 (Status: CacheHit, Opaque: 1002)
bulk_key4 =  (Status: CacheMiss, Opaque: 1003)
bulk_key5 = bulk_value5 (Status: CacheHit, Opaque: 1004)

All examples completed!
```

## Troubleshooting

### Common Issues

1. **"connection refused" errors**
   - Ensure memcached instances are running on the correct ports
   - Check if ports are already in use by other processes

2. **"undefined: NewClient" errors**
   - Make sure to run `go run ./cmd/example/` (with trailing slash)
   - This ensures both `main.go` and `client.go` are compiled together

### Stopping Memcached Instances

To stop the memcached instances:

```bash
# Find and kill memcached processes
pkill memcached
```

## Files in this Directory

- `main.go`: Main example demonstrating all client features
- `client.go`: MemcachedClient implementation with connection pooling
- `README.md`: This documentation file