package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hemal-shah/memlink/codec/memcache"
	"github.com/hemal-shah/memlink/internal/pools"
	"go.uber.org/zap"
)

// Global resettable pools for encoders and decoders
var (
	// Encoder pools
	setEncoderPool        = pools.NewResettablePool(func() *memcache.MetaSetEncoder { return memcache.CreateMetaSetEncoder() })
	getEncoderPool        = pools.NewResettablePool(func() *memcache.MetaGetEncoder { return memcache.CreateMetaGetEncoder() })
	arithmeticEncoderPool = pools.NewResettablePool(func() *memcache.MetaArithmeticEncoder { return memcache.CreateArithmeticEncoder() })
	deleteEncoderPool     = pools.NewResettablePool(func() *memcache.MetaDeleteEncoder { return memcache.CreateMetaDeleteEncoder() })
	bulkGetEncoderPool    = pools.NewResettablePool(func() *memcache.BulkEncoder[*memcache.MetaGetEncoder] {
		return memcache.CreateBulkEncoder[*memcache.MetaGetEncoder](10)
	})

	// Decoder pools
	setDecoderPool        = pools.NewResettablePool(func() *memcache.MetaSetDecoder { return memcache.CreateMetaSetDecoder() })
	getDecoderPool        = pools.NewResettablePool(func() *memcache.MetaGetDecoder { return memcache.CreateMetaGetDecoder() })
	arithmeticDecoderPool = pools.NewResettablePool(func() *memcache.MetaArithmeticDecoder { return memcache.CreateArithmeticDecoder() })
	deleteDecoderPool     = pools.NewResettablePool(func() *memcache.MetaDeleteDecoder { return memcache.CreateMetaDeleteDecoder() })
	bulkGetDecoderPool    = pools.NewResettablePool(func() *memcache.BulkDecoder[*memcache.MetaGetDecoder] {
		return memcache.CreateBulkDecoder[*memcache.MetaGetDecoder](10)
	})
)

func main() {
	log.Println("Running Memcached Client Examples...")

	// Run the basic example
	log.Println("\n=== Running Basic Example ===")
	example()

	// Run the advanced features example
	log.Println("\n=== Running Advanced Features Example ===")
	exampleWithAdvancedFeatures()

	// Run the context example
	log.Println("\n=== Running Context Example ===")
	exampleWithContext()

	// Run the bulk operations example
	log.Println("\n=== Running Bulk Operations Example ===")
	exampleBulkOperations()

	// Run the bulk get example
	log.Println("\n=== Running Bulk Get Example ===")
	exampleBulkGet()

	log.Println("\nAll examples completed!")
}

// Example demonstrates how to use the MemcachedClient with encoders and decoders
func example() {
	// Create a new client connected to multiple memcached instances
	addresses := []string{
		"localhost:11211",
		"localhost:11212",
		"localhost:11213",
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}

	// Create client with 3 connections per backend
	client, err := NewClient(addresses, 3, WithLogger(logger))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Get encoder/decoder objects from pools
	setEncoder := setEncoderPool.Get()
	setDecoder := setDecoderPool.Get()
	getEncoder := getEncoderPool.Get()
	getDecoder := getDecoderPool.Get()
	incrEncoder := arithmeticEncoderPool.Get()
	incrDecoder := arithmeticDecoderPool.Get()
	decrEncoder := arithmeticEncoderPool.Get()
	decrDecoder := arithmeticDecoderPool.Get()
	deleteEncoder := deleteEncoderPool.Get()
	deleteDecoder := deleteDecoderPool.Get()

	// Example 1: Set a value using MetaSetEncoder
	fmt.Println("=== Setting a value ===")
	setEncoder.Key = "mykey"
	setEncoder.Value = []byte("hello world")
	setEncoder.TTL = 60 // 60 seconds

	err = client.MetaSet(ctx, setEncoder, setDecoder)
	if err != nil {
		log.Printf("Failed to set value: %v", err)
	} else {
		fmt.Printf("✓ Value set successfully (Status: %s)\n", setDecoder.Status)
		if setDecoder.CasId > 0 {
			fmt.Printf("  CAS ID: %d\n", setDecoder.CasId)
		}
	}

	// Example 2: Get a value using MetaGetEncoder
	fmt.Println("\n=== Getting a value ===")
	getEncoder.Key = "mykey"
	getEncoder.FetchValue = true
	getEncoder.FetchRemainingTTL = true
	getEncoder.FetchCasId = true

	err = client.MetaGet(ctx, getEncoder, getDecoder)
	if err != nil {
		log.Printf("Failed to get value: %v", err)
	} else {
		fmt.Printf("✓ Value retrieved: %s\n", string(getDecoder.Value))
		fmt.Printf("  Status: %s\n", getDecoder.Status)
		fmt.Printf("  CAS ID: %d\n", getDecoder.CasId)
		fmt.Printf("  TTL: %d seconds\n", getDecoder.RemainingTTLSeconds)
		fmt.Printf("  Is cache miss: %t\n", getDecoder.Status == memcache.NotFound)
	}

	// Example 3: Increment a counter using MetaArithmeticEncoder
	fmt.Println("\n=== Incrementing a counter ===")
	incrEncoder.Key = "counter"
	incrEncoder.Delta = 1
	incrEncoder.FetchValue = true
	incrEncoder.InitialValue = 0 // Create with value 0 if it doesn't exist

	err = client.MetaIncrement(ctx, incrEncoder, incrDecoder)
	if err != nil {
		log.Printf("Failed to increment counter: %v", err)
	} else {
		fmt.Printf("✓ Counter incremented to: %d\n", incrDecoder.ValueUInt64)
		fmt.Printf("  Status: %s\n", incrDecoder.Status)
	}

	// Example 4: Decrement a counter using MetaArithmeticEncoder
	fmt.Println("\n=== Decrementing a counter ===")
	decrEncoder.Key = "counter"
	decrEncoder.Delta = 1
	decrEncoder.Decrement = true
	decrEncoder.FetchValue = true

	err = client.MetaDecrement(ctx, decrEncoder, decrDecoder)
	if err != nil {
		log.Printf("Failed to decrement counter: %v", err)
	} else {
		fmt.Printf("✓ Counter decremented to: %d\n", decrDecoder.ValueUInt64)
		fmt.Printf("  Status: %s\n", decrDecoder.Status)
	}

	// Example 5: Delete a key using MetaDeleteEncoder
	fmt.Println("\n=== Deleting a key ===")
	deleteEncoder.Key = "mykey"

	err = client.MetaDelete(ctx, deleteEncoder, deleteDecoder)
	if err != nil {
		log.Printf("Failed to delete key: %v", err)
	} else {
		fmt.Printf("✓ Key deleted successfully (Status: %s)\n", deleteDecoder.Status)
	}

	// Example 6: Try to get a deleted key
	fmt.Println("\n=== Getting a deleted key ===")
	getEncoder.Key = "mykey"
	getEncoder.FetchValue = true

	err = client.MetaGet(ctx, getEncoder, getDecoder)
	if err != nil {
		log.Printf("Failed to get deleted key: %v", err)
	} else {
		fmt.Printf("✓ Get result for deleted key:\n")
		fmt.Printf("  Status: %s\n", getDecoder.Status)
		fmt.Printf("  Is cache miss: %t\n", getDecoder.Status == memcache.NotFound)
	}

	// Return all encoders/decoders to pools
	setEncoderPool.Put(setEncoder)
	setDecoderPool.Put(setDecoder)
	getEncoderPool.Put(getEncoder)
	getDecoderPool.Put(getDecoder)
	arithmeticEncoderPool.Put(incrEncoder)
	arithmeticDecoderPool.Put(incrDecoder)
	arithmeticEncoderPool.Put(decrEncoder)
	arithmeticDecoderPool.Put(decrDecoder)
	deleteEncoderPool.Put(deleteEncoder)
	deleteDecoderPool.Put(deleteDecoder)
}

// ExampleWithAdvancedFeatures demonstrates advanced encoder features
func exampleWithAdvancedFeatures() {
	addresses := []string{"localhost:11211"}
	client, err := NewClient(addresses, 2)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Get encoder/decoder objects from pools
	setEncoder := setEncoderPool.Get()
	setDecoder := setDecoderPool.Get()
	getEncoder := getEncoderPool.Get()
	getDecoder := getDecoderPool.Get()

	// Example: Set with CAS and client flags
	fmt.Println("=== Setting with CAS and client flags ===")
	setEncoder.Key = "advanced_key"
	setEncoder.Value = []byte("advanced value")
	setEncoder.TTL = 120
	setEncoder.ClientFlags = 42
	setEncoder.FetchCasId = true   // Get CAS ID back
	setEncoder.Mode = memcache.Add // Only set if key doesn't exist

	err = client.MetaSet(ctx, setEncoder, setDecoder)
	if err != nil {
		log.Printf("Failed to set value: %v", err)
	} else {
		fmt.Printf("✓ Advanced set completed (Status: %s)\n", setDecoder.Status)
		if setDecoder.CasId > 0 {
			fmt.Printf("  CAS ID: %d\n", setDecoder.CasId)
		}
	}

	// Example: Get with all metadata
	fmt.Println("\n=== Getting with all metadata ===")
	getEncoder.Key = "advanced_key"
	getEncoder.FetchValue = true
	getEncoder.FetchRemainingTTL = true
	getEncoder.FetchCasId = true
	getEncoder.FetchClientFlags = true
	getEncoder.FetchItemSizeInBytes = true
	getEncoder.FetchLastAccessedTime = true

	err = client.MetaGet(ctx, getEncoder, getDecoder)
	if err != nil {
		log.Printf("Failed to get value: %v", err)
	} else {
		fmt.Printf("✓ Advanced get completed:\n")
		fmt.Printf("  Value: %s\n", string(getDecoder.Value))
		fmt.Printf("  Status: %s\n", getDecoder.Status)
		fmt.Printf("  CAS ID: %d\n", getDecoder.CasId)
		fmt.Printf("  Client Flags: %d\n", getDecoder.ClientFlags)
		fmt.Printf("  Item Size: %d bytes\n", getDecoder.ItemSizeInBytes)
		fmt.Printf("  TTL: %d seconds\n", getDecoder.RemainingTTLSeconds)
		fmt.Printf("  Last Access: %d seconds ago\n", getDecoder.TimeSinceLastAccessedSeconds)
	}

	// Return encoders/decoders to pools
	setEncoderPool.Put(setEncoder)
	setDecoderPool.Put(setDecoder)
	getEncoderPool.Put(getEncoder)
	getDecoderPool.Put(getDecoder)
}

// ExampleWithContext demonstrates context usage and cancellation
func exampleWithContext() {
	addresses := []string{"localhost:11211"}
	client, err := NewClient(addresses, 2)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get encoder/decoder objects from pools
	setEncoder := setEncoderPool.Get()
	setDecoder := setDecoderPool.Get()

	// This operation will timeout if it takes longer than 5 seconds
	setEncoder.Key = "timeout_test"
	setEncoder.Value = []byte("test")
	setEncoder.TTL = 30

	err = client.MetaSet(ctx, setEncoder, setDecoder)
	if err != nil {
		if err == context.DeadlineExceeded {
			fmt.Println("Operation timed out as expected")
		} else {
			log.Printf("Unexpected error: %v", err)
		}
	} else {
		fmt.Println("Operation completed within timeout")
	}

	// Return encoders/decoders to pools
	setEncoderPool.Put(setEncoder)
	setDecoderPool.Put(setDecoder)
}

// ExampleBulkOperations demonstrates how to perform multiple operations
func exampleBulkOperations() {
	addresses := []string{"localhost:11211", "localhost:11212"}
	client, err := NewClient(addresses, 3)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Get encoder/decoder objects from pools
	setEncoder := setEncoderPool.Get()
	setDecoder := setDecoderPool.Get()
	getEncoder := getEncoderPool.Get()
	getDecoder := getDecoderPool.Get()
	deleteEncoder := deleteEncoderPool.Get()
	deleteDecoder := deleteDecoderPool.Get()

	// Set multiple values
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	values := []string{"value1", "value2", "value3", "value4", "value5"}

	fmt.Println("=== Setting multiple values ===")
	for i, key := range keys {
		setEncoder.Key = key
		setEncoder.Value = []byte(values[i])
		setEncoder.TTL = 60

		err = client.MetaSet(ctx, setEncoder, setDecoder)
		if err != nil {
			log.Printf("Failed to set %s: %v", key, err)
		} else {
			fmt.Printf("✓ Set %s = %s (Status: %s)\n", key, values[i], setDecoder.Status)
		}
	}

	// Get multiple values
	fmt.Println("\n=== Getting multiple values ===")
	for _, key := range keys {
		getEncoder.Key = key
		getEncoder.FetchValue = true

		err = client.MetaGet(ctx, getEncoder, getDecoder)
		if err != nil {
			log.Printf("Failed to get %s: %v", key, err)
		} else {
			fmt.Printf("✓ %s = %s (Status: %s)\n", key, string(getDecoder.Value), getDecoder.Status)
		}
	}

	// Clean up
	fmt.Println("\n=== Cleaning up ===")
	for _, key := range keys {
		deleteEncoder.Key = key

		err = client.MetaDelete(ctx, deleteEncoder, deleteDecoder)
		if err != nil {
			log.Printf("Failed to delete %s: %v", key, err)
		} else {
			fmt.Printf("✓ Deleted %s (Status: %s)\n", key, deleteDecoder.Status)
		}
	}

	// Return encoders/decoders to pools
	setEncoderPool.Put(setEncoder)
	setDecoderPool.Put(setDecoder)
	getEncoderPool.Put(getEncoder)
	getDecoderPool.Put(getDecoder)
	deleteEncoderPool.Put(deleteEncoder)
	deleteDecoderPool.Put(deleteDecoder)
}

// ExampleBulkGet demonstrates how to use the BulkGet API with the generic encoder
func exampleBulkGet() {
	addresses := []string{"localhost:11211", "localhost:11212"}
	client, err := NewClient(addresses, 3)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Get encoder/decoder objects from pools
	setEncoder := setEncoderPool.Get()
	setDecoder := setDecoderPool.Get()
	bulkEncoder := bulkGetEncoderPool.Get()
	bulkDecoder := bulkGetDecoderPool.Get()

	// First, set some values that we'll retrieve in bulk
	keys := []string{"bulk_key1", "bulk_key2", "bulk_key3", "bulk_key4", "bulk_key5"}
	values := []string{"bulk_value1", "bulk_value2", "bulk_value3", "bulk_value4", "bulk_value5"}

	fmt.Println("=== Setting values for bulk retrieval ===")
	for i, key := range keys {
		setEncoder.Key = key
		setEncoder.Value = []byte(values[i])
		setEncoder.TTL = 60

		err = client.MetaSet(ctx, setEncoder, setDecoder)
		if err != nil {
			log.Printf("Failed to set %s: %v", key, err)
		} else {
			fmt.Printf("✓ Set %s = %s (Status: %s)\n", key, values[i], setDecoder.Status)
		}
	}

	// Prepare bulk get with sequential opaque values
	startingOpaque := uint64(1000) // Start with a base opaque value
	bulkEncoder.Opaque = startingOpaque

	// Create a map to track opaque values to keys for response correlation
	opaqueToKey := make(map[uint64]string)

	for i, key := range keys {
		getEnc := getEncoderPool.Get()
		getEnc.Key = key
		getEnc.FetchValue = true
		getEnc.Opaque = startingOpaque + uint64(i)
		bulkEncoder.Encoders = append(bulkEncoder.Encoders, getEnc)
		opaqueToKey[getEnc.Opaque] = key
	}

	// Prepare bulk decoder with corresponding decoders
	for i := range keys {
		dec := getDecoderPool.Get()
		bulkDecoder.Decoders = append(bulkDecoder.Decoders, dec)
		bulkDecoder.OpaqueToKey[startingOpaque+uint64(i)] = keys[i]
	}

	err = client.BulkGet(ctx, bulkEncoder, bulkDecoder)
	if err != nil {
		log.Printf("Bulk get failed: %v", err)
	} else {
		fmt.Println("\n=== Bulk Get Results ===")
		for _, dec := range bulkDecoder.Decoders {
			if dec != nil {
				key := bulkDecoder.OpaqueToKey[dec.Opaque]
				fmt.Printf("%s = %s (Status: %s, Opaque: %d)\n", key, string(dec.Value), dec.Status, dec.Opaque)
			} else {
				fmt.Printf("Unknown key = <nil>\n")
			}
		}
	}

	// Return encoders/decoders to pools
	setEncoderPool.Put(setEncoder)
	setDecoderPool.Put(setDecoder)
	getEncoderPool.PutAll(bulkEncoder.Encoders)
	getDecoderPool.PutAll(bulkDecoder.Decoders)
	bulkGetEncoderPool.Put(bulkEncoder)
	bulkGetDecoderPool.Put(bulkDecoder)
}
