package memcache

import (
	"reflect"
	"strings"
	"testing"
)

// isMemcachedCompatibleDefaultFields ensures that any struct pointer s has all the fields set to their sensible defaults
// with respect to memcached.
func isMemcachedCompatibleDefaultFields(t *testing.T, s interface{}) {
	val := reflect.Indirect(reflect.ValueOf(s)) // this dereferences the pointer

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)

		if !field.CanInterface() {
			// Skip the field if it cannot be interfaced (e.g., unexported fields).
			continue
		}

		if field.Kind() == reflect.Chan {
			// Channels shouldn't be reset when the resettable is called.
			// ensure though that the channels have empty length and size 1
			if field.Len() != 0 {
				t.Fatalf("Field %s is not default value for channel. Length: %v. Default channel length should always be 0", val.Type().Field(i).Name, field.Len())
			}

			if field.Cap() != 1 {
				t.Fatalf("Field %s is not default value for channel. Capacity: %v. Default channel capacity should always be 1", val.Type().Field(i).Name, field.Cap())
			}
			continue
		}

		// if the field is of type string, then it maybe an enum string whose default value should either have invalid or notset suffix
		if field.Kind() == reflect.String && field.String() != "" {
			if !strings.HasSuffix(field.String(), "Invalid") && !strings.HasSuffix(field.String(), "NotSet") {
				t.Fatalf("Field %s is not default value for string enums. Value: %v. Default string enums should always have a suffix of NotSet or Invalid", val.Type().Field(i).Name, field.String())
			}
			continue
		}

		// If name of the field is TTL or ends with TTL, then the default value should be -1
		// as long as the type of the field is int32
		if strings.HasSuffix(strings.ToLower(val.Type().Field(i).Name), "ttl") {
			if field.Kind() == reflect.Int32 && field.Int() != -1 {
				t.Fatalf("Field %s is not default value for TTL. Value: %v. Default TTL should always be -1", val.Type().Field(i).Name, field.Int())
			}
			continue
		}

		// if field is an array of other encoders, then a reset should maintain the underlying array and maintain an empty list
		if field.Kind() == reflect.Slice {
			if field.Len() != 0 {
				t.Fatalf("Field %s is not default value for array. Length: %v. Default array length should always be 0", val.Type().Field(i).Name, field.Len())
			}

			// ensure that the underlying array still has the capacity
			//if field.Cap() == 0 {
			//	t.Fatalf("Field %s is not default value for array. Capacity: %v. Default array capacity should always be non-zero", val.Type().Field(i).Name, field.Cap())
			//}

			continue
		}

		if !reflect.DeepEqual(field.Interface(), reflect.Zero(field.Type()).Interface()) {
			t.Fatalf("Field %s is not zero. Value: %v", val.Type().Field(i).Name, field.Interface())
		}
	}
}

func Test_MetaGetEncoderResetsCorrectly(t *testing.T) {
	encoder := &MetaGetEncoder{
		Key:                   "random-key",
		Base64EncodedKey:      true,
		FetchCasId:            true,
		FetchClientFlags:      true,
		FetchItemHitBefore:    true,
		FetchKey:              true,
		FetchLastAccessedTime: true,
		Opaque:                23043092,
		FetchItemSizeInBytes:  true,
		FetchRemainingTTL:     true,
		PreventLRUBump:        true,
		FetchValue:            true,
		CasOverride:           203948,
		BlockTTL:              114342,
		RecacheTTL:            523,
		UpdateTTL:             222,
	}

	encoder.Reset()

	isMemcachedCompatibleDefaultFields(t, encoder)
}

func Test_MetaGetDecoderResetsCorrectly(t *testing.T) {
	decoder := &MetaGetDecoder{
		Status:                       CacheMiss,
		Recache:                      RecacheAlreadySent,
		Value:                        []byte("random--value"),
		CasId:                        209348,
		RemainingTTLSeconds:          199,
		ClientFlags:                  412341,
		Opaque:                       14781234,
		IsItemHitBefore:              true,
		ItemKey:                      "random---key",
		ItemSizeInBytes:              203942,
		TimeSinceLastAccessedSeconds: 20593,
		Stale:                        true,
		HdrLine:                      "CLIENT_ERROR random error line for test",
	}

	decoder.Reset()
	isMemcachedCompatibleDefaultFields(t, decoder)
}

func Test_MetaSetEncoderResetsCorrectly(t *testing.T) {
	encoder := &MetaSetEncoder{
		Key:              "testkeyyyyy",
		Value:            []byte("asldkfjslkdjfkl"),
		Base64EncodedKey: true,
		FetchCasId:       false,
		CasId:            193847,
		CasOverride:      2039452,
		ClientFlags:      198237,
		Invalidate:       true,
		FetchKey:         true,
		FetchItemSize:    true,
		TTL:              2193,
		Opaque:           119,
		Mode:             Add,
		BlockTTL:         39,
	}
	encoder.Reset()
	isMemcachedCompatibleDefaultFields(t, encoder)
}

func Test_MetaSetDecoderResetsCorrectly(t *testing.T) {
	decoder := &MetaSetDecoder{
		Status:  CacheHit,
		Opaque:  918273,
		CasId:   123847,
		ItemKey: "random-keyh",
		HdrLine: "CLIENT_ERROR - setting this for an unit test",
	}

	decoder.Reset()
	isMemcachedCompatibleDefaultFields(t, decoder)
}

func Test_MetaDeleteEncoderResetsCorrectly(t *testing.T) {
	encoder := &MetaDeleteEncoder{
		Key:              "testkeyyyyy",
		Base64EncodedKey: true,
		CasId:            123847,
		CasOverride:      203948,
		Invalidate:       true,
		FetchKey:         true,
		Opaque:           1111,
		TTL:              123412,
		ClientFlags:      5324522342,
		RemoveValue:      true,
	}
	encoder.Reset()
	isMemcachedCompatibleDefaultFields(t, encoder)
}

func Test_MetaDeleteDecoderResetsCorrectly(t *testing.T) {
	decoder := &MetaDeleteDecoder{
		Status:  Deleted,
		Opaque:  918273,
		ItemKey: "random--delete-keyy",
		HdrLine: "HD\r\n",
	}

	decoder.Reset()
	isMemcachedCompatibleDefaultFields(t, decoder)
}

func Test_MetaArithmeticEncoderResetsCorrectly(t *testing.T) {
	encoder := &MetaArithmeticEncoder{
		Key:               "testkeyyyyy",
		Base64EncodedKey:  true,
		CasId:             203948,
		CasOverride:       124911,
		BlockTTL:          918273,
		InitialValue:      123847,
		Delta:             123847,
		TTL:               198273,
		Decrement:         false,
		Opaque:            123847,
		FetchRemainingTTL: true,
		FetchCasId:        true,
		FetchValue:        true,
		FetchKey:          true,
	}
	encoder.Reset()
	isMemcachedCompatibleDefaultFields(t, encoder)
}

func Test_MetaArithmeticDecoderResetsCorrectly(t *testing.T) {
	decoder := &MetaArithmeticDecoder{
		Status:              CacheHit,
		Opaque:              123847,
		RemainingTTLSeconds: 1000,
		Value:               []byte("129038123"),
		ValueUInt64:         1048291942,
		CasId:               12931231,
		ItemKey:             "keyyyyyy",
		HdrLine:             "HD\r\n",
	}
	decoder.Reset()
	isMemcachedCompatibleDefaultFields(t, decoder)
}

func Test_BulkGetEncoderResetsCorrectly(t *testing.T) {
	encoder := &BulkEncoder[*MetaGetEncoder]{
		Encoders: make([]*MetaGetEncoder, 5),
	}
	// add 5 encoders to the slice
	for i := 0; i < 5; i++ {
		encoder.Encoders[i] = &MetaGetEncoder{
			Key:                   "testkeyyyyy",
			Base64EncodedKey:      true,
			FetchCasId:            true,
			FetchClientFlags:      true,
			FetchItemHitBefore:    true,
			FetchKey:              true,
			FetchLastAccessedTime: true,
			Opaque:                22309481,
			FetchItemSizeInBytes:  true,
			FetchRemainingTTL:     true,
			PreventLRUBump:        true,
			FetchValue:            true,
			CasOverride:           5545454,
			BlockTTL:              100,
			RecacheTTL:            123,
			UpdateTTL:             145,
		}
	}

	encoder.Reset()
	isMemcachedCompatibleDefaultFields(t, encoder)
}

func Test_BulkGetDecoderResetsCorrectly(t *testing.T) {
	decoder := &BulkDecoder[*MetaGetDecoder]{
		Decoders: make([]*MetaGetDecoder, 5),
	}
	// add 5 decoders to the slice
	for i := 0; i < 5; i++ {
		decoder.Decoders[i] = &MetaGetDecoder{
			Status: CacheHit,
			Value:  []byte("fake data goes in here"),
			Opaque: uint64(i),
		}
	}

	decoder.Reset()
	isMemcachedCompatibleDefaultFields(t, decoder)
}
