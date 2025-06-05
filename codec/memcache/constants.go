package memcache

import (
	"bytes"
	"fmt"
)

var (
	CRLF                  = []byte("\r\n")
	Version               = []byte("version")
	MetaGet               = []byte("mg ")
	MetaSet               = []byte("ms ")
	MetaDelete            = []byte("md ")
	MetaArithmetic        = []byte("ma ")
	FetchValue            = []byte("v ")
	Base64EncodedKey      = []byte("b ")
	FetchCasId            = []byte("c ")
	FetchRemainingTTL     = []byte("t ")
	FetchClientFlags      = []byte("f ")
	FetchItemHitBefore    = []byte("h ")
	FetchKey              = []byte("k ")
	FetchItemSize         = []byte("s ")
	FetchLastAccessedTime = []byte("l ")
	PreventLRUBump        = []byte("u ")
	Invalidate            = []byte("I ")
	RemoveValue           = []byte("x ")
)

const (
	CasOverride      = 'E'
	BlockTTL         = 'N'
	RecacheTTL       = 'R'
	Opaque           = 'O'
	CasId            = 'C'
	InitialValue     = 'J'
	ClientFlagsToken = 'F'
	TTL              = 'T'
	Delta            = 'D'
	Space            = ' '
)

var (
	CacheMissHeader = []byte("EN")
	Header          = []byte("HD")
	ValueHeader     = []byte("VA")
	NotFoundHeader  = []byte("NF")
	ExistsHeader    = []byte("EX")
	NotStoredHeader = []byte("NS")
	PutIfAbsentMode = []byte("ME ")
	AppendMode      = []byte("MA ")
	PrependMode     = []byte("MP ")
	ReplaceMode     = []byte("MR ")
	DecrementMode   = []byte("MD ")
	NoOpRequest     = []byte("mn\r\n")
	NoOpResponse    = []byte("MN\r\n")
)

type RecacheStatus string

const (
	RecacheNotSet      RecacheStatus = "RecacheNotSet"
	RecacheWon         RecacheStatus = "Won"
	RecacheAlreadySent RecacheStatus = "AlreadySent"
)

type MetadataStatus string

const (
	MetadataStatusInvalid MetadataStatus = "MetadataStatusInvalid"
	CacheHit              MetadataStatus = "CacheHit"
	CacheMiss             MetadataStatus = "CacheMiss"
	NotFound              MetadataStatus = "NotFound"
	NotStored             MetadataStatus = "NotStored"
	Exists                MetadataStatus = "Exists"
	Stored                MetadataStatus = "Stored"
	Deleted               MetadataStatus = "Deleted"
)

/*
	MetaGetStatusFromHeader returns the status of a meta get operation:

- "VA" (CACHE_HIT), to indicate that the item was found
- "HD" (CACHE_HIT), to indicate that the item was found, but request did not ask for value
- "EN" (CACHE_MISS), to indicate that the item was not found
*/
func MetaGetStatusFromHeader(hdrPrefix []byte) MetadataStatus {
	switch {
	case bytes.Equal(hdrPrefix, CacheMissHeader):
		return CacheMiss
	case bytes.Equal(hdrPrefix, Header):
		fallthrough
	case bytes.Equal(hdrPrefix, ValueHeader):
		return CacheHit
	}
	return MetadataStatusInvalid
}

/*
MetaSetStatusFromHeader returns the status of a meta set operation:
  - "HD" (STORED), to indicate success.
  - "NS" (NOT_STORED), to indicate the data was not stored, but not
    because of an error.
  - "EX" (EXISTS), to indicate that the item you are trying to store with
    CAS semantics has been modified since you last fetched it.
  - "NF" (NOT_FOUND), to indicate that the item you are trying to store
    with CAS semantics did not exist.
*/
func MetaSetStatusFromHeader(hdrPrefix []byte) MetadataStatus {
	switch {
	case bytes.Equal(hdrPrefix, Header):
		return Stored
	case bytes.Equal(hdrPrefix, NotStoredHeader):
		return NotStored
	case bytes.Equal(hdrPrefix, ExistsHeader):
		return Exists
	case bytes.Equal(hdrPrefix, NotFoundHeader):
		return NotFound
	}
	return MetadataStatusInvalid
}

/*
ArithmeticStatusFromHeader returns the status of an arithmetic operation:
  - "HD" to indicate success
  - "NF" (NOT_FOUND), to indicate that the item with this key was not found.
  - "NS" (NOT_STORED), to indicate that the item was not created as requested
    after a miss.
  - "EX" (EXISTS), to indicate that the supplied CAS token does not match the
    stored item.
*/
func ArithmeticStatusFromHeader(hdrPrefix []byte) MetadataStatus {
	switch {
	case bytes.Equal(hdrPrefix, Header):
		fallthrough
	case bytes.Equal(hdrPrefix, ValueHeader):
		return Stored
	case bytes.Equal(hdrPrefix, NotStoredHeader):
		return NotStored
	case bytes.Equal(hdrPrefix, ExistsHeader):
		return Exists
	case bytes.Equal(hdrPrefix, NotFoundHeader):
		return NotFound
	}
	return MetadataStatusInvalid
}

/*
MetaDeleteStatusFromHeader returns the status of a meta delete operation:
  - "HD" (DELETED), to indicate success
  - "NF" (NOT_FOUND), to indicate that the item with this key was not found.
  - "EX" (EXISTS), to indicate that the supplied CAS token does not match the
    stored item.
*/
func MetaDeleteStatusFromHeader(hdrPrefix []byte) MetadataStatus {
	switch {
	case bytes.Equal(hdrPrefix, Header):
		return Deleted
	case bytes.Equal(hdrPrefix, ExistsHeader):
		return Exists
	case bytes.Equal(hdrPrefix, NotFoundHeader):
		return NotFound
	case bytes.Equal(hdrPrefix, NotStoredHeader):
		return NotStored
	}
	return MetadataStatusInvalid
}

type IllegaleMemcacheKey struct {
	IllegalKey string
}

func (i *IllegaleMemcacheKey) Error() string {
	return fmt.Sprintf("%s is an invalid key for memcache", i.IllegalKey)
}

var _ error = (*IllegaleMemcacheKey)(nil)
