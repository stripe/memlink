package internal

// Resettable interface should be implemented by any struct can be stored in the pool
// and reused to reduce the memory footprint. This interface should be used to reset the state of the object
type Resettable interface {
	Reset()
}
