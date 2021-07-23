package easypubsub

import (
	"fmt"
	"strings"
)

// Header is a mapping from header keys to values.
type Header map[string][]string

// NewHeader creates an Header.
func NewHeader() Header {
	return make(Header)
}

// NewHeaderWithMap creates an Header from a given key-value map.
func NewHeaderWithMap(m map[string]string) Header {
	header := NewHeader()
	for k, val := range m {
		key := strings.ToLower(k)
		header[key] = append(header[key], val)
	}
	return header
}

// NewHeaderWithPairs returns an Header formed by the mapping of key, value...
func NewHeaderWithPairs(kv ...string) (Header, error) {
	if len(kv)%2 == 1 {
		return nil, fmt.Errorf("pairs got the odd number of input pairs for header: %d", len(kv))
	}
	header := Header{}
	for i := 0; i < len(kv); i += 2 {
		key := kv[i]
		header[key] = append(header[key], kv[i+1])
	}
	return header, nil
}

// Len returns the number of items in Header.
func (header Header) Len() int {
	return len(header)
}

// Clone returns a copy of Header.
func (header Header) Clone() Header {
	return Join(header)
}

// Get obtains the values for a given key.
func (header Header) Get(k string) []string {
	return header[k]
}

// Set sets the value of a given key with a slice of values.
func (header Header) Set(k string, values ...string) {
	if len(values) == 0 {
		return
	}
	header[k] = values
}

// Append adds the values to key k, not overwriting what was already stored at
// that key.
func (header Header) Append(k string, values ...string) {
	if len(values) == 0 {
		return
	}
	header[k] = append(header[k], values...)
}

// Join joins any number of Headers into a single Header.
//
// The order of values for each key is determined by the order in which the Headers
// containing those values are presented to Join.
func Join(Headers ...Header) Header {
	out := Header{}
	for _, header := range Headers {
		for k, v := range header {
			out[k] = append(out[k], v...)
		}
	}
	return out
}
