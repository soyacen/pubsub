package easypubsub

import (
	"fmt"
	"strings"
)

// Header is a mapping from header keys to values.
type Header map[string][]string

// New creates an Header from a given key-value map.
func New(m map[string]string) Header {
	Header := Header{}
	for k, val := range m {
		key := strings.ToLower(k)
		Header[key] = append(Header[key], val)
	}
	return Header
}

// Pairs returns an Header formed by the mapping of key, value ...
func Pairs(kv ...string) (Header, error) {
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
	k = strings.ToLower(k)
	return header[k]
}

// Set sets the value of a given key with a slice of values.
func (header Header) Set(k string, vals ...string) {
	if len(vals) == 0 {
		return
	}
	k = strings.ToLower(k)
	header[k] = vals
}

// Append adds the values to key k, not overwriting what was already stored at
// that key.
func (header Header) Append(k string, vals ...string) {
	if len(vals) == 0 {
		return
	}
	header[k] = append(header[k], vals...)
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
