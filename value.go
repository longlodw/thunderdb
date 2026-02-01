package thunderdb

import (
	"bytes"
	"maps"
)

// Value represents a column value that can be lazily marshaled/unmarshaled.
// Values are returned by Row.Iter() and can be converted to Go types using GetValue().
type Value struct {
	value     any
	raw       []byte
	singleRaw []byte // Cached single-value encoding (without tuple wrapper)
}

// ValueOfLiteral creates a Value from a Go value. The value will be
// marshaled lazily when needed.
func ValueOfLiteral(v any) *Value {
	return &Value{
		value: v,
	}
}

// ValueOfRaw creates a Value from raw bytes. The bytes will be
// unmarshaled lazily when GetValue() is called.
func ValueOfRaw(b []byte) *Value {
	return &Value{
		raw: b,
	}
}

// GetValue returns the Go value, unmarshaling from raw bytes if necessary.
func (v *Value) GetValue() (any, error) {
	if v.value != nil {
		return v.value, nil
	}
	if v.raw != nil {
		err := orderedMaUn.Unmarshal(v.raw, &v.value)
		if err != nil {
			return nil, err
		}
		return v.value, nil
	}
	return nil, nil
}

// GetRaw returns the raw byte representation, marshaling if necessary.
// The result is cached to avoid repeated marshaling.
func (v *Value) GetRaw() ([]byte, error) {
	if v.raw != nil {
		return v.raw, nil
	}
	if v.value != nil {
		// Wrap single values as a tuple to match how ToKey encodes values
		// ToKey(val) produces []any{val}, so we need to match that format
		var toMarshal any
		if slice, ok := v.value.([]any); ok {
			toMarshal = slice
		} else {
			toMarshal = []any{v.value}
		}
		b, err := orderedMaUn.Marshal(toMarshal)
		if err != nil {
			return nil, err
		}
		v.raw = b // Cache the result
		return v.raw, nil
	}
	return nil, nil
}

// GetSingleRaw returns the single-value encoded bytes (without tuple wrapping).
// This is used for building composite index keys where multiple values are
// combined into a single tuple.
// The result is cached to avoid repeated encoding when value is from a literal.
func (v *Value) GetSingleRaw() ([]byte, error) {
	// Return cached value if available
	if v.singleRaw != nil {
		return v.singleRaw, nil
	}

	if v.value != nil {
		// Encode the value directly without tuple wrapping and cache it
		// We can safely cache this because v.value is stable
		encoded, err := encodeSingle(v.value)
		if err != nil {
			return nil, err
		}
		v.singleRaw = encoded
		return encoded, nil
	}
	if v.raw != nil {
		// raw is tuple-encoded, need to unwrap it
		// Tuple format: [tagTuple][...single-encoded values...][tagTupleEnd]
		if len(v.raw) >= 2 && v.raw[0] == tagTuple {
			// Return slice view - DON'T cache because v.raw may be reused
			return v.raw[1 : len(v.raw)-1], nil
		}
		// Already single-encoded
		return v.raw, nil
	}
	return nil, nil
}

// SetRaw updates the raw byte representation and clears all caches.
func (v *Value) SetRaw(b []byte) {
	v.raw = b
	v.value = nil
	v.singleRaw = nil // Clear cached single encoding
}

func mergeEquals(result *map[int]*Value, equalsLeft, equalsRight map[int]*Value) (bool, error) {
	clear(*result)
	maps.Copy(*result, equalsLeft)
	if equalsLeft == nil && equalsRight == nil {
		return true, nil
	}
	if equalsLeft == nil {
		*result = equalsRight
		return true, nil
	}
	if equalsRight == nil {
		return true, nil
	}
	for k, vRight := range equalsRight {
		if vLeft, ok := (*result)[k]; ok {
			leftBytes, err := vLeft.GetRaw()
			if err != nil {
				return false, err
			}
			rightBytes, err := vRight.GetRaw()
			if err != nil {
				return false, err
			}
			if !bytes.Equal(leftBytes, rightBytes) {
				return false, nil
			}
		} else {
			(*result)[k] = vRight
		}
	}
	return true, nil
}

func mergeNotEquals(result *map[int][]*Value, neqLeft, neqRight map[int][]*Value) {
	clear(*result)
	for k, v := range neqLeft {
		(*result)[k] = append((*result)[k], v...)
	}
	for k, v := range neqRight {
		(*result)[k] = append((*result)[k], v...)
	}
}
