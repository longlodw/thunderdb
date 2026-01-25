package thunderdb

import (
	"bytes"
	"maps"
)

// Value represents a column value that can be lazily marshaled/unmarshaled.
// Values are returned by Row.Iter() and can be converted to Go types using GetValue().
type Value struct {
	value any
	raw   []byte
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
		v.raw = b
		return v.raw, nil
	}
	return nil, nil
}

// GetSingleRaw returns the single-value encoded bytes (without tuple wrapping).
// This is used for building composite index keys where multiple values are
// combined into a single tuple.
func (v *Value) GetSingleRaw() ([]byte, error) {
	if v.value != nil {
		// Encode the value directly without tuple wrapping
		return encodeSingle(v.value)
	}
	if v.raw != nil {
		// raw is tuple-encoded, need to unwrap it
		// Tuple format: [tagTuple][...single-encoded values...][tagTupleEnd]
		if len(v.raw) >= 2 && v.raw[0] == tagTuple {
			// Find the content between tagTuple and tagTupleEnd
			// For a single-element tuple, this is just the single-encoded value
			return v.raw[1 : len(v.raw)-1], nil
		}
		// Already single-encoded (shouldn't normally happen)
		return v.raw, nil
	}
	return nil, nil
}

// SetRaw updates the raw byte representation and clears the cached value.
func (v *Value) SetRaw(b []byte) {
	v.raw = b
	v.value = nil
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
