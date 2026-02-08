package thunderdb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"math"
	"time"
)

// Type tags for lexicographic ordering
// Ordered so: nil < false < true < negative ints < positive ints < floats < strings < bytes < time < tuples
// Note: tagTupleEnd is 0x00 so that empty tuples sort before non-empty tuples
// (empty tuple [0x0A][0x00] < tuple with elements [0x0A][0x01...])
const (
	tagTupleEnd byte = 0x00 // Must be 0x00 for correct tuple ordering
	tagNil      byte = 0x01
	tagFalse    byte = 0x02
	tagTrue     byte = 0x03
	tagInt64    byte = 0x04
	tagUint64   byte = 0x05
	tagFloat64  byte = 0x06
	tagString   byte = 0x07
	tagBytes    byte = 0x08
	tagTime     byte = 0x09
	tagTuple    byte = 0x0A
)

// Escape sequences for strings/bytes (to allow embedded 0x00)
const (
	escapeByte byte = 0x00
	escapeNull byte = 0xFF // 0x00 -> 0x00 0xFF
	escapeEnd  byte = 0x00 // Terminator: 0x00 0x00
)

// ToKey creates a tuple-encoded key from one or more Values.
// It concatenates the single-value encoded bytes from each Value
// and wraps them in tuple markers.
func ToKey(values ...*Value) ([]byte, error) {
	// Calculate total size needed
	totalSize := 2 // tagTuple + tagTupleEnd
	parts := make([][]byte, len(values))
	for i, v := range values {
		singleRaw, err := v.GetSingleRaw()
		if err != nil {
			return nil, err
		}
		parts[i] = singleRaw
		totalSize += len(singleRaw)
	}

	// Build the tuple
	buf := make([]byte, totalSize)
	buf[0] = tagTuple
	pos := 1
	for _, part := range parts {
		copy(buf[pos:], part)
		pos += len(part)
	}
	buf[pos] = tagTupleEnd
	return buf, nil
}

var orderedMaUn = &orderedCodec{}

// gobCodec is used internally for metadata serialization only
var gobCodec = &gobMarshalUnmarshaler{}

type gobMarshalUnmarshaler struct{}

func (g *gobMarshalUnmarshaler) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (g *gobMarshalUnmarshaler) Unmarshal(data []byte, v any) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(v)
}

type orderedCodec struct{}

func (c *orderedCodec) Marshal(v any) ([]byte, error) {
	if vList, ok := v.([]any); ok {
		return encodeTuple(vList)
	}
	return encodeSingle(v)
}

func (c *orderedCodec) Unmarshal(data []byte, v any) error {
	if vList, ok := v.(*[]any); ok {
		decoded, err := decodeTuple(data)
		if err != nil {
			return err
		}
		*vList = decoded
		return nil
	}
	if vAny, ok := v.(*any); ok {
		decoded, _, err := decodeSingle(data)
		if err != nil {
			return err
		}
		*vAny = decoded
		return nil
	}

	// Decode to a generic value first
	decoded, _, err := decodeSingle(data)
	if err != nil {
		return err
	}

	// Try to assign to typed destination
	return assignToTyped(decoded, v)
}

// assignToTyped attempts to assign a decoded value to a typed pointer destination
func assignToTyped(decoded any, v any) error {
	switch dest := v.(type) {
	case *string:
		if s, ok := decoded.(string); ok {
			*dest = s
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *int:
		switch val := decoded.(type) {
		case int64:
			*dest = int(val)
			return nil
		case uint64:
			*dest = int(val)
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *int8:
		if val, ok := decoded.(int64); ok {
			*dest = int8(val)
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *int16:
		if val, ok := decoded.(int64); ok {
			*dest = int16(val)
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *int32:
		if val, ok := decoded.(int64); ok {
			*dest = int32(val)
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *int64:
		if val, ok := decoded.(int64); ok {
			*dest = val
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *uint:
		switch val := decoded.(type) {
		case uint64:
			*dest = uint(val)
			return nil
		case int64:
			*dest = uint(val)
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *uint8:
		if val, ok := decoded.(uint64); ok {
			*dest = uint8(val)
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *uint16:
		if val, ok := decoded.(uint64); ok {
			*dest = uint16(val)
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *uint32:
		if val, ok := decoded.(uint64); ok {
			*dest = uint32(val)
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *uint64:
		if val, ok := decoded.(uint64); ok {
			*dest = val
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *float32:
		if val, ok := decoded.(float64); ok {
			*dest = float32(val)
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *float64:
		if val, ok := decoded.(float64); ok {
			*dest = val
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *bool:
		if val, ok := decoded.(bool); ok {
			*dest = val
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *[]byte:
		if val, ok := decoded.([]byte); ok {
			*dest = val
			return nil
		}
		return ErrCannotUnmarshal(v)

	case *time.Time:
		if val, ok := decoded.(time.Time); ok {
			*dest = val
			return nil
		}
		return ErrCannotUnmarshal(v)

	default:
		return ErrCannotUnmarshal(v)
	}
}

// encodeSingle encodes a single value into ordered bytes
func encodeSingle(v any) ([]byte, error) {
	switch val := v.(type) {
	case nil:
		return []byte{tagNil}, nil

	case bool:
		if val {
			return []byte{tagTrue}, nil
		}
		return []byte{tagFalse}, nil

	case int:
		return encodeInt64(int64(val)), nil
	case int8:
		return encodeInt64(int64(val)), nil
	case int16:
		return encodeInt64(int64(val)), nil
	case int32:
		return encodeInt64(int64(val)), nil
	case int64:
		return encodeInt64(val), nil

	case uint:
		return encodeUint64(uint64(val)), nil
	case uint8:
		return encodeUint64(uint64(val)), nil
	case uint16:
		return encodeUint64(uint64(val)), nil
	case uint32:
		return encodeUint64(uint64(val)), nil
	case uint64:
		return encodeUint64(val), nil

	case float32:
		return encodeFloat64(float64(val)), nil
	case float64:
		return encodeFloat64(val), nil

	case string:
		return encodeString(val), nil

	case []byte:
		return encodeBytes(val), nil

	case time.Time:
		return encodeTime(val), nil

	case []any:
		return encodeTuple(val)

	default:
		return nil, ErrCannotMarshal(v)
	}
}

// encodeInt64 encodes a signed int64 with lexicographic ordering
// Uses XOR with sign bit to make negative numbers sort before positive
func encodeInt64(v int64) []byte {
	buf := make([]byte, 9)
	buf[0] = tagInt64
	// XOR with 0x8000000000000000 flips the sign bit
	// This makes: -MAX_INT64 -> 0x00..., 0 -> 0x80..., MAX_INT64 -> 0xFF...
	encoded := uint64(v) ^ (1 << 63)
	binary.BigEndian.PutUint64(buf[1:], encoded)
	return buf
}

// encodeUint64 encodes an unsigned uint64 with lexicographic ordering
func encodeUint64(v uint64) []byte {
	buf := make([]byte, 9)
	buf[0] = tagUint64
	binary.BigEndian.PutUint64(buf[1:], v)
	return buf
}

// encodeFloat64 encodes a float64 with lexicographic ordering
// IEEE 754 bit manipulation to preserve sort order:
// - Positive floats: flip sign bit
// - Negative floats: flip all bits
func encodeFloat64(v float64) []byte {
	buf := make([]byte, 9)
	buf[0] = tagFloat64
	bits := math.Float64bits(v)
	if v >= 0 {
		// Positive (including +0): flip sign bit
		bits ^= 1 << 63
	} else {
		// Negative: flip all bits
		bits = ^bits
	}
	binary.BigEndian.PutUint64(buf[1:], bits)
	return buf
}

// encodeString encodes a string with escape sequences for embedded nulls
func encodeString(v string) []byte {
	return encodeEscapedBytes(tagString, []byte(v))
}

// encodeBytes encodes a byte slice with escape sequences for embedded nulls
func encodeBytes(v []byte) []byte {
	return encodeEscapedBytes(tagBytes, v)
}

// encodeEscapedBytes encodes bytes with a tag, escaping 0x00 bytes
// Format: [tag][escaped bytes][0x00][0x00]
func encodeEscapedBytes(tag byte, data []byte) []byte {
	// Count 0x00 bytes for buffer sizing
	nullCount := 0
	for _, b := range data {
		if b == 0x00 {
			nullCount++
		}
	}

	// Allocate: 1 (tag) + len(data) + nullCount (escapes) + 2 (terminator)
	buf := make([]byte, 1+len(data)+nullCount+2)
	buf[0] = tag

	pos := 1
	for _, b := range data {
		if b == 0x00 {
			buf[pos] = escapeByte
			buf[pos+1] = escapeNull
			pos += 2
		} else {
			buf[pos] = b
			pos++
		}
	}

	// Terminator: 0x00 0x00
	buf[pos] = escapeEnd
	buf[pos+1] = escapeEnd
	return buf
}

// encodeTime encodes a time.Time as nanoseconds since Unix epoch
func encodeTime(v time.Time) []byte {
	buf := make([]byte, 9)
	buf[0] = tagTime
	// Use UnixNano, encoded as int64 for proper ordering of times before/after epoch
	encoded := uint64(v.UnixNano()) ^ (1 << 63)
	binary.BigEndian.PutUint64(buf[1:], encoded)
	return buf
}

// encodeTuple encodes a slice of values as a tuple
func encodeTuple(values []any) ([]byte, error) {
	buf := []byte{tagTuple}
	for _, v := range values {
		encoded, err := encodeSingle(v)
		if err != nil {
			return nil, err
		}
		buf = append(buf, encoded...)
	}
	buf = append(buf, tagTupleEnd)
	return buf, nil
}

// decodeSingle decodes a single value, returning the value and bytes consumed
func decodeSingle(data []byte) (any, int, error) {
	if len(data) == 0 {
		return nil, 0, ErrInvalidEncoding("empty data")
	}

	tag := data[0]
	switch tag {
	case tagNil:
		return nil, 1, nil

	case tagFalse:
		return false, 1, nil

	case tagTrue:
		return true, 1, nil

	case tagInt64:
		if len(data) < 9 {
			return nil, 0, ErrInvalidEncoding("int64 too short")
		}
		encoded := binary.BigEndian.Uint64(data[1:9])
		v := int64(encoded ^ (1 << 63))
		return v, 9, nil

	case tagUint64:
		if len(data) < 9 {
			return nil, 0, ErrInvalidEncoding("uint64 too short")
		}
		v := binary.BigEndian.Uint64(data[1:9])
		return v, 9, nil

	case tagFloat64:
		if len(data) < 9 {
			return nil, 0, ErrInvalidEncoding("float64 too short")
		}
		bits := binary.BigEndian.Uint64(data[1:9])
		// Reverse the encoding transformation
		if bits&(1<<63) != 0 {
			// Was positive: flip sign bit back
			bits ^= 1 << 63
		} else {
			// Was negative: flip all bits back
			bits = ^bits
		}
		v := math.Float64frombits(bits)
		return v, 9, nil

	case tagString:
		decoded, consumed, err := decodeEscapedBytes(data[1:])
		if err != nil {
			return nil, 0, err
		}
		return string(decoded), 1 + consumed, nil

	case tagBytes:
		decoded, consumed, err := decodeEscapedBytes(data[1:])
		if err != nil {
			return nil, 0, err
		}
		return decoded, 1 + consumed, nil

	case tagTime:
		if len(data) < 9 {
			return nil, 0, ErrInvalidEncoding("time too short")
		}
		encoded := binary.BigEndian.Uint64(data[1:9])
		nanos := int64(encoded ^ (1 << 63))
		return time.Unix(0, nanos).UTC(), 9, nil

	case tagTuple:
		values, consumed, err := decodeTupleContents(data[1:])
		if err != nil {
			return nil, 0, err
		}
		return values, 1 + consumed, nil

	default:
		return nil, 0, ErrInvalidEncoding("unknown tag")
	}
}

// decodeEscapedBytes decodes escaped byte data, returning bytes consumed
func decodeEscapedBytes(data []byte) ([]byte, int, error) {
	var result []byte
	pos := 0

	for pos < len(data) {
		if data[pos] == escapeByte {
			if pos+1 >= len(data) {
				return nil, 0, ErrInvalidEncoding("incomplete escape sequence")
			}
			if data[pos+1] == escapeEnd {
				// Terminator found
				return result, pos + 2, nil
			}
			if data[pos+1] == escapeNull {
				// Escaped null byte
				result = append(result, 0x00)
				pos += 2
			} else {
				return nil, 0, ErrInvalidEncoding("invalid escape sequence")
			}
		} else {
			result = append(result, data[pos])
			pos++
		}
	}

	return nil, 0, ErrInvalidEncoding("unterminated string/bytes")
}

// decodeTupleContents decodes tuple contents after the tagTuple byte
func decodeTupleContents(data []byte) ([]any, int, error) {
	var values []any
	pos := 0

	for pos < len(data) {
		if data[pos] == tagTupleEnd {
			return values, pos + 1, nil
		}

		val, consumed, err := decodeSingle(data[pos:])
		if err != nil {
			return nil, 0, err
		}
		values = append(values, val)
		pos += consumed
	}

	return nil, 0, ErrInvalidEncoding("unterminated tuple")
}

// decodeTuple decodes a complete tuple from data
func decodeTuple(data []byte) ([]any, error) {
	if len(data) == 0 {
		return nil, ErrInvalidEncoding("empty data")
	}

	if data[0] != tagTuple {
		// Single value - wrap in slice
		val, _, err := decodeSingle(data)
		if err != nil {
			return nil, err
		}
		return []any{val}, nil
	}

	values, _, err := decodeTupleContents(data[1:])
	return values, err
}
