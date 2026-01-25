package thunderdb

import (
	"bytes"
	"maps"
)

type Value struct {
	value any
	raw   []byte
}

func ValueOfLiteral(v any) *Value {
	return &Value{
		value: v,
	}
}

func ValueOfRaw(b []byte) *Value {
	return &Value{
		raw: b,
	}
}

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
