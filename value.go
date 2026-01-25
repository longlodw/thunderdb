package thunderdb

import (
	"bytes"
	"maps"
)

type Value struct {
	value     any
	marshaler MarshalUnmarshaler
	raw       []byte
}

func ValueOfLiteral(v any, m MarshalUnmarshaler) *Value {
	return &Value{
		value:     v,
		marshaler: m,
	}
}

func ValueOfRaw(b []byte, m MarshalUnmarshaler) *Value {
	return &Value{
		marshaler: m,
		raw:       b,
	}
}

func (v *Value) GetValue() (any, error) {
	if v.value != nil {
		return v.value, nil
	}
	if v.raw != nil {
		err := v.marshaler.Unmarshal(v.raw, &v.value)
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
		b, err := v.marshaler.Marshal(v.value)
		if err != nil {
			return nil, err
		}
		v.raw = b
		return v.raw, nil
	}
	return nil, nil
}

func (v *Value) SetRaw(b []byte, m MarshalUnmarshaler) {
	v.raw = b
	v.marshaler = m
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
