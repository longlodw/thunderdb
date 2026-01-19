package thunderdb

import "bytes"

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

func mergeEquals(equalsLeft, equalsRight map[int]*Value) (map[int]*Value, bool, error) {
	if equalsLeft == nil && equalsRight == nil {
		return nil, true, nil
	}
	if equalsLeft == nil {
		return equalsRight, true, nil
	}
	if equalsRight == nil {
		return equalsLeft, true, nil
	}
	merged := make(map[int]*Value)
	for k, v := range equalsLeft {
		merged[k] = v
	}
	for k, vRight := range equalsRight {
		if vLeft, ok := merged[k]; ok {
			leftBytes, err := vLeft.GetRaw()
			if err != nil {
				return nil, false, err
			}
			rightBytes, err := vRight.GetRaw()
			if err != nil {
				return nil, false, err
			}
			if !bytes.Equal(leftBytes, rightBytes) {
				return nil, false, nil
			}
		}
	}
	return merged, true, nil
}

func mergeNotEquals(neqLeft, neqRight map[int][]*Value) map[int][]*Value {
	result := make(map[int][]*Value)
	for k, v := range neqLeft {
		result[k] = append(result[k], v...)
	}
	for k, v := range neqRight {
		result[k] = append(result[k], v...)
	}
	return result
}
