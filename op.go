package thunderdb

import "bytes"

const (
	OpEq = OpType(0b0010)
	OpNe = OpType(0b0101)
	OpGt = OpType(0b0100)
	OpLt = OpType(0b0001)
	OpGe = OpType(0b0110)
	OpLe = OpType(0b0011)
)

type OpType uint8

type Op struct {
	field  string
	value  []any
	opType OpType
}

func Eq(field string, value ...any) Op {
	return Op{
		field:  field,
		value:  value,
		opType: OpEq,
	}
}

func Ne(field string, value ...any) Op {
	return Op{
		field:  field,
		value:  value,
		opType: OpNe,
	}
}

func Gt(field string, value ...any) Op {
	return Op{
		field:  field,
		value:  value,
		opType: OpGt,
	}
}

func Lt(field string, value ...any) Op {
	return Op{
		field:  field,
		value:  value,
		opType: OpLt,
	}
}

func Ge(field string, value ...any) Op {
	return Op{
		field:  field,
		value:  value,
		opType: OpGe,
	}
}

func Le(field string, value ...any) Op {
	return Op{
		field:  field,
		value:  value,
		opType: OpLe,
	}
}

func ToKeyRanges(ops ...Op) (map[string]*BytesRange, error) {
	keyRanges := make(map[string]*BytesRange)
	for _, op := range ops {
		encodedKey, err := ToKey(op.value...)
		if err != nil {
			return nil, err
		}
		kr, exists := keyRanges[op.field]
		if !exists {
			kr = &BytesRange{}
			keyRanges[op.field] = kr
		}
		switch op.opType {
		case OpEq:
			if kr.start == nil || bytes.Compare(encodedKey, kr.start) > 0 {
				kr.start = encodedKey
				kr.includeStart = true
			}
			if kr.end == nil || bytes.Compare(encodedKey, kr.end) < 0 {
				kr.end = encodedKey
				kr.includeEnd = true
			}
		case OpNe:
			kr.excludes = append(kr.excludes, encodedKey)
		case OpGt:
			if kr.start == nil || bytes.Compare(encodedKey, kr.start) >= 0 {
				kr.start = encodedKey
				kr.includeStart = false
			}
		case OpGe:
			if kr.start == nil || bytes.Compare(encodedKey, kr.start) > 0 {
				kr.start = encodedKey
				kr.includeStart = true
			}
		case OpLt:
			if kr.end == nil || bytes.Compare(encodedKey, kr.end) <= 0 {
				kr.end = encodedKey
				kr.includeEnd = false
			}
		case OpLe:
			if kr.end == nil || bytes.Compare(encodedKey, kr.end) < 0 {
				kr.end = encodedKey
				kr.includeEnd = true
			}
		}
	}
	for field, kr := range keyRanges {
		kr.distance = kr.computeDistance()
		keyRanges[field] = kr
	}
	return keyRanges, nil
}
