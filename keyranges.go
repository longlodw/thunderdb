package thunder

import (
	"bytes"
	"crypto/subtle"
	"slices"
)

type keyRange struct {
	includeStart bool
	includeEnd   bool
	startKey     []byte
	endKey       []byte
	excludes     [][]byte
}

func (ir keyRange) contains(key []byte) bool {
	if ir.startKey != nil {
		cmpStart := bytes.Compare(key, ir.startKey)
		if cmpStart < 0 || (cmpStart == 0 && !ir.includeStart) {
			return false
		}
	}
	if ir.endKey != nil {
		cmpEnd := bytes.Compare(key, ir.endKey)
		if cmpEnd > 0 || (cmpEnd == 0 && !ir.includeEnd) {
			return false
		}
	}
	return !ir.doesExclude(key)
}

func (ir keyRange) doesExclude(key []byte) bool {
	for _, exKey := range ir.excludes {
		if bytes.Equal(key, exKey) {
			return true
		}
	}
	return false
}

func (ir keyRange) distance() []byte {
	start := slices.Clone(ir.startKey)
	if start == nil {
		start = []byte{}
	}
	end := slices.Clone(ir.endKey)
	if end == nil {
		end = []byte{}
	}
	if len(start) < len(end) {
		pad := make([]byte, len(end)-len(start))
		start = append(pad, start...)
	}
	if len(end) < len(start) {
		pad := make([]byte, len(start)-len(end))
		end = append(pad, end...)
	}
	distance := make([]byte, len(start))
	subtle.XORBytes(distance, end, start)
	return distance
}

func toRanges(ops ...Op) (map[string]*keyRange, error) {
	ranges := make(map[string]*keyRange)
	for _, op := range ops {
		idxRange, exists := ranges[op.Field]
		var valSlice []any
		if s, ok := op.Value.([]any); ok {
			valSlice = s
		} else {
			valSlice = []any{op.Value}
		}
		key, err := orderedMa.Marshal(valSlice)
		if err != nil {
			return nil, err
		}
		if !exists {
			idxRange = &keyRange{
				includeStart: true,
				includeEnd:   true,
			}
			ranges[op.Field] = idxRange
		}
		switch op.Type {
		case OpEq:
			if idxRange.startKey == nil || bytes.Compare(key, idxRange.startKey) > 0 {
				idxRange.startKey = key
				idxRange.includeStart = true
			}
			if idxRange.endKey == nil || bytes.Compare(key, idxRange.endKey) < 0 {
				idxRange.endKey = key
				idxRange.includeEnd = true
			}
		case OpLt:
			if idxRange.endKey == nil || bytes.Compare(key, idxRange.endKey) < 0 {
				idxRange.endKey = key
				idxRange.includeEnd = false
			}
		case OpLe:
			if idxRange.endKey == nil || bytes.Compare(key, idxRange.endKey) < 0 {
				idxRange.endKey = key
				idxRange.includeEnd = true
			}
		case OpGt:
			if idxRange.startKey == nil || bytes.Compare(key, idxRange.startKey) > 0 {
				idxRange.startKey = key
				idxRange.includeStart = false
			}
		case OpGe:
			if idxRange.startKey == nil || bytes.Compare(key, idxRange.startKey) > 0 {
				idxRange.startKey = key
				idxRange.includeStart = true
			}
		case OpNe:
			idxRange.excludes = append(idxRange.excludes, key)
		default:
			return nil, ErrUnsupportedOperator(op.Type)
		}
	}
	return ranges, nil
}
