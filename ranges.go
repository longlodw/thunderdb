package thunderdb

import (
	"bytes"
	"crypto/subtle"
	"fmt"
	"maps"
	"slices"
)

type Range struct {
	includeStart bool
	includeEnd   bool
	start        *Value
	end          *Value
	distance     []byte
}

func ToKey(values ...any) ([]byte, error) {
	return orderedMaUn.Marshal(values)
}

func NewRangeFromBytes(startKey, endKey []byte, includeStart, includeEnd bool) (*Range, error) {
	var start *Value
	if startKey != nil {
		start = ValueOfRaw(startKey, orderedMaUn)
	}
	var end *Value
	if endKey != nil {
		end = ValueOfRaw(endKey, orderedMaUn)
	}
	return NewRangeFromValue(start, end, includeStart, includeEnd)
}

func NewRangeFromLiteral(startVals, endVals any, includeStart, includeEnd bool) (*Range, error) {
	return NewRangeFromValue(ValueOfLiteral(startVals, orderedMaUn), ValueOfLiteral(endVals, orderedMaUn), includeStart, includeEnd)
}

func NewRangeFromValue(start, end *Value, includeStart, includeEnd bool) (*Range, error) {
	res := &Range{
		start:        start,
		end:          end,
		includeStart: includeStart,
		includeEnd:   includeEnd,
	}
	distance, err := res.computeDistance()
	if err != nil {
		return nil, err
	}
	res.distance = distance
	return res, nil
}

func (ir *Range) Merge(other *Range) (*Range, error) {
	newStart := ir.start
	newIncludeStart := ir.includeStart
	if other.start != nil {
		otherStart, err := other.start.GetRaw()
		if err != nil {
			return nil, err
		}
		irStart, err := newStart.GetRaw()
		if err != nil {
			return nil, err
		}
		c := bytes.Compare(irStart, otherStart)
		if newStart == nil || c > 0 {
			newStart = other.start
			newIncludeStart = other.includeStart
		} else if c == 0 {
			newIncludeStart = newIncludeStart && other.includeStart
		}
	}
	newEnd := ir.end
	newIncludeEnd := ir.includeEnd
	if other.end != nil {
		otherEnd, err := other.end.GetRaw()
		if err != nil {
			return nil, err
		}
		irEnd, err := newEnd.GetRaw()
		if err != nil {
			return nil, err
		}
		c := bytes.Compare(irEnd, otherEnd)
		if newEnd == nil || c < 0 {
			newEnd = other.end
			newIncludeEnd = other.includeEnd
		} else if c == 0 {
			newIncludeEnd = newIncludeEnd && other.includeEnd
		}
	}
	return NewRangeFromValue(newStart, newEnd, newIncludeStart, newIncludeEnd)
}

func (ir *Range) Contains(key *Value) (bool, error) {
	keyRaw, err := key.GetRaw()
	if err != nil {
		return false, err
	}
	if ir.start != nil {
		irStart, err := ir.start.GetRaw()
		if err != nil {
			return false, err
		}
		cmpStart := bytes.Compare(keyRaw, irStart)
		if cmpStart < 0 || (cmpStart == 0 && !ir.includeStart) {
			return false, nil
		}
	}
	if ir.end != nil {
		irEnd, err := ir.end.GetRaw()
		if err != nil {
			return false, err
		}
		cmpEnd := bytes.Compare(keyRaw, irEnd)
		if cmpEnd > 0 || (cmpEnd == 0 && !ir.includeEnd) {
			return false, nil
		}
	}
	return true, nil
}

func (ir *Range) ToString() string {
	return fmt.Sprintf("%v|%v|%v|%v",
		ir.start,
		ir.end,
		ir.includeStart,
		ir.includeEnd,
	)
}

func (ir *Range) computeDistance() ([]byte, error) {
	var irStart []byte
	if ir.start != nil {
		var err error
		irStart, err = ir.start.GetRaw()
		if err != nil {
			return nil, err
		}
	}
	start := slices.Clone(irStart)
	if start == nil {
		start = []byte{}
	}
	var irEnd []byte
	if ir.end != nil {
		var err error
		irEnd, err = ir.end.GetRaw()
		if err != nil {
			return nil, err
		}
	}
	end := slices.Clone(irEnd)
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
	return distance, nil
}

func MergeRangesMap(a, b map[int]*Range) (map[int]*Range, error) {
	result := make(map[int]*Range)
	maps.Copy(result, a)
	for k, v := range b {
		if existing, ok := result[k]; ok {
			cur, err := existing.Merge(v)
			if err != nil {
				return nil, err
			}
			result[k] = cur
		} else {
			result[k] = v
		}
	}
	return result, nil
}
