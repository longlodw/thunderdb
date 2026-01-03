package thunderdb

import (
	"bytes"
	"crypto/subtle"
	"slices"
)

type BytesRange struct {
	includeStart bool
	includeEnd   bool
	start        []byte
	end          []byte
	excludes     [][]byte
	distance     []byte
}

type RefRange struct {
	includeStart bool
	includeEnd   bool
	start        []string
	end          []string
	excludes     [][]string
}

func ToKey(values ...any) ([]byte, error) {
	return orderedMa.Marshal(values)
}

func NewRefRange(start, end []string, includeStart, includeEnd bool, excludes [][]string) *RefRange {
	return &RefRange{
		start:        start,
		end:          end,
		excludes:     excludes,
		includeStart: includeStart,
		includeEnd:   includeEnd,
	}
}

func NewBytesRange(startKey, endKey []byte, includeStart, includeEnd bool, excludes [][]byte) *BytesRange {
	res := &BytesRange{
		start:        startKey,
		end:          endKey,
		excludes:     excludes,
		includeStart: includeStart,
		includeEnd:   includeEnd,
	}
	res.distance = res.computeDistance()
	return res
}

func NewBytesRangeFromVals(startVals, endVals []any, includeStart, includeEnd bool, excludeVals [][]any) (*BytesRange, error) {
	var startKey []byte = nil
	if len(startVals) > 0 {
		var err error
		startKey, err = ToKey(startVals...)
		if err != nil {
			return nil, err
		}
	}
	var endKey []byte = nil
	if len(endVals) > 0 {
		var err error
		endKey, err = ToKey(endVals...)
		if err != nil {
			return nil, err
		}
	}
	excludes := make([][]byte, len(excludeVals))
	for i, vals := range excludeVals {
		excludeKey, err := ToKey(vals...)
		if err != nil {
			return nil, err
		}
		excludes[i] = excludeKey
	}
	return NewBytesRange(startKey, endKey, includeStart, includeEnd, excludes), nil
}

func (rr *RefRange) Contains(r Row, fieldKey []byte) (bool, error) {
	startValues := make([]any, len(rr.start))
	for i, col := range rr.start {
		val, err := r.Get(col)
		if err != nil {
			return false, err
		}
		startValues[i] = val
	}
	endValues := make([]any, len(rr.end))
	for i, col := range rr.end {
		val, err := r.Get(col)
		if err != nil {
			return false, err
		}
		endValues[i] = val
	}
	excludeValues := make([][]any, len(rr.excludes))
	for i, cols := range rr.excludes {
		excludeRow := make([]any, len(cols))
		for j, col := range cols {
			val, err := r.Get(col)
			if err != nil {
				return false, err
			}
			excludeRow[j] = val
		}
		excludeValues[i] = excludeRow
	}
	bytesRange, err := NewBytesRangeFromVals(startValues, endValues, rr.includeStart, rr.includeEnd, excludeValues)
	if err != nil {
		return false, err
	}
	return bytesRange.Contains(fieldKey), nil
}

func (ir *BytesRange) Contains(key []byte) bool {
	if ir.start != nil {
		cmpStart := bytes.Compare(key, ir.start)
		if cmpStart < 0 || (cmpStart == 0 && !ir.includeStart) {
			return false
		}
	}
	if ir.end != nil {
		cmpEnd := bytes.Compare(key, ir.end)
		if cmpEnd > 0 || (cmpEnd == 0 && !ir.includeEnd) {
			return false
		}
	}
	return !ir.doesExclude(key)
}

func (ir *BytesRange) doesExclude(key []byte) bool {
	for _, exKey := range ir.excludes {
		if bytes.Equal(key, exKey) {
			return true
		}
	}
	return false
}

func (ir *BytesRange) computeDistance() []byte {
	start := slices.Clone(ir.start)
	if start == nil {
		start = []byte{}
	}
	end := slices.Clone(ir.end)
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
