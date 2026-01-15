package thunderdb

import (
	"bytes"
	"crypto/subtle"
	"fmt"
	"maps"
	"slices"
	"strings"
)

type BytesRange struct {
	includeStart bool
	includeEnd   bool
	start        []byte
	end          []byte
	excludes     [][]byte
	distance     []byte
}

func ToKey(values ...any) ([]byte, error) {
	return orderedMa.Marshal(values)
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

func (ir *BytesRange) Merge(other *BytesRange) *BytesRange {
	newStart := ir.start
	newIncludeStart := ir.includeStart
	if other.start != nil {
		c := bytes.Compare(other.start, newStart)
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
		c := bytes.Compare(other.end, newEnd)
		if newEnd == nil || c < 0 {
			newEnd = other.end
			newIncludeEnd = other.includeEnd
		} else if c == 0 {
			newIncludeEnd = newIncludeEnd && other.includeEnd
		}
	}
	newExcludes := append(ir.excludes, other.excludes...)
	return NewBytesRange(newStart, newEnd, newIncludeStart, newIncludeEnd, newExcludes)
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

func (ir *BytesRange) ToString() string {
	excludesStrs := make([]string, len(ir.excludes))
	for i, ex := range ir.excludes {
		excludesStrs[i] = string(ex)
	}
	slices.Sort(excludesStrs)
	return fmt.Sprintf("%v|%v|%v|%v|[%s]",
		ir.start,
		ir.end,
		ir.includeStart,
		ir.includeEnd,
		strings.Join(excludesStrs, ","),
	)
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

func MergeRangesMap(a, b map[int]*BytesRange) map[int]*BytesRange {
	result := make(map[int]*BytesRange)
	maps.Copy(result, a)
	for k, v := range b {
		if existing, ok := result[k]; ok {
			result[k] = existing.Merge(v)
		} else {
			result[k] = v
		}
	}
	return result
}
