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
	distance     []byte
}

func ToKey(values ...any) ([]byte, error) {
	return orderedMa.Marshal(values)
}

func KeyRange(startKey, endKey []byte, includeStart, includeEnd bool, excludes [][]byte) *keyRange {
	res := &keyRange{
		startKey:     startKey,
		endKey:       endKey,
		excludes:     excludes,
		includeStart: includeStart,
		includeEnd:   includeEnd,
	}
	res.distance = res.computeDistance()
	return res
}

func (ir *keyRange) contains(key []byte) bool {
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

func (ir *keyRange) doesExclude(key []byte) bool {
	for _, exKey := range ir.excludes {
		if bytes.Equal(key, exKey) {
			return true
		}
	}
	return false
}

func (ir *keyRange) computeDistance() []byte {
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
