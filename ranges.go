package thunderdb

import (
	"bytes"
	"crypto/subtle"
	"fmt"
	"maps"
	"slices"
)

// interval represents a range of values for index-based queries.
// It is used internally for range scans and query optimization.
type interval struct {
	includeStart bool
	includeEnd   bool
	start        *Value
	end          *Value
	distance     []byte
}

func newIntervalFromBytes(startKey, endKey []byte, includeStart, includeEnd bool) (*interval, error) {
	var start *Value
	if startKey != nil {
		start = ValueOfRaw(startKey)
	}
	var end *Value
	if endKey != nil {
		end = ValueOfRaw(endKey)
	}
	return newIntervalFromValue(start, end, includeStart, includeEnd)
}

// newPointIntervalFromBytes creates a range for a single point (start == end).
// This is optimized to avoid allocations for distance computation since distance is always zero.
func newPointIntervalFromBytes(key []byte) *interval {
	val := ValueOfRaw(key)
	return &interval{
		start:        val,
		end:          val,
		includeStart: true,
		includeEnd:   true,
		distance:     nil, // Zero distance for point lookups
	}
}

func newIntervalFromValue(start, end *Value, includeStart, includeEnd bool) (*interval, error) {
	res := &interval{
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

func (ir *interval) Merge(other *interval) (*interval, error) {
	newStart := ir.start
	newIncludeStart := ir.includeStart
	if other.start != nil {
		otherStart, err := other.start.GetRaw()
		if err != nil {
			return nil, err
		}
		var irStart []byte
		if newStart != nil {
			irStart, err = newStart.GetRaw()
			if err != nil {
				return nil, err
			}
		}
		c := 0
		if newStart != nil {
			c = bytes.Compare(irStart, otherStart)
		}
		if newStart == nil || c < 0 {
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
		var irEnd []byte
		if newEnd != nil {
			irEnd, err = newEnd.GetRaw()
			if err != nil {
				return nil, err
			}
		}
		c := 0
		if newEnd != nil {
			c = bytes.Compare(irEnd, otherEnd)
		}
		if newEnd == nil || c > 0 {
			newEnd = other.end
			newIncludeEnd = other.includeEnd
		} else if c == 0 {
			newIncludeEnd = newIncludeEnd && other.includeEnd
		}
	}
	return newIntervalFromValue(newStart, newEnd, newIncludeStart, newIncludeEnd)
}

func (ir *interval) Contains(key *Value) (bool, error) {
	keyRaw, err := key.GetRaw()
	if err != nil {
		return false, err
	}
	return ir.ContainsBytes(keyRaw)
}

// ContainsBytes checks if raw bytes are within the range without allocating a Value
func (ir *interval) ContainsBytes(keyRaw []byte) (bool, error) {
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

func (ir *interval) ToString() string {
	return fmt.Sprintf("%v|%v|%v|%v",
		ir.start,
		ir.end,
		ir.includeStart,
		ir.includeEnd,
	)
}

func (ir *interval) computeDistance() ([]byte, error) {
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

func MergeRangesMap(result *map[int]*interval, a, b map[int]*interval) error {
	clear(*result)
	maps.Copy(*result, a)
	for k, v := range b {
		if existing, ok := (*result)[k]; ok {
			cur, err := existing.Merge(v)
			if err != nil {
				return err
			}
			(*result)[k] = cur
		} else {
			(*result)[k] = v
		}
	}
	return nil
}

func inRanges(vals map[int]*Value, equals map[int]*Value, ranges map[int]*interval, exclusions map[int][]*Value) (bool, error) {
	comparableBytesCache := make(map[int][]byte)
	for idx, val := range equals {
		var kBytes []byte
		var err error
		// check cache
		if cached, ok := comparableBytesCache[idx]; ok {
			kBytes = cached
		} else {
			// Get tuple-encoded bytes for this column's value
			kBytes, err = ToKey(vals[idx])
			if err != nil {
				return false, err
			}
			comparableBytesCache[idx] = kBytes
		}
		eqBytes, err := val.GetRaw()
		if err != nil {
			return false, err
		}
		if !bytes.Equal(kBytes, eqBytes) {
			return false, nil
		}
	}
	for idx, kr := range ranges {
		var kBytes []byte
		var err error
		// check cache
		if cached, ok := comparableBytesCache[idx]; ok {
			kBytes = cached
		} else {
			// Get tuple-encoded bytes for this column's value
			kBytes, err = ToKey(vals[idx])
			if err != nil {
				return false, err
			}
			comparableBytesCache[idx] = kBytes
		}
		if con, err := kr.ContainsBytes(kBytes); err != nil {
			return false, err
		} else if !con {
			return false, nil
		}
	}
	for idx, exList := range exclusions {
		var kBytes []byte
		var err error
		// check cache
		if cached, ok := comparableBytesCache[idx]; ok {
			kBytes = cached
		} else {
			// Get tuple-encoded bytes for this column's value
			kBytes, err = ToKey(vals[idx])
			if err != nil {
				return false, err
			}
			comparableBytesCache[idx] = kBytes
		}
		for _, ex := range exList {
			rawEx, err := ex.GetRaw()
			if err != nil {
				return false, err
			}
			if bytes.Equal(kBytes, rawEx) {
				return false, nil
			}
		}
	}
	return true, nil
}
