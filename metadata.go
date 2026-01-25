package thunderdb

import (
	"bytes"
	"fmt"
)

type Metadata struct {
	ColumnsCount int
	Indexes      map[uint64]bool
}

func initStoredMetadata(result *Metadata, colsCount int, indexes []IndexInfo) error {
	if colsCount > 64 {
		return ErrColumnCountExceeded64(colsCount)
	}
	indexesMap := make(map[uint64]bool)
	for _, index := range indexes {
		var idxBits uint64 = 0
		for _, col := range index.ReferencedCols {
			idxBits |= 1 << uint64(col)
		}
		indexesMap[idxBits] = index.IsUnique
	}
	result.ColumnsCount = colsCount
	result.Indexes = indexesMap
	return nil
}

func initJoinedMetadata(result, left, right *Metadata) error {
	if left.ColumnsCount+right.ColumnsCount > 64 {
		return ErrColumnCountExceeded64(left.ColumnsCount + right.ColumnsCount)
	}
	indexes := make(map[uint64]bool)
	for idx := range left.Indexes {
		indexes[idx] = false
	}
	for idx := range right.Indexes {
		indexes[idx<<uint64(left.ColumnsCount)] = false
	}
	result.ColumnsCount = left.ColumnsCount + right.ColumnsCount
	result.Indexes = indexes
	return nil
}

func initProjectedMetadata(result, child *Metadata, cols []int) error {
	if len(cols) > 64 {
		return ErrColumnCountExceeded64(len(cols))
	}
	indexes := make(map[uint64]bool)
	childToResultColumnMap := make(map[int][]int)
	for i, col := range cols {
		childToResultColumnMap[col] = append(childToResultColumnMap[col], i)
	}
	for idx, isUnique := range child.Indexes {
		var projectedIdx uint64 = 0
		refCols := ReferenceColumns(idx)
		for _, col := range refCols {
			childCols := childToResultColumnMap[col]
			for _, childCol := range childCols {
				projectedIdx |= 1 << uint64(childCol)
			}
		}
		indexes[projectedIdx] = isUnique
	}
	result.ColumnsCount = len(cols)
	result.Indexes = indexes
	return nil
}

func (sm *Metadata) bestIndex(equals map[int]*Value, ranges map[int]*Range) (uint64, *Range, error) {
	equBits := uint64(0)
	for idx := range equals {
		equBits |= (1 << uint64(idx))
	}
	for idx := range sm.Indexes {
		if idx&equBits == idx {
			selected := uint64(0)
			values := make([]any, 0, sm.ColumnsCount)
			for i := range sm.ColumnsCount {
				if (idx & (1 << uint64(i))) != 0 {
					val, err := equals[i].GetValue()
					if err != nil {
						return 0, nil, err
					}
					values = append(values, val)
					selected |= (1 << uint64(i))
				}
			}
			// Double check we collected all parts
			if selected != idx {
				continue
			}

			keyBytes, err := ToKey(values...)
			if err != nil {
				continue
			}
			r, err := NewRangeFromBytes(keyBytes, keyBytes, true, true)
			return idx, r, err
		}
	}
	shortestIndex := uint64(0)
	var shortestRange *Range
	for col, r := range ranges {
		// This assumes 1-column indexes mainly?
		idxBitsMap := uint64(1) << col
		if _, ok := sm.Indexes[idxBitsMap]; !ok {
			continue
		}
		if shortestRange == nil || bytes.Compare(r.distance, shortestRange.distance) < 0 {
			shortestRange = r
			shortestIndex = idxBitsMap
		}
	}
	return shortestIndex, shortestRange, nil
}

func splitRanges(left, right *Metadata, ranges map[int]*Range) (map[int]*Range, map[int]*Range, error) {
	leftRanges := make(map[int]*Range, len(ranges))
	rightRanges := make(map[int]*Range, len(ranges))
	for col, r := range ranges {
		if col < left.ColumnsCount {
			leftRanges[col] = r
		} else if col-left.ColumnsCount < right.ColumnsCount {
			rightRanges[col-left.ColumnsCount] = r
		} else {
			return nil, nil, ErrFieldNotFound(fmt.Sprintf("Column %d", col))
		}
	}
	return leftRanges, rightRanges, nil
}

func splitEquals(left, right *Metadata, equals map[int]*Value) (map[int]*Value, map[int]*Value, error) {
	leftEquals := make(map[int]*Value, len(equals))
	rightEquals := make(map[int]*Value, len(equals))
	for col, v := range equals {
		if col < left.ColumnsCount {
			leftEquals[col] = v
		} else if col-left.ColumnsCount < right.ColumnsCount {
			rightEquals[col-left.ColumnsCount] = v
		} else {
			return nil, nil, ErrFieldNotFound(fmt.Sprintf("Column %d", col))
		}
	}
	return leftEquals, rightEquals, nil
}

func splitCols(left, right *Metadata, cols map[int]bool) (map[int]bool, map[int]bool, error) {
	leftCols := make(map[int]bool, len(cols))
	rightCols := make(map[int]bool, len(cols))
	for col := range cols {
		if col < left.ColumnsCount {
			leftCols[col] = true
		} else if col-left.ColumnsCount < right.ColumnsCount {
			rightCols[col-left.ColumnsCount] = true
		} else {
			return nil, nil, ErrFieldNotFound(fmt.Sprintf("Column %d", col))
		}
	}
	return leftCols, rightCols, nil
}

func splitExclusion(left, right *Metadata, exclusion map[int][]*Value) (map[int][]*Value, map[int][]*Value, error) {
	leftExclusion := make(map[int][]*Value, len(exclusion))
	rightExclusion := make(map[int][]*Value, len(exclusion))
	for col, vals := range exclusion {
		if col < left.ColumnsCount {
			leftExclusion[col] = vals
		} else if col-left.ColumnsCount < right.ColumnsCount {
			rightExclusion[col-left.ColumnsCount] = vals
		} else {
			return nil, nil, ErrFieldNotFound(fmt.Sprintf("Column %d", col))
		}
	}
	return leftExclusion, rightExclusion, nil
}
