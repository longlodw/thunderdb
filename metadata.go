package thunderdb

import (
	"bytes"
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

func (sm *Metadata) bestIndex(equals map[int]*Value, ranges map[int]*BytesRange) (uint64, *BytesRange, error) {
	equBits := uint64(0)
	for idx := range equals {
		equBits |= (1 << uint64(idx))
	}
	for idx := range sm.Indexes {
		if idx&equBits != idx {
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
				if selected == idx {
					break
				}
			}
			keyBytes, err := ToKey(values...)
			if err != nil {
				continue
			}
			return idx, NewBytesRange(keyBytes, keyBytes, true, true), nil
		}
	}
	shortestIndex := uint64(0)
	var shortestRange *BytesRange
	for col, r := range ranges {
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

func splitRanges(left, right *Metadata, ranges map[int]*BytesRange) (map[int]*BytesRange, map[int]*BytesRange) {
	leftRanges := make(map[int]*BytesRange)
	rightRanges := make(map[int]*BytesRange)
	for col, r := range ranges {
		if col < left.ColumnsCount {
			leftRanges[col] = r
		} else {
			rightRanges[col-left.ColumnsCount] = r
		}
	}
	return leftRanges, rightRanges
}

func splitEquals(left, right *Metadata, equals map[int]*Value) (map[int]*Value, map[int]*Value) {
	leftEquals := make(map[int]*Value)
	rightEquals := make(map[int]*Value)
	for col, v := range equals {
		if col < left.ColumnsCount {
			leftEquals[col] = v
		} else {
			rightEquals[col-left.ColumnsCount] = v
		}
	}
	return leftEquals, rightEquals
}

func splitCols(left, right *Metadata, cols map[int]bool) (map[int]bool, map[int]bool) {
	leftCols := make(map[int]bool)
	rightCols := make(map[int]bool)
	for col := range cols {
		if col < left.ColumnsCount {
			leftCols[col] = true
		} else {
			rightCols[col-left.ColumnsCount] = true
		}
	}
	return leftCols, rightCols
}

func splitExclusion(left, right *Metadata, exclusion map[int][]*Value) (map[int][]*Value, map[int][]*Value) {
	leftExclusion := make(map[int][]*Value)
	rightExclusion := make(map[int][]*Value)
	for col, vals := range exclusion {
		if col < left.ColumnsCount {
			leftExclusion[col] = vals
		} else {
			rightExclusion[col-left.ColumnsCount] = vals
		}
	}
	return leftExclusion, rightExclusion
}
