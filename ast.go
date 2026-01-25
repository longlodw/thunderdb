package thunderdb

import "bytes"

type Query interface {
	Project(cols ...int) (Query, error)
	Join(other Query, conditions ...JoinOn) (Query, error)
	Metadata() *Metadata
}

type HeadQuery struct {
	bodies   []Query
	metadata Metadata
}

func NewDatalogQuery(colsCount int, indexInfos []IndexInfo) (*HeadQuery, error) {
	result := &HeadQuery{}
	if err := initStoredMetadata(&result.metadata, colsCount, indexInfos); err != nil {
		return nil, err
	}
	return result, nil
}

func (h *HeadQuery) Project(cols ...int) (Query, error) {
	return newProjectedQuery(h, cols)
}

func (h *HeadQuery) Join(other Query, conditions ...JoinOn) (Query, error) {
	return newJoinedQuery(h, other, conditions)
}

func (h *HeadQuery) Metadata() *Metadata {
	return &h.metadata
}

func (h *HeadQuery) Bind(bodies ...Query) error {
	for _, body := range bodies {
		if body.Metadata().ColumnsCount != h.metadata.ColumnsCount {
			return ErrFieldCountMismatch(h.Metadata().ColumnsCount, body.Metadata().ColumnsCount)
		}
	}
	h.bodies = bodies
	return nil
}

type ProjectedQuery struct {
	child    Query
	cols     []int
	metadata Metadata
}

func newProjectedQuery(
	child Query,
	cols []int,
) (*ProjectedQuery, error) {
	result := &ProjectedQuery{
		child: child,
		cols:  cols,
	}
	if err := initProjectedMetadata(&result.metadata, child.Metadata(), cols); err != nil {
		return nil, err
	}
	return result, nil
}

func (ph *ProjectedQuery) Project(cols ...int) (Query, error) {
	return newProjectedQuery(ph, cols)
}

func (ph *ProjectedQuery) Join(other Query, conditions ...JoinOn) (Query, error) {
	return newJoinedQuery(ph, other, conditions)
}

func (ph *ProjectedQuery) Metadata() *Metadata {
	return &ph.metadata
}

type JoinedQuery struct {
	left       Query
	right      Query
	conditions []JoinOn
	metadata   Metadata
}

func newJoinedQuery(
	left Query,
	right Query,
	conditions []JoinOn,
) (*JoinedQuery, error) {
	result := &JoinedQuery{
		left:       left,
		right:      right,
		conditions: conditions,
	}
	if err := initJoinedMetadata(&result.metadata, left.Metadata(), right.Metadata()); err != nil {
		return nil, err
	}
	return result, nil
}

func (jh *JoinedQuery) Project(cols ...int) (Query, error) {
	return newProjectedQuery(jh, cols)
}

func (jh *JoinedQuery) Join(other Query, conditions ...JoinOn) (Query, error) {
	return newJoinedQuery(jh, other, conditions)
}

func (jh *JoinedQuery) Metadata() *Metadata {
	return &jh.metadata
}

const (
	EQ = Op(iota)
	NEQ
	LT
	LTE
	GT
	GTE
)

type Op int

type JoinOn struct {
	LeftField  int
	RightField int
	Operator   Op
}

type StoredQuery struct {
	storageName string
	metadata    Metadata
}

func (ph *StoredQuery) Project(cols ...int) (Query, error) {
	return newProjectedQuery(ph, cols)
}

func (ph *StoredQuery) Join(other Query, conditions ...JoinOn) (Query, error) {
	return newJoinedQuery(ph, other, conditions)
}

func (ph *StoredQuery) Metadata() *Metadata {
	return &ph.metadata
}

type IndexInfo struct {
	ReferencedCols []int
	IsUnique       bool
}

type Condition struct {
	Field    int
	Operator Op
	Value    any
}

func parseConditions(conditions []Condition) (equals map[int]*Value, ranges map[int]*Range, exclusion map[int][]*Value, possible bool, err error) {
	equals = make(map[int]*Value)
	ranges = make(map[int]*Range)
	exclusion = make(map[int][]*Value)
	possible = true
	equalsBytes := make(map[int][]byte)
	exclusionBytes := make(map[int]map[string]bool)

	for _, cond := range conditions {
		val := ValueOfLiteral(cond.Value)
		valBytes, err := val.GetRaw()
		if err != nil {
			return nil, nil, nil, false, err
		}
		switch cond.Operator {
		case EQ:
			if existing, exists := equalsBytes[cond.Field]; exists {
				if !bytes.Equal(existing, valBytes) {
					possible = false
					return nil, nil, nil, false, nil
				}
			} else {
				equalsBytes[cond.Field] = valBytes
			}
			if rng, exists := ranges[cond.Field]; exists {
				inRange, err := rng.Contains(val)
				if err != nil {
					return nil, nil, nil, false, err
				}
				if !inRange {
					possible = false
					return nil, nil, nil, false, nil
				}
				delete(ranges, cond.Field)
			}
			if exclusions, exists := exclusionBytes[cond.Field]; exists {
				if exclusions[string(valBytes)] {
					possible = false
					return nil, nil, nil, false, nil
				}
			}
			equals[cond.Field] = val
		case NEQ:
			if existing, exists := equalsBytes[cond.Field]; exists {
				if bytes.Equal(existing, valBytes) {
					possible = false
					return nil, nil, nil, false, nil
				}
			}
			if _, exists := exclusionBytes[cond.Field]; !exists {
				exclusionBytes[cond.Field] = make(map[string]bool)
				exclusion[cond.Field] = append(exclusion[cond.Field], val)
			}
		case LT:
			curRange, err := NewRangeFromValue(nil, val, false, false)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if eqVal, exists := equals[cond.Field]; exists {
				inRange, err := curRange.Contains(eqVal)
				if err != nil {
					return nil, nil, nil, false, err
				}
				if !inRange {
					possible = false
					return nil, nil, nil, false, nil
				}
				continue
			}
			rng, exists := ranges[cond.Field]
			if !exists {
				ranges[cond.Field] = curRange
			} else {
				ranges[cond.Field], err = rng.Merge(curRange)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		case LTE:
			curRange, err := NewRangeFromValue(nil, val, false, true)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if eqVal, exists := equals[cond.Field]; exists {
				inRange, err := curRange.Contains(eqVal)
				if err != nil {
					return nil, nil, nil, false, err
				}
				if !inRange {
					possible = false
					return nil, nil, nil, false, nil
				}
				continue
			}
			rng, exists := ranges[cond.Field]
			if !exists {
				ranges[cond.Field] = curRange
			} else {
				ranges[cond.Field], err = rng.Merge(curRange)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		case GT:
			curRange, err := NewRangeFromValue(val, nil, false, false)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if eqVal, exists := equals[cond.Field]; exists {
				inRange, err := curRange.Contains(eqVal)
				if err != nil {
					return nil, nil, nil, false, err
				}
				if !inRange {
					possible = false
					return nil, nil, nil, false, nil
				}
				continue
			}
			rng, exists := ranges[cond.Field]
			if !exists {
				ranges[cond.Field] = curRange
			} else {
				ranges[cond.Field], err = rng.Merge(curRange)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		case GTE:
			curRange, err := NewRangeFromValue(val, nil, true, false)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if eqVal, exists := equals[cond.Field]; exists {
				inRange, err := curRange.Contains(eqVal)
				if err != nil {
					return nil, nil, nil, false, err
				}
				if !inRange {
					possible = false
					return nil, nil, nil, false, nil
				}
				continue
			}
			rng, exists := ranges[cond.Field]
			if !exists {
				ranges[cond.Field] = curRange
			} else {
				ranges[cond.Field], err = rng.Merge(curRange)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		}
	}
	return
}
