package thunderdb

import (
	"iter"
	"maps"
	"slices"
)

type Projection struct {
	toBase      map[string]string
	base        linkedSelector
	columns     []string
	recursive   bool
	parentsList []*queryParent
}

func newProjection(base linkedSelector, fieldsMap map[string]string) *Projection {
	result := &Projection{
		toBase:      fieldsMap,
		base:        base,
		columns:     slices.Collect(maps.Keys(fieldsMap)),
		recursive:   base.IsRecursive(),
		parentsList: make([]*queryParent, 0),
	}
	base.addParent(&queryParent{
		parent: result,
	})
	return result
}

func (p *Projection) Columns() []string {
	return p.columns
}

func (p *Projection) IsRecursive() bool {
	return p.recursive
}

func (p *Projection) addParent(parent *queryParent) {
	p.parentsList = append(p.parentsList, parent)
}

func (p *Projection) parents() []*queryParent {
	return p.parentsList
}

func (p *Projection) Select(ranges map[string]*BytesRange, refRange map[string]*RefRange) (iter.Seq2[Row, error], error) {
	baseRanges := make(map[string]*BytesRange)
	for projField, kr := range ranges {
		baseField, ok := p.toBase[projField]
		if !ok {
			return nil, ErrFieldNotFound(projField)
		}
		baseRanges[baseField] = kr
	}
	baseRangesRef := make(map[string]*RefRange)
	for projField, rr := range refRange {
		baseField, ok := p.toBase[projField]
		if !ok {
			return nil, ErrFieldNotFound(projField)
		}
		baseRangesRef[baseField] = rr
	}
	baseSeq, err := p.base.Select(baseRanges, baseRangesRef)
	if err != nil {
		return nil, err
	}
	return func(yield func(Row, error) bool) {
		baseSeq(func(item Row, err error) bool {
			if err != nil {
				return yield(nil, err)
			}
			return yield(newProjectedRow(item, p.toBase), nil)
		})
	}, nil
}

func (p *Projection) Join(bodies ...Selector) Selector {
	linedBodies := make([]linkedSelector, len(bodies)+1)
	linedBodies[0] = p
	for i, body := range bodies {
		linedBodies[i+1] = body.(linkedSelector)
	}
	return newJoining(linedBodies)
}

func (p *Projection) Project(mapping map[string]string) Selector {
	newMapping := make(map[string]string)
	for fromField, toField := range mapping {
		baseField, ok := p.toBase[toField]
		if !ok {
			panic(ErrFieldNotFound(toField))
		}
		newMapping[fromField] = baseField
	}
	return newProjection(p.base, newMapping)
}

type projectedRow struct {
	baseRow Row
	toBase  map[string]string
}

func newProjectedRow(base Row, toBase map[string]string) *projectedRow {
	return &projectedRow{
		baseRow: base,
		toBase:  toBase,
	}
}

func (pr *projectedRow) Get(field string) (any, error) {
	baseField, ok := pr.toBase[field]
	if !ok {
		return nil, ErrFieldNotFound(field)
	}
	return pr.baseRow.Get(baseField)
}

func (pr *projectedRow) ToMap() (map[string]any, error) {
	result := make(map[string]any)
	for projField, baseField := range pr.toBase {
		value, err := pr.baseRow.Get(baseField)
		if err != nil {
			return nil, err
		}
		result[projField] = value
	}
	return result, nil
}
