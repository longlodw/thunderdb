package thunder

import (
	"iter"
	"maps"
	"slices"
)

type Projection struct {
	fromBase map[string]string
	toBase   map[string]string
	base     Selector
	columns  []string
}

func newProjection(base Selector, fieldsMap map[string]string) (*Projection, error) {
	baseColumns := base.Columns()
	// But we must validate that all keys in fieldsMap are in baseColumns.

	fromBase := make(map[string]string)
	toBase := make(map[string]string)

	for baseCol, projCol := range fieldsMap {
		if !slices.Contains(baseColumns, baseCol) {
			return nil, ErrFieldNotFound(baseCol)
		}
		fromBase[baseCol] = projCol
		toBase[projCol] = baseCol
	}
	return &Projection{
		fromBase: fromBase,
		toBase:   toBase,
		base:     base,
		columns:  slices.Collect(maps.Keys(toBase)),
	}, nil

}

func (p *Projection) Columns() []string {
	return p.columns
}

func (p *Projection) Select(ranges map[string]*keyRange) (iter.Seq2[map[string]any, error], error) {
	baseRanges := make(map[string]*keyRange)
	for projField, kr := range ranges {
		baseField, ok := p.toBase[projField]
		if !ok {
			return nil, ErrFieldNotFound(projField)
		}
		baseRanges[baseField] = kr
	}
	baseSeq, err := p.base.Select(baseRanges)
	if err != nil {
		return nil, err
	}
	return func(yield func(map[string]any, error) bool) {
		baseSeq(func(item map[string]any, err error) bool {
			if err != nil {
				return yield(nil, err)
			}
			projectedItem := make(map[string]any)
			for baseField, projField := range p.fromBase {
				projectedItem[projField] = item[baseField]
			}
			return yield(projectedItem, nil)
		})
	}, nil
}

func (p *Projection) Project(mapping map[string]string) (Selector, error) {
	newMapping := make(map[string]string)
	for fromField, toField := range mapping {
		baseField, ok := p.toBase[fromField]
		if !ok {
			return nil, ErrFieldNotFound(fromField)
		}
		newMapping[baseField] = toField
	}
	return newProjection(p.base, newMapping)
}
