package thunder

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
			for projField, baseField := range p.toBase {
				projectedItem[projField] = item[baseField]
			}
			return yield(projectedItem, nil)
		})
	}, nil
}

func (p *Projection) Join(bodies ...Selector) Selector {
	return newJoining(append([]Selector{p}, bodies...))
}

func (p *Projection) Project(mapping map[string]string) Selector {
	newMapping := make(map[string]string)
	for fromField, toField := range mapping {
		baseField, ok := p.toBase[toField]
		if !ok {
			continue
		}
		newMapping[fromField] = baseField
	}
	return newProjection(p.base, newMapping)
}
