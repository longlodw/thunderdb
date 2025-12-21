package thunder

import (
	"fmt"
	"iter"
	"maps"
	"slices"
)

type Projection struct {
	fromBase map[string]string
	toBase   map[string]string
	base     Selector
}

func newProjection(base Selector, fieldsMap map[string]string) (*Projection, error) {
	baseColumns := base.Columns()
	// Allow subset projection, so we don't enforce length equality.
	// But we must validate that all keys in fieldsMap are in baseColumns.

	fromBase := make(map[string]string)
	toBase := make(map[string]string)

	for baseCol, projCol := range fieldsMap {
		if !slices.Contains(baseColumns, baseCol) {
			return nil, fmt.Errorf("column %s not found in base columns", baseCol)
		}
		fromBase[baseCol] = projCol
		toBase[projCol] = baseCol
	}
	return &Projection{
		fromBase: fromBase,
		toBase:   toBase,
		base:     base,
	}, nil

}

func (p *Projection) Columns() []string {
	return slices.Collect(maps.Keys(p.toBase))
}

func (p *Projection) Select(ops ...Op) (iter.Seq2[map[string]any, error], error) {
	adjustedOps := make([]Op, len(ops))
	for i, op := range ops {
		adjustedField, ok := p.toBase[op.Field]
		if !ok {
			return nil, fmt.Errorf("field %s not found in projection", op.Field)
		}
		adjustedOp := Op{
			Type:  op.Type,
			Value: op.Value,
			Field: adjustedField,
		}
		adjustedOps[i] = adjustedOp
	}
	baseSeq, err := p.base.Select(adjustedOps...)
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
			return nil, fmt.Errorf("field %s not found in projection", fromField)
		}
		newMapping[baseField] = toField
	}
	return newProjection(p.base, newMapping)
}
