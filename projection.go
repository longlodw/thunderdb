package thunderdb

import (
	"bytes"
	"iter"
	"maps"
)

type Projection struct {
	toBase      map[string]string
	base        linkedSelector
	columns     []string
	recursive   bool
	parentsList []*queryParent
	fieldSpecs  map[string]ColumnSpec
}

func newProjection(base linkedSelector, fieldsMap map[string]string) *Projection {
	columnSpecs := make(map[string]ColumnSpec)
	baseSpecs := base.Fields()
	reverseMap := make(map[string]string)
	for projField, baseField := range fieldsMap {
		reverseMap[baseField] = projField
	}
	for projField, baseField := range fieldsMap {
		if spec, ok := baseSpecs[baseField]; ok {
			referenceCol := make([]string, len(spec.ReferenceCols))
			for i, refCol := range referenceCol {
				if mappedCol, exists := reverseMap[refCol]; exists {
					referenceCol[i] = mappedCol
				} else {
					referenceCol[i] = refCol
				}
			}
			columnSpecs[projField] = ColumnSpec{
				ReferenceCols: referenceCol,
				Indexed:       spec.Indexed,
				Unique:        spec.Unique,
			}
		}
	}
	columns := make([]string, 0, len(fieldsMap))
	for col, spec := range columnSpecs {
		if len(spec.ReferenceCols) == 0 {
			columns = append(columns, col)
		}
	}
	result := &Projection{
		toBase:      fieldsMap,
		base:        base,
		columns:     columns,
		recursive:   base.IsRecursive(),
		parentsList: make([]*queryParent, 0),
		fieldSpecs:  columnSpecs,
	}
	base.addParent(&queryParent{
		parent: result,
	})
	return result
}

func (p *Projection) Columns() []string {
	return p.columns
}

func (p *Projection) Fields() map[string]ColumnSpec {
	return maps.Clone(p.fieldSpecs)
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

func (p *Projection) Select(ranges map[string]*BytesRange) (iter.Seq2[Row, error], error) {
	return p.selectEval(ranges, false)
}

func (p *Projection) selectEval(ranges map[string]*BytesRange, noEval bool) (iter.Seq2[Row, error], error) {
	// fmt.Printf("DEBUG: Projection.Select ranges=%v toBase=%v\n", ranges, p.toBase)
	baseRanges := make(map[string]*BytesRange)
	baseSpecs := p.base.Fields()

	for projField, kr := range ranges {
		baseField, ok := p.toBase[projField]
		if !ok {
			return nil, ErrFieldNotFound(projField)
		}

		// Handle fields with ReferenceCols by pushing down to all reference columns
		if baseSpec, specExists := baseSpecs[baseField]; specExists && len(baseSpec.ReferenceCols) > 0 {
			for _, refCol := range baseSpec.ReferenceCols {
				if current, exists := baseRanges[refCol]; exists {
					if current.start == nil || bytes.Compare(kr.start, current.start) > 0 {
						current.start = kr.start
						current.includeStart = kr.includeStart
					}
					if current.end == nil || bytes.Compare(kr.end, current.end) < 0 {
						current.end = kr.end
						current.includeEnd = kr.includeEnd
					}
					allExcluded := map[string]bool{}
					for _, e := range current.excludes {
						allExcluded[string(e)] = true
					}
					for _, e := range kr.excludes {
						allExcluded[string(e)] = true
					}
					current.excludes = make([][]byte, 0, len(allExcluded))
					for e := range allExcluded {
						current.excludes = append(current.excludes, []byte(e))
					}
				} else {
					baseRanges[refCol] = kr
				}
			}
		} else {
			// Handle regular fields without ReferenceCols
			if current, exists := baseRanges[baseField]; exists {
				if current.start == nil || bytes.Compare(kr.start, current.start) > 0 {
					current.start = kr.start
					current.includeStart = kr.includeStart
				}
				if current.end == nil || bytes.Compare(kr.end, current.end) < 0 {
					current.end = kr.end
					current.includeEnd = kr.includeEnd
				}
				allExcluded := map[string]bool{}
				for _, e := range current.excludes {
					allExcluded[string(e)] = true
				}
				for _, e := range kr.excludes {
					allExcluded[string(e)] = true
				}
				current.excludes = make([][]byte, 0, len(allExcluded))
				for e := range allExcluded {
					current.excludes = append(current.excludes, []byte(e))
				}
			} else {
				baseRanges[baseField] = kr
			}
		}
	}
	baseSeq, err := p.base.selectEval(baseRanges, noEval)
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
	for toField, fromField := range mapping {
		// Example: p maps "col_b" -> "col_a" (p.toBase["col_b"] = "col_a")
		// New call: Project("col_c" -> "col_b") (Project col_b as col_c)
		// mapping: key="col_c", value="col_b"

		// We want to know: what is the BASE field for "col_b"?
		baseField, ok := p.toBase[fromField]
		if !ok {
			// fromField="col_b" must be in p.toBase
			panic(ErrFieldNotFound(fromField))
		}
		// The new projection maps "col_c" (toField) -> "col_a" (baseField)
		newMapping[toField] = baseField
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
